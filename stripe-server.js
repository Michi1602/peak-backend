const express = require('express');
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);
const { createClient } = require('@supabase/supabase-js');
const { Resend } = require('resend');
const cron = require('node-cron');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const crypto = require('crypto');

// ── STARTUP ENV VALIDATION (Apr 2026 hardening) ──────────────────────
// Fail loud at boot if a critical secret is missing instead of crashing
// later when a user hits the affected endpoint. Prevents the worst case
// where Stripe webhooks silently fail because STRIPE_WEBHOOK_SECRET was
// forgotten on Render after a redeploy.
const REQUIRED_ENV = [
  'STRIPE_SECRET_KEY',
  'STRIPE_WEBHOOK_SECRET',
  'SUPABASE_URL',
  'SUPABASE_SERVICE_KEY',
  'ANTHROPIC_API_KEY',
  'RESEND_API_KEY',
];
const MISSING_ENV = REQUIRED_ENV.filter(k => !process.env[k]);
if (MISSING_ENV.length) {
  console.error('═══════════════════════════════════════════════════════════════');
  console.error('❌ FATAL: Missing required environment variables:');
  MISSING_ENV.forEach(k => console.error(`   • ${k}`));
  console.error('   Set these in Render → Environment before redeploying.');
  console.error('═══════════════════════════════════════════════════════════════');
  process.exit(1);
}

const app = express();
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const resend = new Resend(process.env.RESEND_API_KEY);

const FRONTEND_URL = process.env.FRONTEND_URL || 'https://peak-mj-performance.app';
const BACKEND_URL = process.env.BACKEND_URL || 'https://peak-backend-u52q.onrender.com';
// FROM_EMAIL: visible "from" address. Uses mj-performance.net because that's
// the domain verified at Resend (free tier limits to one domain). The user
// sees "PEAK <support@mj-performance.net>" but if they hit "Reply", their
// mail client routes to REPLY_TO instead — that mailbox actually receives.
const FROM_EMAIL = 'PEAK <support@mj-performance.net>';
// REPLY_TO: real, monitored support inbox. Set as a header on every send
// so user replies don't disappear into a non-existent mailbox.
const REPLY_TO = 'support@peak-mj-performance.app';

const COMPANY = {
  name: 'MJ Performance',
  address: 'Am Hasel 6, 85139 Wettstetten',
  email: REPLY_TO, // Public-facing contact — must match imprint + privacy
  website: 'https://peak-mj-performance.app',
  owner: 'Michael Jahn',
};

// ── CORS — restricted to known origins (security hardening Apr 2026) ──
// Previously: app.use(cors()) — open to all origins, allowed CSRF-style abuse.
// Now: explicit allowlist. TWA wraps the app so the origin in production
// is the same as the deployed frontend.
const ALLOWED_ORIGINS = [
  'https://peak-mj-performance.app',
  'https://www.peak-mj-performance.app',
  'https://mj-performance.net',
  'https://www.mj-performance.net',
  // Vercel default URLs — both the production alias (no hash) and
  // any preview deployment (with hash). Bug fixed Apr 2026: previously
  // only the hashed preview pattern matched, which meant the stable
  // production alias https://peak-frontend.vercel.app was blocked,
  // breaking every backend call from any user who landed on that URL.
  'https://peak-frontend.vercel.app',
];
app.use(cors({
  origin: (origin, callback) => {
    // Allow no-origin requests (mobile apps, curl, server-to-server)
    if (!origin) return callback(null, true);
    if (ALLOWED_ORIGINS.includes(origin)) return callback(null, true);
    // Vercel preview deployments use either of these patterns:
    //   https://peak-frontend-<hash>.vercel.app           (branch alias)
    //   https://peak-frontend-<hash>-michi1602.vercel.app (deploy alias)
    if (/^https:\/\/peak-frontend(-[a-z0-9-]+)?\.vercel\.app$/.test(origin)) return callback(null, true);
    console.warn(`🚫 CORS blocked origin: ${origin}`);
    return callback(new Error('Not allowed by CORS'));
  },
  credentials: true,
}));
app.use('/webhook', express.raw({ type: 'application/json' }));
app.use(express.json({ limit: '10mb' }));

// ── RATE LIMITERS — protect expensive AI endpoints + auth flows ──────
// Render free tier sits behind a proxy, so we trust X-Forwarded-For.
app.set('trust proxy', 1);

// AI endpoints: per IP, 60 req / 10 min (covers normal use, blocks scripts).
// Authenticated users get a more generous quota via the per-user quota in
// /ai/generate; this is just the floor.
const aiLimiter = rateLimit({
  windowMs: 10 * 60 * 1000,
  max: 60,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many requests', code: 'RATE_LIMIT' },
  // Rate-limit by user id when authenticated, else by IP. This prevents
  // one rogue user from blocking other users behind the same NAT.
  keyGenerator: (req) => {
    const auth = req.headers.authorization;
    if (auth && auth.startsWith('Bearer ')) return 'tok:' + auth.slice(7, 50);
    return req.ip;
  },
});

// Auth endpoints: stricter — 20 req / 10 min per IP.
// Login/OTP flows shouldn't be hammered.
const authLimiter = rateLimit({
  windowMs: 10 * 60 * 1000,
  max: 20,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many auth requests', code: 'AUTH_RATE_LIMIT' },
});

// ── PAGINATED USER LOOKUP (Apr 2026 fix) ─────────────────────────────
// Supabase's auth.admin.listUsers() returns ONE PAGE at a time (default
// 50, max 1000). Earlier code called it without pagination, which means
// any user past the first page was effectively invisible — verify-otp
// would create duplicate auth entries for them, signup-free would miss
// the existence check, and the webhook's deduplication broke the same way.
// This helper iterates pages until the user is found or pages run out.
async function findAuthUserByEmail(email) {
  const target = String(email || '').toLowerCase().trim();
  if (!target) return null;
  const PAGE_SIZE = 1000; // Supabase max — fewer round-trips
  let page = 1;
  // Hard cap at 50 pages (50k users) to prevent runaway loops on a broken API.
  while (page <= 50) {
    let data;
    try {
      const result = await supabase.auth.admin.listUsers({ page, perPage: PAGE_SIZE });
      data = result?.data;
    } catch (e) {
      console.error(`listUsers page ${page} failed:`, e.message);
      return null;
    }
    const users = data?.users || [];
    if (users.length === 0) return null;
    const match = users.find(u => (u.email || '').toLowerCase() === target);
    if (match) return match;
    if (users.length < PAGE_SIZE) return null; // last page, no match
    page++;
  }
  console.warn(`findAuthUserByEmail: gave up after 50 pages for ${target}`);
  return null;
}

// ── AUTH + TIER HELPERS (Apr 2026 hardening) ─────────────────────────
// Centralised auth-resolution + tier-validation so we don't duplicate the
// same check across every premium endpoint. Consistent error responses
// make the frontend simpler too.
//
// Returns { ok: true, userId, email, tier } on success, or
// { ok: false, status, body } on any failure (caller just spreads body).
async function resolveAuthAndTier(req, { requirePremium = false } = {}) {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return { ok: false, status: 401, body: { error: 'auth_required', code: 'AUTH_REQUIRED' } };
  }
  const token = authHeader.slice(7);
  let authUserId, email;
  try {
    const { data, error } = await supabase.auth.getUser(token);
    if (error || !data?.user?.id) {
      return { ok: false, status: 401, body: { error: 'auth_invalid', code: 'AUTH_INVALID' } };
    }
    authUserId = data.user.id;
    email = data.user.email;
  } catch (_) {
    return { ok: false, status: 401, body: { error: 'auth_invalid', code: 'AUTH_INVALID' } };
  }

  // Pull tier + abuse-block status in one shot.
  let tier = 'free', status = null;
  try {
    const { data: u } = await supabase
      .from('users')
      .select('tier, status')
      .eq('id', authUserId)
      .maybeSingle();
    if (u?.tier) tier = u.tier;
    if (u?.status) status = u.status;
  } catch (e) {
    // DB error → fail-closed for premium endpoints, fail-open otherwise
    if (requirePremium) {
      return { ok: false, status: 500, body: { error: 'tier_check_failed', code: 'TIER_CHECK_FAILED' } };
    }
  }

  if (status === 'blocked_voucher_abuse') {
    return { ok: false, status: 403, body: { error: 'account_blocked', code: 'ACCOUNT_BLOCKED' } };
  }
  if (requirePremium && tier !== 'premium') {
    return { ok: false, status: 403, body: { error: 'premium_required', code: 'PREMIUM_REQUIRED' } };
  }
  return { ok: true, userId: authUserId, email, tier };
}

// ── HEALTH CHECK ──────────────────────────────────────────────────────
app.get('/', (req, res) => {
  res.json({ status: 'PEAK Backend running', time: new Date().toISOString() });
});

// ── UNSUBSCRIBE ───────────────────────────────────────────────────────
app.get('/unsubscribe', async (req, res) => {
  const { email } = req.query;
  if (!email) return res.status(400).send('Missing email.');
  await supabase.from('users').update({ unsubscribed: true }).eq('email', email);
  res.send(`<!DOCTYPE html><html lang="de"><head><meta charset="UTF-8"><title>Abgemeldet</title>
  <style>body{font-family:sans-serif;max-width:480px;margin:80px auto;padding:0 20px;text-align:center}
  .btn{display:inline-block;background:#2D6A4F;color:#fff;padding:12px 24px;border-radius:10px;text-decoration:none;font-weight:600;margin-top:20px}</style>
  </head><body>
  <h1>✅ Du wurdest abgemeldet</h1>
  <p style="color:#666">Du erhältst keine weiteren E-Mails von PEAK.<br><br>You have been unsubscribed from PEAK emails.</p>
  <a href="${FRONTEND_URL}" class="btn">Zurück zur App</a>
  </body></html>`);
});

// ── LEGAL PAGES ───────────────────────────────────────────────────────
// The authoritative versions of Datenschutz/Impressum live on the
// frontend (Vercel) as static HTML files. They have full DE+EN content
// with TOC and processing-activities table. We keep these backend routes
// only as 301 redirects so any old bookmarks or search-engine cached
// URLs still land users on the right page.
app.get('/datenschutz', (req, res) => res.redirect(301, `${FRONTEND_URL}/datenschutz`));
app.get('/privacy',     (req, res) => res.redirect(301, `${FRONTEND_URL}/datenschutz`));
app.get('/impressum',   (req, res) => res.redirect(301, `${FRONTEND_URL}/impressum`));
app.get('/imprint',     (req, res) => res.redirect(301, `${FRONTEND_URL}/impressum`));

// ── CREATE CHECKOUT SESSION ───────────────────────────────────────────
// ── CHECK IF EMAIL ALREADY HAS AN ACCOUNT ─────────────────────────────
// Called from Step 7 before redirecting to Stripe Checkout.
// Returns { exists: true/false, hasSubscription: true/false }
app.post('/auth/check-email', authLimiter, async (req, res) => {
  try {
    const { email } = req.body;
    if (!email) return res.status(400).json({ error: 'Email required' });

    const normalizedEmail = email.toLowerCase().trim();

    // Check if a user profile exists for this email in our DB
    const { data: profile, error: profileErr } = await supabase
      .from('users')
      .select('id, email, status, stripe_customer_id')
      .eq('email', normalizedEmail)
      .maybeSingle();

    if (profileErr) {
      console.error('❌ check-email DB error:', profileErr.message);
      return res.status(500).json({ error: 'Lookup failed' });
    }

    const exists = !!profile;
    const hasSubscription = !!(profile && profile.stripe_customer_id);

    console.log(`ℹ️  check-email: ${normalizedEmail} → exists=${exists}, sub=${hasSubscription}`);
    res.json({ exists, hasSubscription });
  } catch (err) {
    console.error('❌ check-email error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── SEND MAGIC LINK (for "login instead of new signup" flow) ──────────
// Called when user realizes they already have an account and wants to log in.
app.post('/auth/send-login-link', authLimiter, async (req, res) => {
  try {
    const { email, lang } = req.body;
    if (!email) return res.status(400).json({ error: 'Email required' });

    const { data, error } = await supabase.auth.admin.generateLink({
      type: 'magiclink',
      email: email.toLowerCase().trim(),
      options: { redirectTo: `${FRONTEND_URL}/` },
    });

    if (error) {
      console.error('❌ generateLink failed:', error.message);
      return res.status(500).json({ error: 'Could not generate link' });
    }

    // Send the magic link via email
    const magicLink = data?.properties?.action_link;
    if (!magicLink) {
      return res.status(500).json({ error: 'No link generated' });
    }

    // Use user's language, or fall back to DB lookup, then 'en'
    let emailLang = (lang === 'de' || lang === 'en') ? lang : null;
    if (!emailLang) {
      try {
        const { data: user } = await supabase
          .from('users')
          .select('lang')
          .eq('email', email.toLowerCase().trim())
          .maybeSingle();
        if (user?.lang === 'de' || user?.lang === 'en') emailLang = user.lang;
      } catch (_) {}
    }
    const mail = buildMagicLinkEmail(magicLink, email, emailLang || 'en');
    await resend.emails.send({
      from: FROM_EMAIL,
      reply_to: REPLY_TO,
      to: email,
      subject: mail.subject,
      html: mail.html,
    });

    console.log(`✅ Login link sent to ${email} (${emailLang || 'en'})`);
    res.json({ success: true });
  } catch (err) {
    console.error('❌ send-login-link error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── FREE TIER SIGNUP ──────────────────────────────────────────────────
// Creates a user without Stripe: just auth user + profile with tier='free'.
// ── OTP LOGIN (PWA-FRIENDLY) ──────────────────────────────────────────
// Magic links break on iOS/Android PWAs because the link opens in the
// browser (separate storage context from the installed PWA). OTP codes
// work in any app/browser context: user enters a 6-digit code manually.
//
// Flow:
//   1. POST /auth/send-otp { email, lang }  → sends 6-digit code via email
//   2. POST /auth/verify-otp { email, code } → returns Supabase session tokens
//      on success. Frontend then calls supabase.auth.setSession(tokens).

function hashCode(code) {
  return crypto.createHash('sha256').update(String(code)).digest('hex');
}

function generateOTP() {
  // 6-digit, zero-padded, avoids leading-zero truncation issues
  return String(crypto.randomInt(0, 1000000)).padStart(6, '0');
}

app.post('/auth/send-otp', authLimiter, async (req, res) => {
  try {
    const { email, lang } = req.body || {};
    if (!email || typeof email !== 'string' || !email.includes('@')) {
      return res.status(400).json({ error: 'Valid email required' });
    }
    const normalizedEmail = email.trim().toLowerCase();
    const emailLang = (lang === 'de' || lang === 'en') ? lang : 'en';

    // ── ACCOUNT EXISTENCE CHECK ────────────────────────────────────────
    // Before sending an OTP we make sure an account actually exists for
    // this email. Saves the user from waiting for an email that says
    // "no account" — and saves us Resend credits.
    let existingUser = null;
    try {
      const { data } = await supabase
        .from('users')
        .select('status')
        .eq('email', normalizedEmail)
        .maybeSingle();
      existingUser = data;
    } catch (e) {
      console.warn('Account-existence check failed (fail-open):', e.message);
    }

    if (!existingUser) {
      console.log(`ℹ️  OTP refused (no account): ${normalizedEmail}`);
      return res.status(404).json({
        error: 'no_account',
        message: emailLang === 'de'
          ? 'Für diese E-Mail gibt es noch kein Konto. Möchtest du dich kostenlos anmelden?'
          : 'There is no account for this email yet. Want to sign up for free?',
      });
    }

    // Block users flagged for voucher abuse — no re-entry via OTP
    if (existingUser.status === 'blocked_voucher_abuse') {
      console.warn(`🚫 OTP blocked for abuse-flagged user: ${normalizedEmail}`);
      return res.status(403).json({
        error: 'account_blocked',
        message: emailLang === 'de'
          ? 'Dieser Account wurde wegen Verstoß gegen die Voucher-Regeln gesperrt. Bitte kontaktiere den Support.'
          : 'This account has been blocked due to voucher policy violation. Please contact support.',
      });
    }

    // Rate limit: max 3 codes per email per 15 minutes
    const fifteenMinAgo = new Date(Date.now() - 15 * 60 * 1000).toISOString();
    const { count: recentCount } = await supabase
      .from('login_codes')
      .select('id', { count: 'exact', head: true })
      .eq('email', normalizedEmail)
      .gte('created_at', fifteenMinAgo);

    if ((recentCount || 0) >= 3) {
      console.warn(`🚫 OTP rate limit for ${normalizedEmail}`);
      return res.status(429).json({
        error: 'rate_limit',
        message: emailLang === 'de'
          ? 'Zu viele Anfragen. Bitte warte 15 Minuten.'
          : 'Too many requests. Please wait 15 minutes.',
      });
    }

    // Generate code + store hash (never store plaintext)
    const code = generateOTP();
    const codeHash = hashCode(code);
    const expiresAt = new Date(Date.now() + 10 * 60 * 1000); // 10 min validity

    const { error: insertErr } = await supabase.from('login_codes').insert({
      email: normalizedEmail,
      code_hash: codeHash,
      expires_at: expiresAt.toISOString(),
    });
    if (insertErr) {
      console.error('❌ OTP insert failed:', insertErr.message);
      return res.status(500).json({ error: 'Could not create code' });
    }

    // Send email with code
    const mail = buildOtpEmail(code, normalizedEmail, emailLang);
    try {
      await resend.emails.send({
        from: FROM_EMAIL,
        reply_to: REPLY_TO,
        to: normalizedEmail,
        subject: mail.subject,
        html: mail.html,
        text: mail.text,
      });
    } catch (mailErr) {
      console.error('❌ OTP mail send failed:', mailErr.message);
      return res.status(500).json({ error: 'Could not send email' });
    }

    console.log(`📧 OTP sent to ${normalizedEmail} (expires in 10min)`);
    res.json({ ok: true, expiresIn: 600 });
  } catch (err) {
    console.error('❌ /auth/send-otp error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/auth/verify-otp', authLimiter, async (req, res) => {
  try {
    const { email, code } = req.body || {};
    if (!email || !code) {
      return res.status(400).json({ error: 'Email and code required' });
    }
    const normalizedEmail = String(email).trim().toLowerCase();
    const normalizedCode = String(code).trim().replace(/\s/g, '');
    if (!/^\d{6}$/.test(normalizedCode)) {
      return res.status(400).json({ error: 'Invalid code format', code: 'INVALID_CODE' });
    }

    // Load most recent unused, non-expired code for this email
    const { data: row } = await supabase
      .from('login_codes')
      .select('id, code_hash, expires_at, used_at, attempts')
      .eq('email', normalizedEmail)
      .is('used_at', null)
      .gt('expires_at', new Date().toISOString())
      .order('created_at', { ascending: false })
      .limit(1)
      .maybeSingle();

    if (!row) {
      return res.status(400).json({
        error: 'No valid code — request a new one',
        code: 'CODE_EXPIRED',
      });
    }

    // Brute-force protection: 5 attempts per code
    if (row.attempts >= 5) {
      await supabase
        .from('login_codes')
        .update({ used_at: new Date().toISOString() }) // mark as dead
        .eq('id', row.id);
      return res.status(400).json({
        error: 'Too many attempts — request a new code',
        code: 'TOO_MANY_ATTEMPTS',
      });
    }

    // Verify hash
    const providedHash = hashCode(normalizedCode);
    if (providedHash !== row.code_hash) {
      await supabase
        .from('login_codes')
        .update({ attempts: row.attempts + 1 })
        .eq('id', row.id);
      return res.status(400).json({
        error: 'Wrong code',
        code: 'WRONG_CODE',
        attemptsLeft: Math.max(0, 4 - row.attempts),
      });
    }

    // Code valid — mark as used, generate Supabase session
    await supabase
      .from('login_codes')
      .update({ used_at: new Date().toISOString() })
      .eq('id', row.id);

    // Find or create Supabase auth user
    let authUserId = null;
    try {
      const { data: created, error: createErr } = await supabase.auth.admin.createUser({
        email: normalizedEmail,
        email_confirm: true,
      });
      if (createErr) {
        // Already exists — look up by email
        if (/already been registered|already registered|exists/i.test(createErr.message)) {
          const match = await findAuthUserByEmail(normalizedEmail);
          if (!match) throw createErr;
          authUserId = match.id;
        } else {
          throw createErr;
        }
      } else {
        authUserId = created.user.id;
      }
    } catch (e) {
      console.error('❌ Auth user lookup/create failed:', e.message);
      return res.status(500).json({ error: 'Auth error' });
    }

    // Generate a magic-link token, then swap it for a full session.
    // generateLink with type='magiclink' gives us tokens we can pass to the frontend.
    const { data: linkData, error: linkErr } = await supabase.auth.admin.generateLink({
      type: 'magiclink',
      email: normalizedEmail,
    });
    if (linkErr || !linkData) {
      console.error('❌ generateLink failed:', linkErr?.message);
      return res.status(500).json({ error: 'Session generation failed' });
    }

    // Extract the OTP token (hashed_token) from the action link —
    // we can exchange it server-side for a session via verifyOtp.
    const hashedToken = linkData.properties?.hashed_token;
    if (!hashedToken) {
      console.error('❌ No hashed_token in generateLink response');
      return res.status(500).json({ error: 'Session token missing' });
    }

    const { data: verifyData, error: verifyErr } = await supabase.auth.verifyOtp({
      type: 'magiclink',
      token_hash: hashedToken,
    });
    if (verifyErr || !verifyData?.session) {
      console.error('❌ verifyOtp failed:', verifyErr?.message);
      return res.status(500).json({ error: 'Session exchange failed' });
    }

    console.log(`✅ OTP login success for ${normalizedEmail}`);
    res.json({
      ok: true,
      session: {
        access_token: verifyData.session.access_token,
        refresh_token: verifyData.session.refresh_token,
      },
      userId: authUserId,
    });
  } catch (err) {
    console.error('❌ /auth/verify-otp error:', err.message);
    res.status(500).json({ error: err.message });
  }
});


// Sends magic link to log them in. No payment, no Stripe customer.
//
// Free tier limits (enforced elsewhere):
//   - max 3 AI plan generations (tracked via plan_generations_used)
//   - no training progression
//   - no recovery tools
//   - no workout adjustments
app.post('/auth/signup-free', authLimiter, async (req, res) => {
  try {
    const { email, userData, consent, lang } = req.body;

    if (!email) return res.status(400).json({ error: 'Email is required' });

    if (!consent || consent.healthData !== true || consent.terms !== true) {
      console.warn(`⚠️ Free signup blocked for ${email}: missing GDPR consent`);
      return res.status(400).json({ error: 'Consent required' });
    }

    // ── AGE GATE (GDPR §8 BDSG: minimum 16) ────────────────────────────
    const ageNum = parseInt(userData && userData.age, 10);
    if (!ageNum || ageNum < 16 || ageNum > 120) {
      console.warn(`⚠️ Free signup blocked for ${email}: invalid age (${userData && userData.age})`);
      return res.status(400).json({
        error: 'AGE_RESTRICTION',
        message: lang === 'de'
          ? 'PEAK ist ab 16 Jahren verfügbar.'
          : 'PEAK is available from age 16.'
      });
    }

    const normalizedEmail = email.toLowerCase().trim();

    // Check if account already exists in auth.users
    let existingAuthUserId = null;
    try {
      const match = await findAuthUserByEmail(normalizedEmail);
      if (match) {
        // Auth user exists — but does the public.users profile row exist too?
        // If we early-return here without checking, users who only have an auth
        // record (e.g. half-completed signup, profile manually deleted, schema
        // migration leftover) would receive a magic link, log in, and hit a
        // 404 on /user/profile — which the frontend renders as "Noch kein Abo"
        // (the bug Nick reported on 03.05.2026).
        //
        // Resolution: load the profile. If absent, fall through to the upsert
        // below — but use the EXISTING auth user id instead of creating a new
        // auth user (which would fail with "user already registered").
        const { data: existingProfile } = await supabase
          .from('users')
          .select('id, tier, stripe_subscription_id')
          .eq('id', match.id)
          .maybeSingle();

        if (existingProfile) {
          // Both auth + profile exist → just send a login link, don't re-upsert.
          console.log(`ℹ️  Free signup for existing user: ${normalizedEmail} → sending login link`);
          const { data: linkData } = await supabase.auth.admin.generateLink({
            type: 'magiclink',
            email: normalizedEmail,
            options: { redirectTo: `${FRONTEND_URL}/` },
          });
          const magicLink = linkData?.properties?.action_link;
          if (magicLink) {
            const mail = buildMagicLinkEmail(magicLink, normalizedEmail, lang === 'de' ? 'de' : 'en');
            await resend.emails.send({
              from: FROM_EMAIL,
              reply_to: REPLY_TO,
              to: normalizedEmail,
              subject: mail.subject,
              html: mail.html,
            });
          }
          return res.json({ success: true, existing: true });
        }

        // Auth user exists but no profile → reuse the auth id and fall through
        // to the upsert path below to create the missing profile row.
        console.warn(`⚠️  Repairing orphaned auth user: ${normalizedEmail} (${match.id}) — auth exists, profile missing`);
        existingAuthUserId = match.id;
      }
    } catch (e) {
      console.warn('User-existence check failed, proceeding with signup:', e.message);
    }

    // Create new auth user only if we don't already have one for this email.
    let authUserId;
    if (existingAuthUserId) {
      authUserId = existingAuthUserId;
    } else {
      const { data: created, error: createErr } = await supabase.auth.admin.createUser({
        email: normalizedEmail,
        email_confirm: true,
        user_metadata: {
          name: userData?.name || '',
          source: 'free_signup',
        },
      });

      if (createErr) {
        console.error('❌ Free auth user creation failed:', createErr.message);
        return res.status(500).json({ error: createErr.message });
      }

      authUserId = created.user.id;
    }

    const consentAt = consent.at || new Date().toISOString();

    // Upsert free profile row
    const userRow = {
      id: authUserId,
      email: normalizedEmail,
      name: userData?.name || '',
      age: userData?.age ? parseInt(userData.age) : null,
      gender: userData?.gender || null,
      weight: userData?.weight ? parseFloat(userData.weight) : null,
      dweight: userData?.dweight ? parseFloat(userData.dweight) : null,
      height: userData?.height ? parseFloat(userData.height) : null,
      sleep: userData?.sleep ? parseFloat(userData.sleep) : null,
      job: userData?.job || null,
      commute: userData?.commute || null,
      stress: userData?.stress ? parseFloat(userData.stress) : null,
      level: userData?.level || null,
      sessions: userData?.sessions ? parseInt(userData.sessions) : null,
      dur: userData?.dur ? parseInt(userData.dur) : null,
      equip: userData?.equip || null,
      al: Array.isArray(userData?.al) ? userData.al : [],
      di: Array.isArray(userData?.di) ? userData.di : [],
      cu: Array.isArray(userData?.cu) ? userData.cu : [],
      cook: userData?.cook || null,
      budget: userData?.budget ? parseFloat(userData.budget) : null,
      stretch_areas: Array.isArray(userData?.stretchAreas) ? userData.stretchAreas : [],
      stretch_dur: userData?.stretchDur ? parseInt(userData.stretchDur) : 10,
      train_days: Array.isArray(userData?.trainDays)
        ? userData.trainDays.filter(d => Number.isInteger(d) && d >= 0 && d <= 6).slice(0, 7)
        : [],
      stripe_customer_id: null,
      stripe_subscription_id: null,
      plan: 'free',
      tier: 'free',
      goal: userData?.goal || '',
      goals: Array.isArray(userData?.goals) && userData.goals.length ? userData.goals : (userData?.goal ? [userData.goal] : []),
      sport: userData?.sport || '',
      lang: (lang === 'de' || lang === 'en') ? lang : 'en',
      trial_start: null,
      trial_end: null,
      status: 'free_active',
      unsubscribed: false,
      plan_generations_used: 0,
      plan_generations_window_start: new Date().toISOString(),
      consent_health_data: true,
      consent_terms: true,
      consent_at: consentAt,
      analytics_optin: consent && consent.analytics === true,
    };

    // Before upsert: check if a profile already exists for this auth id.
    // If yes AND it's already on a paid tier, abort — return existing state.
    try {
      const { data: prior } = await supabase
        .from('users')
        .select('tier, stripe_subscription_id')
        .eq('id', authUserId)
        .maybeSingle();
      if (prior && (prior.stripe_subscription_id || (prior.tier && prior.tier !== 'free'))) {
        console.warn(`⚠️  Free signup attempted for existing paid user: ${normalizedEmail}. Sending login link instead.`);
        const { data: linkData } = await supabase.auth.admin.generateLink({
          type: 'magiclink',
          email: normalizedEmail,
          options: { redirectTo: `${FRONTEND_URL}/` },
        });
        const magicLink = linkData?.properties?.action_link;
        if (magicLink) {
          const mail = buildMagicLinkEmail(magicLink, normalizedEmail, lang === 'de' ? 'de' : 'en');
          await resend.emails.send({
            from: FROM_EMAIL,
            reply_to: REPLY_TO,
            to: normalizedEmail,
            subject: mail.subject,
            html: mail.html,
          });
        }
        return res.json({ success: true, existing: true });
      }
    } catch (e) {
      console.warn('Prior-profile check failed:', e.message);
    }

    const { error: upsertErr } = await supabase.from('users').upsert(userRow, {
      onConflict: 'id',
    });
    if (upsertErr) {
      console.error('❌ Free user upsert failed:', upsertErr.message);
      return res.status(500).json({ error: upsertErr.message });
    }

    // Generate TWO magic links:
    //   1. magicLinkAuto — returned to the client, consumed immediately by
    //      window.location.href = magicLinkAuto in the onboarding flow.
    //      One-time use, dies on first click.
    //   2. magicLinkEmail — embedded in the welcome email button. Must stay
    //      valid because users frequently open PEAK on a second device
    //      (phone after onboarding on laptop, or vice versa). If we used
    //      the same link for both, the auto-login would already have
    //      consumed it by the time the email arrives — user clicks the
    //      mail button → Supabase rejects with "link expired" → user is
    //      bounced to the login screen and has to do OTP. This was the
    //      Free-signup double-login bug reported May 12.
    const { data: linkAutoData } = await supabase.auth.admin.generateLink({
      type: 'magiclink',
      email: normalizedEmail,
      options: { redirectTo: `${FRONTEND_URL}/` },
    });
    const magicLinkAuto = linkAutoData?.properties?.action_link || null;

    const { data: linkEmailData } = await supabase.auth.admin.generateLink({
      type: 'magiclink',
      email: normalizedEmail,
      options: { redirectTo: `${FRONTEND_URL}/` },
    });
    const magicLinkEmail = linkEmailData?.properties?.action_link || null;

    try {
      await sendEmail(normalizedEmail, 'welcome', {
        name: userData?.name || '',
        goal: userData?.goal || '',
        goals: Array.isArray(userData?.goals) ? userData.goals : [],
        sport: userData?.sport || '',
        magicLink: magicLinkEmail,
        isFree: true,
        tier: 'free',
        lang: lang === 'de' ? 'de' : (lang === 'en' ? 'en' : undefined),
      });
    } catch (err) {
      console.error('⚠️  Free welcome email failed:', err.message);
    }

    console.log(`✅ Free signup complete: ${normalizedEmail} (${authUserId})`);
    // Return the auto-login link to the client. The email link is the
    // backup and stays valid independently.
    res.json({ success: true, userId: authUserId, magicLink: magicLinkAuto });
  } catch (err) {
    console.error('❌ signup-free error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── OPEN STRIPE CUSTOMER PORTAL ───────────────────────────────────────
// Generates a one-time portal session URL for the given email.
// User is redirected there to manage/cancel their subscription.
app.post('/customer-portal', authLimiter, async (req, res) => {
  try {
    const { email } = req.body;
    if (!email) return res.status(400).json({ error: 'Email required' });

    // ─── AUTH CHECK ──────────────────────────────────────────────────
    // Verify the requester owns this email. Without this, any visitor
    // could open a Stripe billing portal session for any customer email
    // and view/cancel their subscription. CRITICAL fix (Apr 2026).
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ error: 'Authentication required' });
    }
    const token = authHeader.slice(7);
    const { data: authData, error: authErr } = await supabase.auth.getUser(token);
    if (authErr || !authData?.user?.email) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    // Email must match the authenticated user's email (case-insensitive).
    if (authData.user.email.toLowerCase() !== email.toLowerCase().trim()) {
      console.warn(`🚫 customer-portal email mismatch: token=${authData.user.email}, requested=${email}`);
      return res.status(403).json({ error: 'Email does not match authenticated user' });
    }

    const { data: profile, error: profileErr } = await supabase
      .from('users')
      .select('stripe_customer_id')
      .eq('email', email.toLowerCase().trim())
      .maybeSingle();

    if (profileErr || !profile?.stripe_customer_id) {
      return res.status(404).json({ error: 'No subscription found for this email' });
    }

    let portalSession;
    try {
      portalSession = await stripe.billingPortal.sessions.create({
        customer: profile.stripe_customer_id,
        return_url: FRONTEND_URL,
      });
    } catch (stripeErr) {
      // Customer may have been deleted in Stripe Dashboard but still exists in our DB.
      // Clean up the dangling reference + inform the user.
      if (stripeErr?.message?.includes('No such customer')) {
        console.warn(`⚠️  Dangling stripe_customer_id for ${email}: ${profile.stripe_customer_id}. Clearing.`);
        await supabase.from('users').update({
          stripe_customer_id: null,
          stripe_subscription_id: null,
          tier: 'free',
          status: 'cancelled',
          trial_end: null,
        }).eq('email', email.toLowerCase().trim());
        return res.status(410).json({
          error: 'subscription_not_found',
          code: 'SUBSCRIPTION_NOT_FOUND',
          message: 'Your subscription no longer exists. Your account has been reset to Free.',
        });
      }
      throw stripeErr;
    }

    console.log(`✅ Portal session created for ${email}`);
    res.json({ url: portalSession.url });
  } catch (err) {
    console.error('❌ customer-portal error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── SHARING ENDPOINTS ─────────────────────────────────────────────────
// Users on Basic+ can share individual recipes, workouts and stretch
// routines via a short link. Flow:
//   1. Frontend POSTs to /share with {type, payload}
//   2. Backend generates a short ID, stores payload + expiry in
//      `shared_content` table, returns {url}
//   3. Recipient opens FRONTEND_URL/?share=ID
//   4. Frontend reads the ?share= param, calls /share/:id/data, and
//      shows the content in a modal
//
// Expiry: 30 days. Stored payload is a JSON snapshot (recipes/workouts
// are user-mutable, we freeze them at share time). Cron job sweeps
// expired rows weekly (see cleanup_shared_content cron).

// Generates a URL-safe short ID for share links. 8 characters from a
// 62-char alphabet → 62^8 ≈ 2.18 × 10^14 unique IDs. Collisions are
// astronomically unlikely at our scale; we don't bother with retry-on-
// collision because the chance is ~0 even after a billion shares.
function generateShareId() {
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let id = '';
  for (let i = 0; i < 8; i++) {
    id += alphabet[Math.floor(Math.random() * alphabet.length)];
  }
  return id;
}

// POST /share — create a new share entry
// Auth: optional. Email is taken from the JWT if present, else from the
// body. Free-tier users can't share (tier check below); Basic+ can.
app.post('/share', authLimiter, async (req, res) => {
  try {
    const { type, payload, email: bodyEmail, lang } = req.body || {};
    // Validate type
    if (!type || !['recipe', 'workout', 'stretch'].includes(type)) {
      return res.status(400).json({ error: 'invalid type' });
    }
    if (!payload || typeof payload !== 'object') {
      return res.status(400).json({ error: 'payload required' });
    }
    // Resolve user (optional auth — if a Bearer token is present, use it
    // to find the authenticated user; otherwise fall back to body.email).
    let email = bodyEmail ? String(bodyEmail).toLowerCase().trim() : null;
    let authedUser = null;
    const authHeader = req.headers.authorization;
    if (authHeader && authHeader.startsWith('Bearer ')) {
      const token = authHeader.slice(7);
      try {
        const { data: { user } } = await supabase.auth.getUser(token);
        if (user) {
          authedUser = user;
          email = user.email.toLowerCase().trim();
        }
      } catch (_) {}
    }
    if (!email) {
      return res.status(401).json({ error: 'authentication required' });
    }
    // Tier-gate: only basic+ can share. Free users get a 402 so the
    // frontend can show the upgrade prompt.
    const { data: userRow } = await supabase
      .from('users')
      .select('tier, status')
      .eq('email', email)
      .maybeSingle();
    if (!userRow) {
      return res.status(404).json({ error: 'user not found' });
    }
    if (userRow.tier === 'free') {
      return res.status(402).json({ error: 'sharing requires Basic or Premium' });
    }
    // Insert with retry-on-collision (rare but possible)
    const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000); // 30 days
    let id, insertErr;
    for (let attempt = 0; attempt < 5; attempt++) {
      id = generateShareId();
      const { error } = await supabase.from('shared_content').insert({
        id,
        type,
        payload,
        creator_email: email,
        creator_tier: userRow.tier,
        expires_at: expiresAt.toISOString(),
      });
      if (!error) { insertErr = null; break; }
      // 23505 = unique violation → retry with new ID. Anything else → abort.
      if (error.code !== '23505') { insertErr = error; break; }
      insertErr = error;
    }
    if (insertErr) {
      console.error('❌ /share insert failed:', insertErr.message);
      return res.status(500).json({ error: 'could not create share link' });
    }
    const url = `${FRONTEND_URL}/?share=${id}`;
    console.log(`📤 Share created: ${type} by ${email} → ${id}`);
    res.json({ success: true, id, url });
  } catch (err) {
    console.error('❌ /share error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /share/:id — redirect to the frontend with ?share= param.
// Useful if someone shares the bare /share/ID URL instead of the
// /?share=ID frontend URL. We don't render HTML server-side — the
// frontend handles the share modal once loaded.
app.get('/share/:id', (req, res) => {
  const id = req.params.id;
  if (!/^[A-Za-z0-9]{4,16}$/.test(id)) {
    return res.status(400).send('invalid share id');
  }
  res.redirect(302, `${FRONTEND_URL}/?share=${id}`);
});

// GET /share/:id/data — return the stored payload as JSON.
// Public endpoint (no auth needed — anyone with the link can view).
// Returns 404 if expired or not found.
app.get('/share/:id/data', async (req, res) => {
  try {
    const id = req.params.id;
    if (!/^[A-Za-z0-9]{4,16}$/.test(id)) {
      return res.status(400).json({ error: 'invalid id' });
    }
    const { data, error } = await supabase
      .from('shared_content')
      .select('id, type, payload, expires_at')
      .eq('id', id)
      .maybeSingle();
    if (error) {
      console.error('❌ /share/:id/data error:', error.message);
      return res.status(500).json({ error: error.message });
    }
    if (!data) {
      return res.status(404).json({ error: 'not found' });
    }
    // Expired? Treat as 404 so the frontend renders the "expired" state
    // the same way as missing rows. Cron will sweep these eventually.
    if (data.expires_at && new Date(data.expires_at) < new Date()) {
      return res.status(404).json({ error: 'expired' });
    }
    res.json({ type: data.type, payload: data.payload });
  } catch (err) {
    console.error('❌ /share/:id/data error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── AI PROXY ──────────────────────────────────────────────────────────
// Frontend can't call Anthropic directly (CORS + API key must stay server-side).
// This endpoint proxies requests. Max tokens clamped 100-2000 to prevent abuse.
app.post('/ai/generate', aiLimiter, async (req, res) => {
  try {
    const { prompt, max_tokens, purpose } = req.body;
    if (!prompt || typeof prompt !== 'string') {
      return res.status(400).json({ error: 'prompt required' });
    }

    // ─── AUTH RESOLUTION ────────────────────────────────────────────────
    // Auth is OPTIONAL on this endpoint because the very first plan
    // generation happens during onboarding before signup completes.
    // BUT: anonymous users get tighter restrictions (see below) — this
    // closes the previous quota-bypass where unauthenticated callers
    // could bypass per-user plan-generation limits entirely.
    const authHeader = req.headers.authorization;
    let userEmail = 'anonymous';
    let authUserId = null;
    if (authHeader && authHeader.startsWith('Bearer ')) {
      try {
        const token = authHeader.slice(7);
        const { data } = await supabase.auth.getUser(token);
        if (data?.user?.email) userEmail = data.user.email;
        if (data?.user?.id) authUserId = data.user.id;
      } catch (_) { /* ignore, treat as anonymous onboarding */ }
    }

    // ─── ANONYMOUS GUARDRAILS (Apr 2026 hardening) ─────────────────────
    // Block anonymous abuse vectors:
    //   1. plan_generation requires auth — onboarding flow signs up first
    //      then calls this endpoint, so authUserId should always be set
    //      for purpose='plan_generation'. The OLD onboarding flow that
    //      called this anonymously was a bug.
    //   2. plan_generation_initial is the new free first-time slot used
    //      during onboarding before/right after signup. Allow anonymous
    //      but strictly via the aiLimiter (60/10min per IP).
    //   3. Everything else (training_enrich, recipe, mood_recipe, etc.)
    //      requires auth — these are post-onboarding features.
    if (!authUserId) {
      // Allowlist of purposes safe to run anonymously (rate-limited by IP).
      const ANONYMOUS_PURPOSES = new Set([
        'plan_generation_initial',  // first plan during signup flow
        'session_translate',         // translation helper, no DB writes
        null, undefined, ''         // legacy calls without explicit purpose
      ]);
      if (!ANONYMOUS_PURPOSES.has(purpose)) {
        console.warn(`🚫 Anonymous AI call blocked: purpose=${purpose} from IP=${req.ip}`);
        return res.status(401).json({
          error: 'Authentication required for this operation',
          code: 'AUTH_REQUIRED',
        });
      }
    }

    // ─── BLOCK CHECK ──────────────────────────────────────────────────────
    // Users flagged for voucher abuse can't use AI features even if they
    // still have an active session from before the block.
    if (authUserId) {
      try {
        const { data: blockCheck } = await supabase
          .from('users')
          .select('status')
          .eq('id', authUserId)
          .maybeSingle();
        if (blockCheck?.status === 'blocked_voucher_abuse') {
          console.warn(`🚫 AI call blocked for abuse-flagged user: ${userEmail}`);
          return res.status(403).json({
            error: 'account_blocked',
            code: 'ACCOUNT_BLOCKED',
            message: 'Account has been blocked due to policy violation.',
          });
        }
      } catch (e) {
        console.warn('AI block-check failed (fail-open):', e.message);
      }
    }

    // ─── PREMIUM-ONLY PURPOSES (Apr 2026 hardening) ───────────────────
    // Some AI flows are advertised as Premium-only in the frontend
    // (recipe generation, mood-based recipe swap, quick-log via /ai/generate).
    // Lock them down server-side too — frontend gating alone could be
    // bypassed by anyone with a logged-in token + the API URL.
    const PREMIUM_PURPOSES = new Set(['recipe', 'mood_recipe', 'recipe_builder']);
    if (PREMIUM_PURPOSES.has(purpose)) {
      if (!authUserId) {
        return res.status(401).json({ error: 'auth_required', code: 'AUTH_REQUIRED' });
      }
      try {
        const { data: u } = await supabase
          .from('users').select('tier').eq('id', authUserId).maybeSingle();
        if (!u || u.tier !== 'premium') {
          console.warn(`🚫 Premium-only AI call from non-premium user: ${userEmail} (purpose=${purpose})`);
          return res.status(403).json({
            error: 'premium_required',
            code: 'PREMIUM_REQUIRED',
          });
        }
      } catch (e) {
        // Fail-closed: a DB hiccup must not let a Free user through.
        console.error('Premium tier check failed:', e.message);
        return res.status(500).json({ error: 'tier_check_failed', code: 'TIER_CHECK_FAILED' });
      }
    }

    // ─── MONTHLY PLAN-GENERATION LIMITS ───────────────────────────────
    // Only enforced when purpose === 'plan_generation' AND user is authenticated.
    // Other purposes (training_enrich, adjustment) are NOT counted (internal
    // follow-up calls).
    //
    // Limits per tier (rolling 30-day window):
    //   free    → 1 plan generation / 30 days
    //   basic   → 5 plan generations / 30 days
    //   premium → unlimited
    //
    // Window resets when 30 days pass since plan_generations_window_start.
    if (purpose === 'plan_generation' && authUserId) {
      try {
        const { data: profile } = await supabase
          .from('users')
          .select('tier, plan_generations_used, plan_generations_window_start')
          .eq('id', authUserId)
          .maybeSingle();

        const tier = profile?.tier || 'free';
        const LIMITS = { free: 1, basic: 5, premium: Infinity };
        const limit = LIMITS[tier] != null ? LIMITS[tier] : 0;

        if (limit !== Infinity) {
          // Check + roll window if needed
          const now = new Date();
          let windowStart = profile?.plan_generations_window_start
            ? new Date(profile.plan_generations_window_start)
            : null;
          let used = profile?.plan_generations_used || 0;
          const THIRTY_DAYS_MS = 30 * 24 * 60 * 60 * 1000;
          const isExpired = !windowStart || (now - windowStart) > THIRTY_DAYS_MS;

          if (isExpired) {
            // Reset window
            windowStart = now;
            used = 0;
          }

          if (used >= limit) {
            const resetAt = new Date(windowStart.getTime() + THIRTY_DAYS_MS);
            const daysLeft = Math.max(1, Math.ceil((resetAt - now) / (24 * 60 * 60 * 1000)));
            console.log(`🔒 ${tier}-tier monthly cap hit for ${userEmail} (used=${used}/${limit}, reset in ${daysLeft}d)`);
            return res.status(402).json({
              error: 'Monthly plan limit reached',
              code: 'FREE_LIMIT_REACHED',
              tier,
              used,
              max: limit,
              daysUntilReset: daysLeft,
              resetAt: resetAt.toISOString(),
            });
          }

          // Increment BEFORE the call (prevents abuse via retries)
          await supabase
            .from('users')
            .update({
              plan_generations_used: used + 1,
              plan_generations_window_start: windowStart.toISOString(),
            })
            .eq('id', authUserId);
          console.log(`📊 ${tier} user ${userEmail}: plan_generations ${used+1}/${limit}`);
        }
      } catch (e) {
        console.warn('Plan-gen cap check failed (fail-open):', e.message);
      }
    }

    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) {
      console.error('❌ ANTHROPIC_API_KEY not set');
      return res.status(500).json({ error: 'AI service not configured' });
    }

    const modelName = process.env.ANTHROPIC_MODEL || 'claude-opus-4-7';
    // Token cap: 2000 was the legacy default for single-meal/day plans.
    // Raised to 12000 so the 14-day meal pool (≈9k output) fits with
    // headroom. 12k is a deliberate ceiling — anything over that signals
    // a malformed prompt and we'd rather fail-fast than burn tokens.
    const tokens = Math.min(Math.max(parseInt(max_tokens) || 800, 100), 12000);

    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: modelName,
        max_tokens: tokens,
        messages: [{ role: 'user', content: prompt }],
      }),
    });

    if (!r.ok) {
      const errText = await r.text();
      console.error(`❌ Anthropic API ${r.status} (model=${modelName}) for ${userEmail}:`, errText.slice(0, 400));
      return res.status(502).json({ error: 'AI service error' });
    }

    const data = await r.json();
    const text = data?.content?.[0]?.text;
    if (!text) {
      console.error('❌ Empty Anthropic response for', userEmail);
      return res.status(502).json({ error: 'Empty AI response' });
    }

    console.log(`✅ AI call OK for ${userEmail} (${tokens} tokens, ${text.length} chars, purpose=${purpose||'unknown'})`);
    res.json({ text });
  } catch (err) {
    console.error('❌ /ai/generate error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── SCAN: MENU PHOTO (Claude Vision) ─────────────────────────────────
// User uploads a photo of a restaurant menu. Claude Haiku Vision reads
// the menu and returns the top 3 dishes that best fit the user's goals,
// with estimated kcal/protein and a fit-rating.
app.post('/ai/scan-menu', aiLimiter, async (req, res) => {
  try {
    const { image, mediaType, userGoal, userLang } = req.body;
    if (!image || typeof image !== 'string') {
      return res.status(400).json({ error: 'image required (base64 data)' });
    }
    const mt = (mediaType && /^image\/(jpeg|png|webp|gif)$/.test(mediaType)) ? mediaType : 'image/jpeg';

    // ── PREMIUM-ONLY (Apr 2026 hardening) ─────────────────────────────
    // Scanner is a Premium feature. Frontend gates it (tSc: isFree → upgrade
    // screen, isBasic → upgrade screen) but the API was open before, so a
    // logged-in Free user with a valid token could bypass the UI gate and
    // burn AI credits. Enforce server-side now.
    const auth = await resolveAuthAndTier(req, { requirePremium: true });
    if (!auth.ok) return res.status(auth.status).json(auth.body);
    const userEmail = auth.email || 'unknown';
    const authUserId = auth.userId;

    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) return res.status(500).json({ error: 'AI not configured' });

    const de = userLang === 'de';
    const goalHint = userGoal ? (de ? `Ziel des Nutzers: ${userGoal}.` : `User goal: ${userGoal}.`) : '';
    const prompt = de
      ? `Du siehst ein Foto einer Speisekarte. Wähle die 3 Gerichte die am BESTEN zum Ziel des Nutzers passen. ${goalHint}

KALORIEN-SCHÄTZUNG (sei ehrlich, nicht schönrechnen):
- Deutsche/europäische Restaurantportionen sind GROSS (Fleisch 250-400g, Beilagen 200-300g)
- Pommes: +380-450 kcal, Reis: +320-380 kcal, Kartoffeln: +280-340 kcal, Spätzle: +400 kcal
- Panade + Frittieren: +200-300 kcal auf das Grundfleisch
- Sahnesauce, Rahmsauce, Käseüberbackung: +200-300 kcal
- Typische Gesamtwerte: Schnitzel mit Pommes = 1000-1300 kcal, Steak mit Beilagen = 900-1100 kcal, Salat mit Hähnchen = 450-650 kcal
- Grillgerichte ohne Panade/Sauce sind am kalorienärmsten
- Sei lieber realistisch-hoch als zu niedrig

PROTEIN-SCHÄTZUNG:
- 100g mageres Fleisch (Huhn/Pute/Rind) ≈ 25-30g Protein
- 100g fetteres Fleisch (Schwein/Lamm) ≈ 20-25g Protein
- 100g Fisch ≈ 20-25g Protein (Lachs ~22g, magerer Fisch ~20g)
- Panade senkt Protein-Anteil pro Gramm Gericht
- Typische Restaurantportion Fleisch: 200-300g

FIT-BEWERTUNG (strikt anwenden!):
- "best" = mageres gegrilltes/gekochtes Protein, wenig Fett, viel Eiweiß, wenig Kohlenhydrate aus Panade oder Sauce. Beispiele: Gegrillter Fisch, Steak ohne Sauce, gegrilltes Hähnchen, Salat mit magerem Fleisch.
- "good" = gutes Protein aber mit Beilagen/Sauce/leichter Panade. Beispiele: Jägerschnitzel mit Pommes, Gyros-Teller, Lachs mit Kartoffeln.
- "ok" = viel Panade, Frittiertes, schwere Saucen, Überbacken, viel Kohlenhydrate im Verhältnis zum Protein. Beispiele: Hawaiischnitzel überbacken, Rahmschnitzel, Risotto.
- Wenn das Ziel Muskelaufbau/Performance ist: bevorzuge hohe Protein-Dichte (Protein/kcal). Paniertes Schnitzel ist NIE "best".
- Wenn das Ziel Gewichtsabnahme ist: bevorzuge niedrige Kalorien + hohes Protein.

Antworte AUSSCHLIESSLICH als JSON (kein Markdown):
{"dishes":[{"name":"...","kcal":<zahl>,"protein":<zahl>,"fit":"best"|"good"|"ok","reason":"kurzer Grund max 8 Wörter"}]}
Falls kein Menü erkennbar ist: {"dishes":[],"error":"no_menu"}.`
      : `You see a photo of a restaurant menu. Pick the 3 dishes that BEST match the user's goal. ${goalHint}

CALORIE ESTIMATION (be honest, don't undersell):
- European/German restaurant portions are LARGE (meat 250-400g, sides 200-300g)
- Fries: +380-450 kcal, rice: +320-380 kcal, potatoes: +280-340 kcal, spätzle: +400 kcal
- Breading + frying: +200-300 kcal on top of the base meat
- Cream sauce, cheese topping: +200-300 kcal
- Typical totals: schnitzel with fries = 1000-1300 kcal, steak with sides = 900-1100 kcal, salad with chicken = 450-650 kcal
- Grilled dishes without breading/sauce are the lowest-calorie option
- Err on the realistic-high side

PROTEIN ESTIMATION:
- 100g lean meat (chicken/turkey/beef) ≈ 25-30g protein
- 100g fattier meat (pork/lamb) ≈ 20-25g protein
- 100g fish ≈ 20-25g protein (salmon ~22g, lean fish ~20g)
- Breading lowers protein ratio per gram of dish
- Typical restaurant meat portion: 200-300g

FIT RATING (apply strictly!):
- "best" = lean grilled/cooked protein, low fat, high protein, low carbs from breading/sauce. Examples: grilled fish, steak without sauce, grilled chicken, salad with lean meat.
- "good" = good protein but with sides/sauce/light breading. Examples: hunter schnitzel with fries, gyros plate, salmon with potatoes.
- "ok" = heavy breading, fried, heavy sauces, cheese-topped, high carb-to-protein ratio. Examples: Hawaiian schnitzel topped, cream schnitzel, risotto.
- If goal is muscle building/performance: prefer high protein density (protein/kcal). Breaded schnitzel is NEVER "best".
- If goal is weight loss: prefer low calories + high protein.

Respond ONLY as JSON (no markdown):
{"dishes":[{"name":"...","kcal":<number>,"protein":<number>,"fit":"best"|"good"|"ok","reason":"short reason max 8 words"}]}
If no menu is visible: {"dishes":[],"error":"no_menu"}.`;

    const modelName = process.env.ANTHROPIC_MODEL || 'claude-haiku-4-5-20251001';
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: modelName,
        max_tokens: 600,
        messages: [{
          role: 'user',
          content: [
            { type: 'image', source: { type: 'base64', media_type: mt, data: image } },
            { type: 'text', text: prompt },
          ],
        }],
      }),
    });

    if (!r.ok) {
      const errText = await r.text();
      console.error(`❌ scan-menu Anthropic ${r.status}:`, errText.slice(0, 300));
      return res.status(502).json({ error: 'AI service error' });
    }

    const data = await r.json();
    const text = data?.content?.[0]?.text || '';
    // Strip any accidental markdown fences
    const clean = text.replace(/^```json\s*|```$/g, '').trim();
    let parsed;
    try { parsed = JSON.parse(clean); }
    catch (e) {
      console.error(`❌ scan-menu JSON parse failed for ${userEmail}:`, clean.slice(0, 200));
      return res.status(502).json({ error: 'Could not read menu' });
    }

    console.log(`✅ scan-menu OK for ${userEmail}: ${(parsed.dishes||[]).length} dishes`);
    res.json(parsed);
  } catch (err) {
    console.error('❌ /ai/scan-menu error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── SCAN: BARCODE (Open Food Facts proxy + fit rating) ──────────────
// Frontend sends a barcode string. We look up Open Food Facts directly
// (they have great EU/DE coverage, free, no API key) and add a "fit"
// rating based on the user's goal.
app.post('/ai/scan-barcode', aiLimiter, async (req, res) => {
  try {
    const { barcode, userGoal, userLang } = req.body;
    if (!barcode || !/^\d{6,14}$/.test(String(barcode))) {
      return res.status(400).json({ error: 'valid numeric barcode required' });
    }

    // ── PREMIUM-ONLY (Apr 2026 hardening) ─────────────────────────────
    // Same rationale as /ai/scan-menu: Scanner is a Premium feature, the
    // frontend gates it but the API was open. Locked down server-side now.
    const auth = await resolveAuthAndTier(req, { requirePremium: true });
    if (!auth.ok) return res.status(auth.status).json(auth.body);
    const userEmail = auth.email || 'unknown';
    const authUserId = auth.userId;

    // Look up Open Food Facts
    const offUrl = `https://world.openfoodfacts.org/api/v2/product/${encodeURIComponent(barcode)}.json`;
    const offRes = await fetch(offUrl, { headers: { 'User-Agent': 'PEAK-by-MJ-Performance/1.0 (support@mj-performance.net)' } });
    if (!offRes.ok) {
      console.warn(`⚠️ Open Food Facts error ${offRes.status} for ${barcode}`);
      return res.status(502).json({ error: 'lookup_failed' });
    }
    const offData = await offRes.json();
    if (offData.status !== 1 || !offData.product) {
      console.log(`ℹ️ Barcode ${barcode} not found in OFF (asked by ${userEmail})`);
      return res.status(404).json({ error: 'product_not_found', barcode });
    }

    const p = offData.product;
    const per100 = p.nutriments || {};
    // Best-effort portion sizing: if serving_size given, use that; else 100g
    const servingG = parseFloat(p.serving_quantity) || 100;
    const factor = servingG / 100;

    const name = p.product_name || p.product_name_en || p.product_name_de || p.generic_name || 'Unknown';
    const brand = (p.brands || '').split(',')[0].trim() || '';
    const kcalPer100 = per100['energy-kcal_100g'] || per100['energy-kcal'] || (per100['energy_100g'] ? per100['energy_100g']/4.184 : 0);
    const proteinPer100 = per100['proteins_100g'] || 0;
    const carbsPer100 = per100['carbohydrates_100g'] || 0;
    const sugarsPer100 = per100['sugars_100g'] || 0;
    const fatPer100 = per100['fat_100g'] || 0;
    const satFatPer100 = per100['saturated-fat_100g'] || 0;

    const round = n => Math.round(n * 10) / 10;
    const product = {
      barcode,
      name,
      brand,
      serving_g: Math.round(servingG),
      kcal: Math.round(kcalPer100 * factor),
      protein: round(proteinPer100 * factor),
      carbs: round(carbsPer100 * factor),
      sugars: round(sugarsPer100 * factor),
      fat: round(fatPer100 * factor),
      saturated_fat: round(satFatPer100 * factor),
      nutri_score: (p.nutriscore_grade || '').toUpperCase() || null,
      image: p.image_small_url || p.image_thumb_url || null,
    };

    // Simple fit-rating — not AI, just heuristics (saves a roundtrip)
    // "fit"=good match, "caution"=high sugar/cals, "avoid"=very high sugar or saturated fat
    const de = userLang === 'de';
    let fit = 'fit';
    const warnings = [];
    if (sugarsPer100 > 20) { fit = 'caution'; warnings.push(de ? 'Hoher Zuckergehalt' : 'High sugar'); }
    if (satFatPer100 > 8) { fit = 'caution'; warnings.push(de ? 'Viel gesättigtes Fett' : 'High saturated fat'); }
    if (kcalPer100 > 400 && proteinPer100 < 10) { fit = 'caution'; warnings.push(de ? 'Kalorien-dicht, wenig Protein' : 'Calorie-dense, low protein'); }
    if (sugarsPer100 > 40 || satFatPer100 > 15) fit = 'avoid';
    if (proteinPer100 >= 15 && sugarsPer100 < 10) fit = 'fit';
    product.fit = fit;
    product.warnings = warnings;

    console.log(`✅ scan-barcode OK for ${userEmail}: ${barcode} → ${name} (${fit})`);
    res.json({ product });
  } catch (err) {
    console.error('❌ /ai/scan-barcode error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── QUICK-LOG: TEXT → CALORIES + MACROS (Claude Haiku) ────────────────
// User types free text ("Putenschnitzel mit Kartoffelsalat") and gets
// a realistic kcal + macro estimate with appropriate emoji. If input is
// too vague, returns a clarifying question instead of a bad estimate.
// Premium-only (Protokoll tab is Premium-gated in frontend, backend
// double-checks tier for security).
app.post('/ai/quick-log', aiLimiter, async (req, res) => {
  try {
    const { text, clarification, originalText, userLang } = req.body;
    if (!text || typeof text !== 'string' || text.trim().length === 0) {
      return res.status(400).json({ error: 'text required' });
    }

    // Auth (required — this endpoint is Premium-only)
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ error: 'auth_required' });
    }
    let userEmail = null, authUserId = null;
    try {
      const token = authHeader.slice(7);
      const { data } = await supabase.auth.getUser(token);
      if (data?.user?.email) userEmail = data.user.email;
      if (data?.user?.id) authUserId = data.user.id;
    } catch (_) { /* ignore */ }
    if (!authUserId) return res.status(401).json({ error: 'auth_invalid' });

    // Tier + block check
    try {
      const { data: u } = await supabase
        .from('users')
        .select('tier, status')
        .eq('id', authUserId)
        .maybeSingle();
      if (u?.status === 'blocked_voucher_abuse') {
        return res.status(403).json({ error: 'account_blocked' });
      }
      if (u?.tier !== 'premium') {
        return res.status(403).json({ error: 'premium_required' });
      }
    } catch (_) { /* fail-closed on DB errors for tier check */
      return res.status(500).json({ error: 'tier_check_failed' });
    }

    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) return res.status(500).json({ error: 'AI not configured' });

    const de = userLang === 'de';
    const inputText = text.trim();
    const isFollowup = clarification && originalText;

    // If this is a follow-up, combine original + clarification
    const combinedText = isFollowup
      ? `${originalText} (${de ? 'Präzisierung' : 'clarification'}: ${clarification.trim()})`
      : inputText;

    const prompt = de
      ? `Du bist ein präziser Ernährungs-Analytiker. Der Nutzer hat eingegeben: "${combinedText}"

AUFGABE: Schätze Kalorien + Makros für diese Mahlzeit/diesen Snack.

KALORIEN-SCHÄTZUNG (realistisch, deutsche Portionsgrößen):
- Standard-Hauptgericht: 600-1200 kcal
- Schnitzel (paniert) mit Beilage: 900-1300 kcal
- Kartoffelsalat (Portion): 250-400 kcal
- Pommes (Portion): 380-450 kcal
- Reis/Nudeln (Portion): 300-450 kcal
- Ein Bier (0.5L): 200-240 kcal, Wein (0.2L): 150-180 kcal
- Snack (Handvoll Nüsse): 180-250 kcal
- Obst (1 Stück): 50-120 kcal
- Kaffee schwarz: 2 kcal, Kaffee mit Milch/Zucker: 30-120 kcal
- Sei lieber realistisch-hoch als zu niedrig. Paniertes/Frittiertes niemals unter 600 kcal für Hauptgang.

MAKRO-SCHÄTZUNG:
- Protein: 100g mageres Fleisch ≈ 25-30g, Fisch ≈ 20-25g, Eier ≈ 12g pro Stück
- Carbs: Pommes 35g/Portion, Reis 50g/Portion, Bier 15g/0.5L
- Fat: Panade/Frittierung +15-25g, Sahnesauce +20-30g

VAGHEITS-CHECK (WICHTIG):
Wenn der Input zu vage ist um sinnvoll zu schätzen (z.B. nur "Pizza", "Pasta", "Salat", "Fleisch", "Getränk", "Snack" ohne Menge/Art), stelle EINE prägnante Rückfrage statt zu schätzen.
Beispiele für zu vage:
- "Pizza" → Rückfrage: "Welche Pizza (Salami, Margherita...) und wie viele Stücke?"
- "Pasta" → Rückfrage: "Welche Pasta und welche Sauce (Bolognese, Carbonara, Pesto...)?"
- "Salat" → Rückfrage: "Welcher Salat und mit welchen Zutaten (Hähnchen, Thunfisch, Käse...)?"
- "Bier" → Rückfrage: "Wie viele und welche Größe (0.3L, 0.5L)?"
Nicht zu vage sind: konkrete Gerichte mit Zutaten ("Putenschnitzel mit Kartoffelsalat"), Alltagsprodukte ("2 Bier", "ein Apfel"), zusammengesetzte Angaben.
${isFollowup ? 'DIES IST BEREITS EINE PRÄZISIERUNG — schätze jetzt, stelle KEINE weitere Rückfrage mehr.' : ''}

EMOJI: Wähle passendes Emoji aus: 🍽️ (allgemein), 🥩 (Fleisch), 🍗 (Geflügel), 🐟 (Fisch), 🥗 (Salat), 🍝 (Pasta), 🍕 (Pizza), 🍔 (Burger), 🌯 (Wrap), 🍜 (Suppe), 🍲 (Eintopf), 🥘 (Hauptgericht), 🍚 (Reis), 🥐 (Gebäck), 🥖 (Brot), 🍳 (Eier), 🥞 (Pancakes), 🍫 (Schokolade), 🍪 (Süß), 🍎 (Obst), 🥜 (Nüsse), 🍺 (Bier), 🍷 (Wein), ☕ (Kaffee), 🍵 (Tee), 💧 (Wasser).

Antworte AUSSCHLIESSLICH als JSON (kein Markdown, keine Erklärung):
- Bei klarem Input: {"kcal":<zahl>,"protein":<zahl>,"carbs":<zahl>,"fat":<zahl>,"emoji":"<emoji>","label":"<kurze Beschreibung max 50 Zeichen>"}
- Bei zu vagem Input: {"needsClarification":true,"question":"<eine prägnante Rückfrage>"}`
      : `You are a precise nutrition analyst. User entered: "${combinedText}"

TASK: Estimate calories + macros for this meal/snack.

CALORIE ESTIMATION (realistic, standard portions):
- Standard main dish: 600-1200 kcal
- Breaded schnitzel with side: 900-1300 kcal
- Potato salad (portion): 250-400 kcal
- Fries (portion): 380-450 kcal
- Rice/pasta (portion): 300-450 kcal
- One beer (500ml): 200-240 kcal, wine (200ml): 150-180 kcal
- Snack (handful of nuts): 180-250 kcal
- Fruit (1 piece): 50-120 kcal
- Black coffee: 2 kcal, coffee with milk/sugar: 30-120 kcal
- Err on the realistic-high side. Breaded/fried main never below 600 kcal.

MACRO ESTIMATION:
- Protein: 100g lean meat ≈ 25-30g, fish ≈ 20-25g, eggs ≈ 12g each
- Carbs: fries 35g/portion, rice 50g/portion, beer 15g/500ml
- Fat: breading/frying +15-25g, cream sauce +20-30g

VAGUENESS CHECK (IMPORTANT):
If input is too vague for a meaningful estimate (e.g. just "pizza", "pasta", "salad", "meat", "drink", "snack" without quantity/type), ask ONE concise clarifying question instead of estimating.
Examples of too vague:
- "Pizza" → Question: "Which pizza (pepperoni, margherita...) and how many slices?"
- "Pasta" → Question: "Which pasta and which sauce (bolognese, carbonara, pesto...)?"
- "Salad" → Question: "Which salad and with which ingredients (chicken, tuna, cheese...)?"
- "Beer" → Question: "How many and what size (pint, 500ml)?"
Not too vague: specific dishes with ingredients ("turkey schnitzel with potato salad"), everyday items ("2 beers", "an apple"), composed descriptions.
${isFollowup ? 'THIS IS ALREADY A CLARIFICATION — estimate now, DO NOT ask further questions.' : ''}

EMOJI: Pick fitting emoji from: 🍽️ (general), 🥩 (meat), 🍗 (poultry), 🐟 (fish), 🥗 (salad), 🍝 (pasta), 🍕 (pizza), 🍔 (burger), 🌯 (wrap), 🍜 (soup), 🍲 (stew), 🥘 (main), 🍚 (rice), 🥐 (pastry), 🥖 (bread), 🍳 (eggs), 🥞 (pancakes), 🍫 (chocolate), 🍪 (sweet), 🍎 (fruit), 🥜 (nuts), 🍺 (beer), 🍷 (wine), ☕ (coffee), 🍵 (tea), 💧 (water).

Respond ONLY as JSON (no markdown, no explanation):
- Clear input: {"kcal":<number>,"protein":<number>,"carbs":<number>,"fat":<number>,"emoji":"<emoji>","label":"<short description max 50 chars>"}
- Too vague: {"needsClarification":true,"question":"<one concise follow-up question>"}`;

    const modelName = process.env.ANTHROPIC_MODEL || 'claude-haiku-4-5-20251001';
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: modelName,
        max_tokens: 400,
        messages: [{ role: 'user', content: prompt }],
      }),
    });

    if (!r.ok) {
      const errText = await r.text();
      console.error(`❌ quick-log Anthropic ${r.status} for ${userEmail}:`, errText.slice(0, 300));
      return res.status(502).json({ error: 'AI service error' });
    }

    const data = await r.json();
    const rawText = data?.content?.[0]?.text?.trim();
    if (!rawText) return res.status(502).json({ error: 'Empty AI response' });

    // Parse JSON (strip any accidental markdown fences)
    let parsed;
    try {
      const jsonText = rawText.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/i, '').trim();
      parsed = JSON.parse(jsonText);
    } catch (e) {
      console.error(`❌ quick-log JSON parse failed for ${userEmail}:`, rawText.slice(0, 200));
      return res.status(502).json({ error: 'AI response format error' });
    }

    // Clarification branch
    if (parsed.needsClarification) {
      console.log(`✅ quick-log clarify for ${userEmail}: "${inputText}" → ask`);
      return res.json({
        needsClarification: true,
        question: parsed.question || (de ? 'Kannst du das präzisieren?' : 'Can you clarify?'),
        originalText: inputText,
      });
    }

    // Estimate branch — sanitize numbers
    const result = {
      kcal: Math.max(0, Math.round(Number(parsed.kcal) || 0)),
      protein: Math.max(0, Math.round(Number(parsed.protein) || 0)),
      carbs: Math.max(0, Math.round(Number(parsed.carbs) || 0)),
      fat: Math.max(0, Math.round(Number(parsed.fat) || 0)),
      emoji: (typeof parsed.emoji === 'string' && parsed.emoji.length <= 4) ? parsed.emoji : '🍽️',
      label: (typeof parsed.label === 'string' ? parsed.label : combinedText).slice(0, 80),
    };

    console.log(`✅ quick-log OK for ${userEmail}: "${combinedText.slice(0,40)}" → ${result.kcal}kcal`);
    res.json(result);
  } catch (err) {
    console.error('❌ /ai/quick-log error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/create-checkout', authLimiter, async (req, res) => {
  try {
    const { email, plan, tier, userData, consent, voucher, lang } = req.body;

    if (!email) {
      return res.status(400).json({ error: 'Email is required' });
    }

    if (!consent || consent.healthData !== true || consent.terms !== true) {
      console.warn(`⚠️ Checkout blocked for ${email}: missing GDPR consent`);
      return res.status(400).json({ error: 'Consent required' });
    }

    // ── AGE GATE (GDPR §8 BDSG: minimum 16) ────────────────────────────
    const ageNum = parseInt(userData && userData.age, 10);
    if (!ageNum || ageNum < 16 || ageNum > 120) {
      console.warn(`⚠️ Checkout blocked for ${email}: invalid age (${userData && userData.age})`);
      return res.status(400).json({
        error: 'AGE_RESTRICTION',
        message: lang === 'de'
          ? 'PEAK ist ab 16 Jahren verfügbar.'
          : 'PEAK is available from age 16.'
      });
    }

    // ── DUPLICATE SUBSCRIPTION PREVENTION ─────────────────────────────
    // If this email already has an active paid subscription, block new
    // checkout and point the user to the customer portal instead.
    // Check BOTH sources of truth:
    //   1. our users table (stripe_subscription_id)
    //   2. Stripe directly (live status, catches edge cases)
    const normalizedEmail = email.toLowerCase().trim();
    try {
      const { data: existing } = await supabase
        .from('users')
        .select('tier, stripe_customer_id, stripe_subscription_id, status')
        .eq('email', normalizedEmail)
        .maybeSingle();

      if (existing?.stripe_subscription_id && existing?.tier && existing.tier !== 'free') {
        // Verify with Stripe that subscription is actually active
        try {
          const sub = await stripe.subscriptions.retrieve(existing.stripe_subscription_id);
          const activeStatuses = ['active', 'trialing', 'past_due'];
          if (activeStatuses.includes(sub.status)) {
            console.warn(`🔒 Duplicate checkout blocked for ${normalizedEmail} (${sub.status})`);
            return res.status(409).json({
              error: 'already_subscribed',
              code: 'ALREADY_SUBSCRIBED',
              message: lang === 'de'
                ? 'Du hast bereits ein aktives Abo. Verwalte es im Kundenbereich.'
                : 'You already have an active subscription. Manage it in the customer portal.',
              currentTier: existing.tier,
              currentStatus: sub.status,
              customerId: existing.stripe_customer_id,
            });
          }
        } catch (stripeErr) {
          // Sub may have been deleted in Stripe but not in DB — allow checkout
          console.warn(`Stripe sub lookup failed for ${normalizedEmail}:`, stripeErr.message);
        }
      }

      // Also check by customer_id directly with Stripe (catches records we may have missed)
      if (existing?.stripe_customer_id) {
        try {
          const subs = await stripe.subscriptions.list({
            customer: existing.stripe_customer_id,
            status: 'all',
            limit: 10,
          });
          const activeSub = subs.data.find(s =>
            ['active', 'trialing', 'past_due'].includes(s.status)
          );
          if (activeSub) {
            console.warn(`🔒 Stripe shows active sub for ${normalizedEmail}: ${activeSub.id} (${activeSub.status})`);
            return res.status(409).json({
              error: 'already_subscribed',
              code: 'ALREADY_SUBSCRIBED',
              message: lang === 'de'
                ? 'Du hast bereits ein aktives Abo. Verwalte es im Kundenbereich.'
                : 'You already have an active subscription. Manage it in the customer portal.',
              currentStatus: activeSub.status,
              customerId: existing.stripe_customer_id,
            });
          }
        } catch (stripeErr) {
          console.warn(`Stripe customer sub list failed:`, stripeErr.message);
        }
      }
    } catch (checkErr) {
      // Non-fatal — log and proceed to checkout
      console.warn(`Duplicate-sub check failed for ${normalizedEmail}:`, checkErr.message);
    }

    // Pick price based on tier + interval
    const normalizedTier = tier === 'basic' ? 'basic' : 'premium';
    const normalizedPlan = plan === 'annual' ? 'annual' : 'monthly';

    let priceId;
    if (normalizedTier === 'basic' && normalizedPlan === 'annual') {
      priceId = process.env.STRIPE_PRICE_BASIC_ANNUAL;
    } else if (normalizedTier === 'basic' && normalizedPlan === 'monthly') {
      priceId = process.env.STRIPE_PRICE_BASIC_MONTHLY;
    } else if (normalizedTier === 'premium' && normalizedPlan === 'annual') {
      priceId = process.env.STRIPE_PRICE_PREMIUM_ANNUAL || process.env.STRIPE_PRICE_ANNUAL;
    } else {
      priceId = process.env.STRIPE_PRICE_PREMIUM_MONTHLY || process.env.STRIPE_PRICE_MONTHLY;
    }

    if (!priceId) {
      console.error('❌ Missing Stripe price env var for', normalizedTier, normalizedPlan);
      return res.status(500).json({ error: 'Server misconfiguration: price not set' });
    }

    const consentAt = consent.at || new Date().toISOString();
    const goalsArr = Array.isArray(userData?.goals) && userData.goals.length
      ? userData.goals
      : (userData?.goal ? [userData.goal] : []);
    // Pack full onboarding profile into two JSON metadata fields so the webhook
    // can restore everything after payment. Stripe limit: 500 chars per metadata value.
    const profileBio = JSON.stringify({
      age: userData?.age || null,
      gender: userData?.gender || null,
      weight: userData?.weight || null,
      dweight: userData?.dweight || null,
      height: userData?.height || null,
      sleep: userData?.sleep || null,
      job: userData?.job || null,
      commute: userData?.commute || null,
      stress: userData?.stress || null,
    }).slice(0, 480);
    const profileTrain = JSON.stringify({
      level: userData?.level || null,
      sessions: userData?.sessions || null,
      dur: userData?.dur || null,
      equip: userData?.equip || null,
      al: Array.isArray(userData?.al) ? userData.al : [],
      di: Array.isArray(userData?.di) ? userData.di : [],
      cu: Array.isArray(userData?.cu) ? userData.cu : [],
      cook: userData?.cook || null,
      budget: userData?.budget || null,
      stretchAreas: Array.isArray(userData?.stretchAreas) ? userData.stretchAreas : [],
      stretchDur: userData?.stretchDur || null,
      trainDays: Array.isArray(userData?.trainDays)
        ? userData.trainDays.filter(d => Number.isInteger(d) && d >= 0 && d <= 6).slice(0, 7)
        : [],
    }).slice(0, 480);
    const sharedMetadata = {
      userName: userData?.name || '',
      userGoal: userData?.goal || '',
      userGoals: JSON.stringify(goalsArr).slice(0, 450), // Stripe metadata values max 500 chars
      userSport: userData?.sport || '',
      userLang: (lang === 'de' || lang === 'en') ? lang : '',
      profileBio: profileBio,
      profileTrain: profileTrain,
      plan: normalizedPlan,
      tier: normalizedTier,
      consentHealthData: 'true',
      consentTerms: 'true',
      consentAt: consentAt,
      // Stripe metadata values must be strings — cast bool to '1' / '0'.
      // Read back in webhook profile-row creation (see meta.consentAnalytics).
      consentAnalytics: (consent && consent.analytics === true) ? '1' : '0',
    };

    // ─── VOUCHER HANDLING ───────────────────────────────────────────────
    // Three voucher types, each stored as a Stripe Promotion Code with
    // specific metadata that tells us how to apply it:
    //   - discount (default): Stripe applies coupon automatically
    //   - trial_extend: we read metadata.trial_days and override trial_period_days
    //   - trial_full: 100% off for N months (Stripe coupon + longer trial)
    let trialDays = 7;
    let appliedPromoCode = null;
    let voucherError = null;

    if (voucher && typeof voucher === 'string' && voucher.trim()) {
      const code = voucher.trim().toUpperCase();
      try {
        // Look up promotion code
        const promos = await stripe.promotionCodes.list({ code, active: true, limit: 1 });
        if (promos.data.length === 0) {
          voucherError = 'Invalid or expired voucher code';
        } else {
          const promo = promos.data[0];
          // Check redemption limit
          if (promo.max_redemptions && promo.times_redeemed >= promo.max_redemptions) {
            voucherError = 'Voucher has reached its redemption limit';
          } else if ((promo.metadata?.annual_only === 'true' || promo.coupon?.metadata?.annual_only === 'true') && normalizedPlan !== 'annual') {
            // Code restricted to annual plan
            voucherError = 'This code is only valid for the annual plan';
          } else if ((promo.metadata?.premium_only === 'true' || promo.coupon?.metadata?.premium_only === 'true') && normalizedTier !== 'premium') {
            // Code restricted to premium tier
            voucherError = 'This code is only valid for Premium';
          } else {
            // ─── ABUSE CHECK: has this email already redeemed this code? ───
            const normalizedEmail = (email || '').trim().toLowerCase();
            let abuseFound = false;
            try {
              const { data: prior } = await supabase
                .from('voucher_redemptions')
                .select('id')
                .eq('voucher_code', code)
                .eq('email', normalizedEmail)
                .limit(1);
              if (prior && prior.length > 0) {
                voucherError = 'This voucher has already been used with this email';
                abuseFound = true;
              }
            } catch (e) {
              console.warn('Voucher abuse check failed (fail-open):', e.message);
            }

            if (!abuseFound) {
              appliedPromoCode = promo.id;
              sharedMetadata.voucherCode = code;

              // Check voucher type via promotion code metadata (fall back to coupon metadata)
              const voucherType = promo.metadata?.type || promo.coupon?.metadata?.type || 'discount';
              const metaSrc = promo.metadata?.type ? promo.metadata : (promo.coupon?.metadata || {});
              if (voucherType === 'trial_extend') {
                trialDays = parseInt(metaSrc.trial_days, 10) || 28;
                appliedPromoCode = null; // no coupon — only trial extension
                sharedMetadata.voucherType = 'trial_extend';
                console.log(`🎟 Trial extended to ${trialDays} days for ${email} via ${code}`);
              } else if (voucherType === 'trial_full') {
                // Full free trial period (e.g. 3 months) then paid. Applied as
                // 100% off coupon + extended trial.
                trialDays = parseInt(metaSrc.trial_days, 10) || 90;
                sharedMetadata.voucherType = 'trial_full';
                console.log(`🎁 Full free trial ${trialDays} days for ${email} via ${code}`);
              } else {
                sharedMetadata.voucherType = 'discount';
                console.log(`💸 Discount voucher ${code} applied for ${email}`);
              }
            }
          }
        }
      } catch (err) {
        console.error('Voucher lookup failed:', err.message);
        voucherError = 'Could not verify voucher';
      }

      if (voucherError) {
        return res.status(400).json({ error: voucherError });
      }
    }

    const sessionConfig = {
      mode: 'subscription',
      // When payment_method_types is omitted on a Checkout Session, Stripe
      // automatically shows all payment methods enabled in the Dashboard
      // (card, Apple Pay, Google Pay, PayPal, Klarna, SEPA, etc.) — no code
      // change needed when you enable/disable methods in Stripe.
      // (Note: `automatic_payment_methods` is a PaymentIntent-only param and
      // must NOT be passed here — it throws "unknown parameter".)
      //
      // billing_address_collection: 'auto' is REQUIRED for PayPal subscriptions
      // (Stripe API constraint). 'auto' means Stripe collects address only when
      // the payment method requires it — so card users aren't bothered.
      billing_address_collection: 'auto',
      customer_email: email,
      line_items: [{ price: priceId, quantity: 1 }],
      subscription_data: {
        trial_period_days: trialDays,
        metadata: sharedMetadata,
      },
      success_url: `${FRONTEND_URL}?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${FRONTEND_URL}?cancelled=true`,
      metadata: sharedMetadata,
    };

    if (appliedPromoCode) {
      sessionConfig.discounts = [{ promotion_code: appliedPromoCode }];
    } else {
      // Allow manual code entry in Stripe UI as fallback
      sessionConfig.allow_promotion_codes = true;
    }

    const session = await stripe.checkout.sessions.create(sessionConfig);

    console.log(`✅ Checkout: ${email} (${normalizedTier}/${normalizedPlan}, trial=${trialDays}d${appliedPromoCode?', promo='+appliedPromoCode:''})`);
    res.json({ url: session.url, sessionId: session.id });
  } catch (err) {
    console.error('❌ Checkout error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── VOUCHER VALIDATION (public, no auth needed) ───────────────────────
// Frontend calls this when user types a code to show preview of discount
// before they commit to checkout.
app.post('/voucher/validate', authLimiter, async (req, res) => {
  try {
    const { code, plan, tier, email } = req.body;
    if (!code || typeof code !== 'string') {
      return res.status(400).json({ error: 'Code required' });
    }
    const normalizedCode = code.trim().toUpperCase();
    const promos = await stripe.promotionCodes.list({ code: normalizedCode, active: true, limit: 1 });
    if (promos.data.length === 0) {
      return res.status(404).json({ valid: false, error: 'Invalid or expired code' });
    }
    const promo = promos.data[0];
    if (promo.max_redemptions && promo.times_redeemed >= promo.max_redemptions) {
      return res.status(410).json({ valid: false, error: 'Code has reached its redemption limit' });
    }
    // Annual-only / Premium-only restrictions (checked if user already chose plan/tier)
    const annualOnly = promo.metadata?.annual_only === 'true' || promo.coupon?.metadata?.annual_only === 'true';
    const premiumOnly = promo.metadata?.premium_only === 'true' || promo.coupon?.metadata?.premium_only === 'true';
    if (annualOnly && plan && plan !== 'annual') {
      return res.status(400).json({ valid: false, error: 'This code is only valid for the annual plan', requiresAnnual: true });
    }
    if (premiumOnly && tier && tier !== 'premium') {
      return res.status(400).json({ valid: false, error: 'This code is only valid for Premium', requiresPremium: true });
    }
    // ─── ABUSE CHECK: email already redeemed this code? ─────────────────
    if (email && typeof email === 'string') {
      const normalizedEmail = email.trim().toLowerCase();
      try {
        const { data: prior, error } = await supabase
          .from('voucher_redemptions')
          .select('id')
          .eq('voucher_code', normalizedCode)
          .eq('email', normalizedEmail)
          .limit(1);
        if (!error && prior && prior.length > 0) {
          return res.status(409).json({ valid: false, error: 'This code has already been used with this email', alreadyUsed: true });
        }
      } catch (e) {
        console.warn('Voucher abuse check (email) failed:', e.message);
        // Fail-open: allow validation to continue if DB check fails
      }
    }
    const type = promo.metadata?.type || promo.coupon?.metadata?.type || 'discount';
    const metaSrc = promo.metadata?.type ? promo.metadata : (promo.coupon?.metadata || {});
    const response = { valid: true, type, code: normalizedCode };
    if (annualOnly) response.annualOnly = true;
    if (premiumOnly) response.premiumOnly = true;
    if (type === 'trial_extend') {
      response.trialDays = parseInt(metaSrc.trial_days, 10) || 28;
      response.label = `${response.trialDays} days free trial`;
    } else if (type === 'trial_full') {
      response.trialDays = parseInt(metaSrc.trial_days, 10) || 90;
      response.label = `${response.trialDays} days premium free`;
    } else {
      // Discount — fetch coupon details
      if (promo.coupon) {
        if (promo.coupon.percent_off) {
          response.percentOff = promo.coupon.percent_off;
          response.label = `${promo.coupon.percent_off}% off`;
        } else if (promo.coupon.amount_off) {
          response.amountOff = promo.coupon.amount_off / 100;
          response.label = `€${response.amountOff} off`;
        }
      }
    }
    res.json(response);
  } catch (err) {
    console.error('❌ Voucher validation error:', err.message);
    res.status(500).json({ valid: false, error: err.message });
  }
});

// ── APPLY VOUCHER TO EXISTING SUBSCRIPTION ───────────────────────────
// Use case: a paying user receives a Creator-Code AFTER checkout (e.g.
// a friend shares a code, or a late-onboarding promo). Without this
// endpoint they'd have nowhere to enter it.
//
// Behaviour by voucher type:
//   - 'discount' (default): apply Stripe coupon to active subscription
//     → next invoice gets the discount automatically
//   - 'trial_extend': REJECTED — trial extension only makes sense at
//     signup. Returning user already paid, so trial is moot.
//   - 'trial_full': REJECTED — same reasoning
//
// Free users: REJECTED. They go through /create-checkout with the
// voucher pre-filled instead. We tell the frontend to redirect them.
//
// Records the redemption in voucher_redemptions for abuse-tracking
// (same table as new-signup vouchers, same email-uniqueness rule).
app.post('/voucher/apply-existing', authLimiter, async (req, res) => {
  try {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
    if (!token) return res.status(401).json({ error: 'Missing auth token' });

    const { data: userData, error: userErr } = await supabase.auth.getUser(token);
    if (userErr || !userData?.user) {
      return res.status(401).json({ error: 'Invalid or expired token' });
    }
    const authUserId = userData.user.id;
    const userEmail = (userData.user.email || '').trim().toLowerCase();

    const { code } = req.body || {};
    if (!code || typeof code !== 'string' || !code.trim()) {
      return res.status(400).json({ error: 'Code required' });
    }
    const normalizedCode = code.trim().toUpperCase();

    // ─── Look up user's current subscription state ───────────────────
    const { data: u, error: uErr } = await supabase
      .from('users')
      .select('tier, status, stripe_subscription_id, stripe_customer_id')
      .eq('id', authUserId)
      .maybeSingle();
    if (uErr || !u) {
      return res.status(404).json({ error: 'User profile not found' });
    }
    if (u.status === 'blocked_voucher_abuse') {
      return res.status(403).json({ error: 'Account is blocked', code: 'ACCOUNT_BLOCKED' });
    }
    // Free users → route them to checkout with voucher pre-filled.
    // Frontend handles the redirect to the plan-picker step.
    if (!u.tier || u.tier === 'free') {
      return res.status(400).json({
        error: 'Free users must use the voucher at checkout',
        code: 'CHECKOUT_REQUIRED',
        redirectToCheckout: true,
      });
    }
    if (!u.stripe_subscription_id) {
      return res.status(400).json({ error: 'No active subscription found' });
    }

    // ─── Look up Stripe promotion code ───────────────────────────────
    const promos = await stripe.promotionCodes.list({ code: normalizedCode, active: true, limit: 1 });
    if (promos.data.length === 0) {
      return res.status(404).json({ error: 'Invalid or expired code' });
    }
    const promo = promos.data[0];
    if (promo.max_redemptions && promo.times_redeemed >= promo.max_redemptions) {
      return res.status(410).json({ error: 'Code has reached its redemption limit' });
    }

    // ─── Voucher type gating for existing subs ───────────────────────
    const voucherType = promo.metadata?.type || promo.coupon?.metadata?.type || 'discount';
    if (voucherType === 'trial_extend' || voucherType === 'trial_full') {
      return res.status(400).json({
        error: 'Trial-extension vouchers can only be used at signup',
        code: 'TRIAL_VOUCHER_NOT_APPLICABLE',
      });
    }

    // ─── Abuse check: same email already used this code? ────────────
    try {
      const { data: prior } = await supabase
        .from('voucher_redemptions')
        .select('id')
        .eq('voucher_code', normalizedCode)
        .eq('email', userEmail)
        .limit(1);
      if (prior && prior.length > 0) {
        return res.status(409).json({ error: 'You have already used this code', alreadyUsed: true });
      }
    } catch (e) {
      console.warn('Voucher abuse check (apply-existing) failed:', e.message);
      // Fail-open
    }

    // ─── Apply the coupon to the subscription ────────────────────────
    // Stripe's subscription.update with `promotion_code` adds the coupon
    // for future invoices. The current cycle's invoice is unchanged
    // (Stripe doesn't retroactively discount issued invoices).
    try {
      await stripe.subscriptions.update(u.stripe_subscription_id, {
        promotion_code: promo.id,
      });
    } catch (stripeErr) {
      console.error('Stripe update failed for voucher apply:', stripeErr.message);
      return res.status(500).json({ error: 'Could not apply voucher to subscription' });
    }

    // ─── Record the redemption (abuse-tracking) ──────────────────────
    // Schema mirrors the checkout-time insert (see webhook handler):
    // voucher_code + email + stripe_customer_id + stripe_subscription_id.
    // card_fingerprint is null here — we're not at a payment step, just
    // applying a coupon to the existing sub.
    try {
      await supabase.from('voucher_redemptions').insert({
        voucher_code: normalizedCode,
        email: userEmail,
        card_fingerprint: null,
        stripe_customer_id: u.stripe_customer_id || null,
        stripe_subscription_id: u.stripe_subscription_id,
      });
    } catch (e) {
      // Non-fatal — Stripe already applied the discount. Log it but
      // don't fail the whole request.
      console.warn('voucher_redemptions insert failed (post-apply):', e.message);
    }

    // ─── Build human-readable success response ───────────────────────
    let label = 'Discount applied';
    if (promo.coupon) {
      if (promo.coupon.percent_off) label = `${promo.coupon.percent_off}% off`;
      else if (promo.coupon.amount_off) label = `€${promo.coupon.amount_off / 100} off`;
    }
    console.log(`💸 Voucher ${normalizedCode} applied to existing sub for ${userEmail}`);
    res.json({ ok: true, label, code: normalizedCode });
  } catch (err) {
    console.error('❌ /voucher/apply-existing error:', err.message);
    res.status(500).json({ error: err.message });
  }
});


// ── USER PROFILE (auth-protected) ─────────────────────────────────────
// Frontend calls this after Supabase auth to load full profile data
// (plan, goal, sport, trial_end, status, etc.).
// Uses the user's access token to validate identity, then looks up the
// profile row via service role (bypasses RLS, safe because we verified first).
app.get('/user/profile', async (req, res) => {
  try {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
    if (!token) {
      console.warn('[/user/profile] no auth token in request');
      return res.status(401).json({ error: 'Missing auth token' });
    }

    // Validate token by asking Supabase who this is
    const { data: userData, error: userErr } = await supabase.auth.getUser(token);
    if (userErr || !userData?.user) {
      console.warn('[/user/profile] token validation failed:', userErr?.message || 'no user');
      return res.status(401).json({ error: 'Invalid or expired token' });
    }
    const user = userData.user;
    // Diagnostic log so we can trace 404 cases — includes the auth id,
    // email, and whether the token came from a fresh login or a restore
    // (created_at gives us age info for token-rotation issues).
    console.log(`[/user/profile] resolved user: ${user.email} id=${user.id} created=${user.created_at}`);

    // Load profile row by id (matches auth.users.id now)
    const { data: profile, error: profileErr } = await supabase
      .from('users')
      .select('*')
      .eq('id', user.id)
      .maybeSingle();

    if (profileErr) {
      console.error('❌ Profile fetch failed for', user.email, ':', profileErr.message);
      return res.status(500).json({ error: 'Failed to load profile' });
    }

    if (!profile) {
      // Edge case: auth user exists but no public.users row. Could indicate
      // (a) genuine missing profile (orphaned auth user), or (b) auth.id
      // mismatch with the public.users.id (e.g. user re-signed up under
      // same email creating a new auth row pointing at no profile while
      // an old profile sits with the previous id).
      // Try to find a profile by EMAIL as a fallback diagnostic — if we
      // find one, the IDs are out of sync and we report it loudly.
      const { data: byEmail } = await supabase
        .from('users')
        .select('id, email, tier, status')
        .ilike('email', user.email)
        .maybeSingle();
      if (byEmail) {
        console.error(`🚨 ID MISMATCH for ${user.email}: auth.id=${user.id} but public.users.id=${byEmail.id}. Profile lookup by id returned 404.`);
        // Repair: rewrite the profile row's id to match the new auth id
        // so future lookups work. Safe because the email is unique.
        const { error: updErr } = await supabase
          .from('users')
          .update({ id: user.id })
          .eq('email', user.email);
        if (!updErr) {
          console.log(`✅ Auto-repaired ID mismatch for ${user.email} → ${user.id}`);
          // Re-fetch with the new id
          const { data: repaired } = await supabase
            .from('users')
            .select('*')
            .eq('id', user.id)
            .maybeSingle();
          if (repaired) {
            return res.json({ profile: repaired });
          }
        } else {
          console.error('❌ Could not auto-repair ID mismatch:', updErr.message);
        }
      } else {
        console.warn(`[/user/profile] no profile row found for ${user.email} (id=${user.id})`);
      }
      return res.status(404).json({ error: 'Profile not found', email: user.email });
    }

    res.json({ profile });
  } catch (err) {
    console.error('❌ /user/profile error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── PROFILE UPDATE (auth-protected) ────────────────────────────────────
// Whitelist: only allow users to update their own editable fields.
app.post('/user/update-profile', async (req, res) => {
  try {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
    if (!token) return res.status(401).json({ error: 'Missing auth token' });

    const { data: userData, error: userErr } = await supabase.auth.getUser(token);
    if (userErr || !userData?.user) {
      return res.status(401).json({ error: 'Invalid or expired token' });
    }
    const user = userData.user;

    // Block voucher-abuse-flagged accounts from any profile update.
    // Without this, a blocked user could keep editing their plan inputs and
    // re-trigger plan generations, even though all their AI endpoints are
    // already locked.
    try {
      const { data: status } = await supabase
        .from('users').select('status').eq('id', user.id).maybeSingle();
      if (status?.status === 'blocked_voucher_abuse') {
        return res.status(403).json({ error: 'account_blocked', code: 'ACCOUNT_BLOCKED' });
      }
    } catch (_) { /* fail-open on transient DB errors — auth still verified */ }

    // Whitelist of editable fields. Anything not in this list is silently ignored.
    // NO access to: id, email, stripe_*, plan, tier, trial_*, status, consent_*, created_at
    const ALLOWED = [
      'name','age','gender','weight','dweight','height','sleep',
      'job','commute','stress',
      'sport','level','sessions','dur','equip',
      'al','di','cu','cook','budget','goal','goals','lang',
      'stretchAreas','stretchDur',
      'trainDays',
    ];

    // Type rules per field. Validation is conservative: anything that doesn't
    // match the rule is rejected with 400, NOT silently coerced — better to
    // surface the error to the caller than to write garbage that breaks the
    // app downstream (e.g. weight="abc" would crash BMR calculation).
    const NUMERIC_FIELDS = new Set(['age','weight','dweight','height','sleep','sessions','dur','stress','budget','stretchDur']);
    const ARRAY_FIELDS = new Set(['al','di','cu','goals','stretchAreas']);
    const INT_ARRAY_FIELDS = new Set(['trainDays']);
    const STRING_FIELDS = new Set(['name','gender','job','commute','sport','level','equip','cook','goal','lang']);

    // Sane numeric ranges — protects against -50 weight, age=999, etc.
    const NUMERIC_RANGES = {
      age:      [16, 120],
      weight:   [25, 350],
      dweight:  [25, 350],
      height:   [100, 250],
      sleep:    [0, 24],
      sessions: [0, 14],
      dur:      [10, 240],
      stretchDur: [5, 60],
      stress:   [1, 10],
      budget:   [0, 10000],
    };

    const updates = {};
    for (const k of ALLOWED) {
      if (req.body[k] === undefined) continue;
      const v = req.body[k];

      if (NUMERIC_FIELDS.has(k)) {
        if (v === null) { updates[k] = null; continue; }
        const num = typeof v === 'number' ? v : parseFloat(v);
        if (!isFinite(num)) {
          return res.status(400).json({ error: `Invalid number for ${k}` });
        }
        const range = NUMERIC_RANGES[k];
        if (range && (num < range[0] || num > range[1])) {
          return res.status(400).json({ error: `${k} out of range (${range[0]}–${range[1]})` });
        }
        updates[k] = num;
      } else if (ARRAY_FIELDS.has(k)) {
        if (!Array.isArray(v)) {
          return res.status(400).json({ error: `${k} must be an array` });
        }
        // stretchAreas has stricter rules than the other arrays:
        //   - hard cap at 3 entries (UX promise + AI prompt focus)
        //   - only specific keys allowed (matches the onboarding chip set)
        // We validate this BEFORE the generic length+string check so a bad
        // value gets a precise error message instead of a generic one.
        if (k === 'stretchAreas') {
          const ALLOWED_AREAS = new Set(['hip','chest','upBack','lowBack','ham','calf','neck','knee','ankle']);
          if (v.length > 3) {
            return res.status(400).json({ error: 'stretchAreas: max 3 areas' });
          }
          for (const item of v) {
            if (typeof item !== 'string' || !ALLOWED_AREAS.has(item)) {
              return res.status(400).json({ error: `stretchAreas: invalid area "${item}"` });
            }
          }
          updates[k] = v.slice();
          continue;
        }
        if (v.length > 30) {
          return res.status(400).json({ error: `${k} too many entries` });
        }
        // Each entry must be a non-empty string under 80 chars (protects against
        // accidentally storing objects, html, or unbounded strings).
        const cleaned = [];
        for (const item of v) {
          if (typeof item !== 'string') {
            return res.status(400).json({ error: `${k} entries must be strings` });
          }
          const s = item.trim();
          if (s && s.length <= 80) cleaned.push(s);
        }
        updates[k] = cleaned;
      } else if (INT_ARRAY_FIELDS.has(k)) {
        // trainDays: optional weekday picker. Frontend sends 0..6 (Mon..Sun).
        // Accept null/[] as "clear preference, use auto-distribution".
        if (v === null) { updates[k] = []; continue; }
        if (!Array.isArray(v)) {
          return res.status(400).json({ error: `${k} must be an array` });
        }
        const seen = new Set();
        const cleanedInts = [];
        for (const item of v) {
          const n = typeof item === 'number' ? item : parseInt(item);
          if (!Number.isInteger(n) || n < 0 || n > 6) {
            return res.status(400).json({ error: `${k}: entries must be integers 0..6` });
          }
          if (!seen.has(n)) { seen.add(n); cleanedInts.push(n); }
        }
        cleanedInts.sort((a, b) => a - b);
        updates[k] = cleanedInts;
      } else if (STRING_FIELDS.has(k)) {
        if (v === null || v === '') { updates[k] = null; continue; }
        if (typeof v !== 'string') {
          return res.status(400).json({ error: `${k} must be a string` });
        }
        const s = v.trim();
        if (s.length > 200) {
          return res.status(400).json({ error: `${k} too long` });
        }
        updates[k] = s;
      }
    }

    if (Object.keys(updates).length === 0) {
      return res.status(400).json({ error: 'No valid fields to update' });
    }

    // Field-name mapping: frontend uses camelCase, some DB columns are
    // snake_case (mostly multi-word fields added later in the project's
    // life). Translate before writing — keeps the validation logic above
    // simple and the API contract stable for the frontend.
    const FIELD_TO_COLUMN = {
      stretchAreas: 'stretch_areas',
      stretchDur:   'stretch_dur',
      trainDays:    'train_days',
    };
    const dbUpdates = {};
    for (const [k, v] of Object.entries(updates)) {
      dbUpdates[FIELD_TO_COLUMN[k] || k] = v;
    }

    dbUpdates.updated_at = new Date().toISOString();

    const { data, error } = await supabase
      .from('users')
      .update(dbUpdates)
      .eq('id', user.id)
      .select()
      .maybeSingle();

    if (error) {
      console.error('❌ Profile update failed for', user.email, ':', error.message);
      return res.status(500).json({ error: 'Failed to update profile' });
    }

    console.log(`✅ Profile updated for ${user.email} (${Object.keys(updates).length - 1} fields)`);
    res.json({ profile: data });
  } catch (err) {
    console.error('❌ /user/update-profile error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── DELETE ACCOUNT (auth-protected) ───────────────────────────────────
// Full GDPR Art. 17 "Right to Erasure" compliance — also Google Play
// (since 2024) and Apple (since 2022) require an in-app account deletion
// option. This is a HARD DELETE, not soft — executes immediately.
//
// Flow:
//   1. Cancel Stripe subscription immediately (if any)
//   2. Delete login_codes rows
//   3. Delete public.users row
//   4. Delete Supabase Auth user
//   5. Send confirmation email
app.delete('/user/account', async (req, res) => {
  try {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
    if (!token) return res.status(401).json({ error: 'Missing auth token' });

    const { data: userData, error: userErr } = await supabase.auth.getUser(token);
    if (userErr || !userData?.user) {
      return res.status(401).json({ error: 'Invalid or expired token' });
    }

    const userId = userData.user.id;
    const email = (userData.user.email || '').toLowerCase();
    const lang = (req.body && req.body.lang) || 'de';

    console.log(`🗑️  Account deletion started: ${email} (${userId})`);

    // 1. Load profile for Stripe IDs + lang
    const { data: profile } = await supabase
      .from('users')
      .select('stripe_customer_id, stripe_subscription_id, lang, name')
      .eq('id', userId)
      .maybeSingle();

    const userLang = (profile && profile.lang) || lang || 'de';
    const userName = (profile && profile.name) || '';

    // 2. Cancel Stripe subscription immediately (if any)
    if (profile && profile.stripe_subscription_id) {
      // FIRST: mark the user row as pending deletion. The
      // customer.subscription.deleted webhook fires asynchronously after
      // the Stripe cancel below; without this flag it would send a
      // "cancellation_final" email even though the user is also about to
      // get an "account_deleted" email — i.e. two emails for one action.
      // The webhook checks this status and skips the email.
      try {
        await supabase
          .from('users')
          .update({ status: 'pending_deletion' })
          .eq('id', userId);
      } catch (err) {
        console.warn(`   ⚠ Failed to mark pending_deletion (continuing): ${err.message}`);
      }
      try {
        await stripe.subscriptions.cancel(profile.stripe_subscription_id, {
          invoice_now: false,
          prorate: false,
        });
        console.log(`   ✓ Stripe subscription cancelled: ${profile.stripe_subscription_id}`);
      } catch (err) {
        // Sub might already be cancelled — log but continue
        console.warn(`   ⚠ Stripe sub cancel failed (continuing): ${err.message}`);
      }
    }

    // 3. Delete Stripe customer (optional — keeps invoice history if not deleted)
    // We keep the customer for legal/tax record reasons (invoices must be
    // retained 10 years in DE per §147 AO). Only the subscription is cancelled.

    // 4. Delete login_codes rows (cleanup)
    try {
      await supabase
        .from('login_codes')
        .delete()
        .eq('email', email);
      console.log(`   ✓ login_codes deleted for ${email}`);
    } catch (err) {
      console.warn(`   ⚠ login_codes delete failed: ${err.message}`);
    }

    // 4.5 Leave family group cleanly. The ON DELETE CASCADE on family_memberships
    // would handle row removal automatically when we delete the users row below,
    // but doing it explicitly here means: (a) the cleanup_empty_family_group
    // trigger fires before user-row removal so the foreign-key references are
    // all still valid, (b) regenerateFutureMealsAfterMemberChange can run for
    // the remaining members, (c) we log it for audit purposes.
    try {
      const { data: activeMembership } = await supabase
        .from('family_memberships')
        .select('id, group_id')
        .eq('user_id', userId)
        .eq('status', 'active')
        .maybeSingle();
      if (activeMembership) {
        await supabase.from('family_memberships')
          .update({ status: 'left', left_at: new Date().toISOString() })
          .eq('id', activeMembership.id);
        console.log(`   ✓ Family membership ended (group ${activeMembership.group_id})`);
        // Best-effort: clean stale future meals for the remaining members
        await regenerateFutureMealsAfterMemberChange(activeMembership.group_id).catch(() => {});
      }
    } catch (err) {
      console.warn(`   ⚠ Family leave during deletion failed (continuing): ${err.message}`);
    }

    // 5. Delete public.users row
    try {
      const { error: delProfileErr } = await supabase
        .from('users')
        .delete()
        .eq('id', userId);
      if (delProfileErr) throw delProfileErr;
      console.log(`   ✓ public.users row deleted`);
    } catch (err) {
      console.error(`   ❌ users row delete failed: ${err.message}`);
      return res.status(500).json({ error: 'Failed to delete profile data' });
    }

    // 6. Delete Supabase Auth user (hard delete)
    try {
      const { error: delAuthErr } = await supabase.auth.admin.deleteUser(userId);
      if (delAuthErr) throw delAuthErr;
      console.log(`   ✓ Supabase Auth user deleted`);
    } catch (err) {
      console.error(`   ❌ auth user delete failed: ${err.message}`);
      // Don't fail the whole request — profile is already gone, user
      // effectively can't login anymore. Log for manual cleanup.
    }

    // 7. Send confirmation email (best-effort)
    try {
      await sendEmail(email, 'account_deleted', { name: userName, lang: userLang });
      console.log(`   ✓ Confirmation email sent to ${email}`);
    } catch (err) {
      console.warn(`   ⚠ Confirmation email failed: ${err.message}`);
    }

    console.log(`✅ Account deletion complete: ${email}`);
    res.json({ success: true });
  } catch (err) {
    console.error('❌ /user/account DELETE error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── EXPORT MY DATA (GDPR Art. 20) ─────────────────────────────────────
// Assembles all user-related data into a JSON package and emails it
// as a download link (stored briefly). Users have a right to portability.
app.get('/user/export-data', async (req, res) => {
  try {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
    if (!token) return res.status(401).json({ error: 'Missing auth token' });

    const { data: userData, error: userErr } = await supabase.auth.getUser(token);
    if (userErr || !userData?.user) {
      return res.status(401).json({ error: 'Invalid or expired token' });
    }

    const userId = userData.user.id;
    const email = (userData.user.email || '').toLowerCase();

    // Gather everything we have on this user
    const { data: profile } = await supabase
      .from('users')
      .select('*')
      .eq('id', userId)
      .maybeSingle();

    // Build export JSON (strip internal-only fields that aren't user data)
    const exportPayload = {
      export_generated_at: new Date().toISOString(),
      export_format_version: '1.0',
      user_id: userId,
      email: email,
      profile: profile || null,
      _notice: 'This export contains all personal data MJ Performance / PEAK holds about you. Payment data is held by Stripe and not included here — see stripe.com/privacy. AI prompts sent to Anthropic during your usage are not retained (Zero Data Retention).'
    };

    console.log(`📦 Data export generated for ${email}`);

    // Return as downloadable JSON
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename="peak-data-export-${Date.now()}.json"`);
    res.send(JSON.stringify(exportPayload, null, 2));
  } catch (err) {
    console.error('❌ /user/export-data error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── TRAINING STATE (auth-protected) ────────────────────────────────────
// GET: load user's training progress (completed sessions, feedback, week)
app.get('/user/training-state', async (req, res) => {
  try {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
    if (!token) return res.status(401).json({ error: 'Missing auth token' });

    const { data: userData, error: userErr } = await supabase.auth.getUser(token);
    if (userErr || !userData?.user) {
      return res.status(401).json({ error: 'Invalid or expired token' });
    }

    const { data, error } = await supabase
      .from('users')
      .select('training_state')
      .eq('id', userData.user.id)
      .maybeSingle();

    if (error) {
      console.error('❌ training-state GET failed:', error.message);
      return res.status(500).json({ error: 'Failed to load training state' });
    }

    res.json({ training_state: data?.training_state || null });
  } catch (err) {
    console.error('❌ /user/training-state GET error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// POST: save training state (upserts entire blob)
// ── MEAL TRACKING (Apr 2026) ──────────────────────────────────────────
// Stores which planned meals the user has checked off as eaten today.
// Frontend keeps localStorage as source of truth on the device; this
// endpoint syncs that state to Supabase so it follows the user across
// devices. Basic + Premium only — frontend gates Free users with an
// upgrade prompt before they can even tap a checkbox.
app.post('/user/meal-track', async (req, res) => {
  try {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
    if (!token) return res.status(401).json({ error: 'Missing auth token' });

    const { data: userData, error: userErr } = await supabase.auth.getUser(token);
    if (userErr || !userData?.user) {
      return res.status(401).json({ error: 'Invalid or expired token' });
    }

    // Tier gate — Free users cannot persist meal tracking
    const { data: u } = await supabase
      .from('users').select('tier, status').eq('id', userData.user.id).maybeSingle();
    if (u?.status === 'blocked_voucher_abuse') {
      return res.status(403).json({ error: 'account_blocked', code: 'ACCOUNT_BLOCKED' });
    }
    if (!u || u.tier === 'free') {
      return res.status(403).json({ error: 'basic_required', code: 'BASIC_REQUIRED' });
    }

    const { date, checked } = req.body;
    // Validate: date is YYYY-MM-DD, checked is { "0": true, ... } with ≤ 12 keys
    if (!date || typeof date !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({ error: 'date must be YYYY-MM-DD' });
    }
    if (!checked || typeof checked !== 'object' || Array.isArray(checked)) {
      return res.status(400).json({ error: 'checked must be an object' });
    }
    const keys = Object.keys(checked);
    if (keys.length > 12) {
      return res.status(400).json({ error: 'too many entries' });
    }
    // Sanitise: only string keys → boolean values
    const cleaned = {};
    for (const k of keys) {
      if (/^\d{1,2}$/.test(k) && checked[k] === true) cleaned[k] = true;
    }

    const payload = { date, checked: cleaned };
    const { error } = await supabase
      .from('users')
      .update({ meal_track: payload, updated_at: new Date().toISOString() })
      .eq('id', userData.user.id);

    if (error) {
      console.error('❌ meal-track POST failed:', error.message);
      return res.status(500).json({ error: 'Failed to save meal track' });
    }
    res.json({ ok: true });
  } catch (err) {
    console.error('❌ /user/meal-track POST error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/user/training-state', async (req, res) => {
  try {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
    if (!token) return res.status(401).json({ error: 'Missing auth token' });

    const { data: userData, error: userErr } = await supabase.auth.getUser(token);
    if (userErr || !userData?.user) {
      return res.status(401).json({ error: 'Invalid or expired token' });
    }

    const ts = req.body.training_state;
    if (!ts || typeof ts !== 'object') {
      return res.status(400).json({ error: 'training_state must be an object' });
    }

    // Basic shape validation
    const cleaned = {
      completed: ts.completed && typeof ts.completed === 'object' ? ts.completed : {},
      feedback: ts.feedback && typeof ts.feedback === 'object' ? ts.feedback : {},
      currentWeek: Number.isFinite(ts.currentWeek) ? Math.max(1, Math.min(12, ts.currentWeek)) : 1
    };

    const { error } = await supabase
      .from('users')
      .update({ training_state: cleaned, updated_at: new Date().toISOString() })
      .eq('id', userData.user.id);

    if (error) {
      console.error('❌ training-state POST failed:', error.message);
      return res.status(500).json({ error: 'Failed to save training state' });
    }

    res.json({ ok: true });
  } catch (err) {
    console.error('❌ /user/training-state POST error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── MEAL POOL (auth-protected) ─────────────────────────────────────────
// Persists the user's 14-day meal rotation across devices. The pool is
// generated client-side via Anthropic API calls; this endpoint is purely
// pass-through storage so Phone and Desktop see the same meals.
//
// Tier gate: Free users do NOT sync — their pool stays device-local
// (matches the auto-refresh tier gate; both align with "paid tiers get
// the polished multi-device experience" positioning).
//
// Last-write-wins: meal_pool_updated_at is set on every write. The
// frontend pulls the backend pool only if it's newer than what's local
// (see loadAuthProfile). No merge conflicts, no Last-Write-Loses
// surprises if two devices generate at the same second — whichever the
// backend processes last is the one everyone sees.
app.post('/user/meal-pool', async (req, res) => {
  try {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
    if (!token) return res.status(401).json({ error: 'Missing auth token' });

    const { data: userData, error: userErr } = await supabase.auth.getUser(token);
    if (userErr || !userData?.user) {
      return res.status(401).json({ error: 'Invalid or expired token' });
    }

    // Tier gate — Free users don't get cross-device sync
    const { data: u } = await supabase
      .from('users').select('tier, status').eq('id', userData.user.id).maybeSingle();
    if (u?.status === 'blocked_voucher_abuse') {
      return res.status(403).json({ error: 'account_blocked', code: 'ACCOUNT_BLOCKED' });
    }
    if (!u || u.tier === 'free') {
      return res.status(403).json({ error: 'basic_required', code: 'BASIC_REQUIRED' });
    }

    const { meal_pool, meal_pool_anchor, meal_pool_last_refresh, meal_pool_refresh_count } = req.body || {};

    // Validate: pool is an array of 14 day-arrays, each with 4 meal objects.
    // Anything else is rejected — better to surface bad data here than to
    // store garbage that breaks the client on next pull.
    if (!Array.isArray(meal_pool) || meal_pool.length !== 14) {
      return res.status(400).json({ error: 'meal_pool must be an array of exactly 14 days' });
    }
    for (let d = 0; d < 14; d++) {
      const day = meal_pool[d];
      if (!Array.isArray(day) || day.length !== 4) {
        return res.status(400).json({ error: `meal_pool[${d}] must be an array of 4 meals` });
      }
      for (let m = 0; m < 4; m++) {
        const meal = day[m];
        if (!meal || typeof meal !== 'object') {
          return res.status(400).json({ error: `meal_pool[${d}][${m}] must be an object` });
        }
        // Spot-check key fields — rejects obviously malformed payloads
        // without enumerating every property (we trust the client schema
        // here since pool is generated by our own AI prompt).
        if (typeof meal.name !== 'string' || typeof meal.time !== 'string') {
          return res.status(400).json({ error: `meal_pool[${d}][${m}] missing name/time` });
        }
      }
    }

    // Bound size: 14 days × 4 meals × ~6 ingredients × ~50 chars ≈ 16KB
    // total. We cap the JSON serialisation at 64KB to catch pathological
    // pools (e.g. AI hallucinated 100 ingredients per meal) before they
    // bloat the row.
    const serialised = JSON.stringify(meal_pool);
    if (serialised.length > 64 * 1024) {
      return res.status(400).json({ error: 'meal_pool payload too large' });
    }

    // Anchor and refresh fields are optional but typed when present
    if (meal_pool_anchor != null && typeof meal_pool_anchor !== 'string') {
      return res.status(400).json({ error: 'meal_pool_anchor must be a string' });
    }
    if (meal_pool_last_refresh != null && typeof meal_pool_last_refresh !== 'string') {
      return res.status(400).json({ error: 'meal_pool_last_refresh must be a string' });
    }
    const refreshCountClean = Number.isFinite(meal_pool_refresh_count)
      ? Math.max(0, Math.min(1000, Math.floor(meal_pool_refresh_count)))
      : 0;

    const { error } = await supabase
      .from('users')
      .update({
        meal_pool,
        meal_pool_anchor: meal_pool_anchor || null,
        meal_pool_last_refresh: meal_pool_last_refresh || null,
        meal_pool_refresh_count: refreshCountClean,
        meal_pool_updated_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      })
      .eq('id', userData.user.id);

    if (error) {
      console.error('❌ meal-pool POST failed:', error.message);
      return res.status(500).json({ error: 'Failed to save meal pool' });
    }
    res.json({ ok: true });
  } catch (err) {
    console.error('❌ /user/meal-pool POST error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── LITE-SYNC ENDPOINT (May 2026) ──────────────────────────────────────
// Single endpoint that persists the small per-user cross-device state
// pieces that don't need their own dedicated endpoint: meal ratings,
// workout ratings, food log, and weekly shopping check-offs.
//
// All four are Basic+ only (same tier policy as meal-pool/meal-track).
// Each field is independently optional in the payload — clients send
// whatever they want to update. Backend stores them as JSONB columns.
//
// Size guards: ratings are small (<200 entries), food log is bounded to
// 200 entries, weekly-shop-checks bounded to 500 keys. Anything bigger
// is rejected — we don't store unbounded user payloads.
app.post('/user/lite-sync', async (req, res) => {
  try {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
    if (!token) return res.status(401).json({ error: 'Missing auth token' });

    const { data: userData, error: userErr } = await supabase.auth.getUser(token);
    if (userErr || !userData?.user) {
      return res.status(401).json({ error: 'Invalid or expired token' });
    }

    // Tier gate
    const { data: u } = await supabase
      .from('users').select('tier, status').eq('id', userData.user.id).maybeSingle();
    if (u?.status === 'blocked_voucher_abuse') {
      return res.status(403).json({ error: 'account_blocked', code: 'ACCOUNT_BLOCKED' });
    }
    if (!u || u.tier === 'free') {
      return res.status(403).json({ error: 'basic_required', code: 'BASIC_REQUIRED' });
    }

    const { meal_ratings, workout_ratings, food_log, weekly_shop_checks, meditation_log, mobility_log, analytics_optin, hydration_log } = req.body || {};
    const updates = { updated_at: new Date().toISOString() };

    // Validate + sanitise each field independently. Anything malformed is
    // rejected wholesale — we don't half-save a payload because that would
    // make state hard to reason about on the next client read.
    if (meal_ratings !== undefined) {
      if (!meal_ratings || typeof meal_ratings !== 'object' || Array.isArray(meal_ratings)) {
        return res.status(400).json({ error: 'meal_ratings must be an object' });
      }
      const keys = Object.keys(meal_ratings);
      if (keys.length > 200) {
        return res.status(400).json({ error: 'meal_ratings too large' });
      }
      const cleaned = {};
      for (const k of keys) {
        const v = meal_ratings[k];
        // Keys are small integers (meal idx), values are 1-5 ratings
        if (/^\d{1,3}$/.test(k) && typeof v === 'number' && v >= 1 && v <= 5) {
          cleaned[k] = v;
        }
      }
      updates.meal_ratings = cleaned;
    }

    if (workout_ratings !== undefined) {
      if (!workout_ratings || typeof workout_ratings !== 'object' || Array.isArray(workout_ratings)) {
        return res.status(400).json({ error: 'workout_ratings must be an object' });
      }
      const keys = Object.keys(workout_ratings);
      if (keys.length > 200) {
        return res.status(400).json({ error: 'workout_ratings too large' });
      }
      const cleaned = {};
      for (const k of keys) {
        const v = workout_ratings[k];
        // Keys can be week-day strings ("w1d2") or numeric — string check is broader
        if (typeof k === 'string' && k.length <= 24 && typeof v === 'number' && v >= 1 && v <= 5) {
          cleaned[k] = v;
        }
      }
      updates.workout_ratings = cleaned;
    }

    if (food_log !== undefined) {
      if (!Array.isArray(food_log)) {
        return res.status(400).json({ error: 'food_log must be an array' });
      }
      if (food_log.length > 200) {
        return res.status(400).json({ error: 'food_log too large' });
      }
      // Each entry: {text, emoji, kcal, time}. Shallow-validate types.
      const cleaned = [];
      for (const entry of food_log) {
        if (!entry || typeof entry !== 'object') continue;
        const text = typeof entry.text === 'string' ? entry.text.slice(0, 200) : '';
        const emoji = typeof entry.emoji === 'string' ? entry.emoji.slice(0, 8) : '';
        const kcal = Number.isFinite(entry.kcal) ? Math.round(entry.kcal) : 0;
        const time = typeof entry.time === 'string' ? entry.time.slice(0, 16) : '';
        if (text) cleaned.push({ text, emoji, kcal, time });
      }
      updates.food_log = cleaned;
    }

    if (weekly_shop_checks !== undefined) {
      if (!weekly_shop_checks || typeof weekly_shop_checks !== 'object' || Array.isArray(weekly_shop_checks)) {
        return res.status(400).json({ error: 'weekly_shop_checks must be an object' });
      }
      const keys = Object.keys(weekly_shop_checks);
      if (keys.length > 500) {
        return res.status(400).json({ error: 'weekly_shop_checks too large' });
      }
      const cleaned = {};
      for (const k of keys) {
        // Keys can be long (item|qty composite) — cap at 400 chars
        if (k.length > 400) continue;
        const v = weekly_shop_checks[k];
        // Values are timestamp millis from Date.now() — must be a number
        if (typeof v === 'number' && v > 0) cleaned[k] = v;
      }
      updates.weekly_shop_checks = cleaned;
    }

    // ── Habit-streak logs (Bündel 3): meditation + mobility ────────────
    // Shape: { 'YYYY-MM-DD': [id1, id2, ...] } — array entries are the
    // exercise/routine IDs completed that day. Same shape for both
    // categories. Capped at 100 dates (≈ 3 months of daily activity)
    // and 30 entries per day (no realistic user does 30 sessions/day).
    function validateDateLog(input, name) {
      if (!input || typeof input !== 'object' || Array.isArray(input)) {
        return { error: name + ' must be an object' };
      }
      const dateKeys = Object.keys(input);
      if (dateKeys.length > 100) {
        return { error: name + ' too large (>100 days)' };
      }
      const cleaned = {};
      for (const dk of dateKeys) {
        // Keys must be YYYY-MM-DD — anything else rejected silently to
        // avoid breaking the whole sync over a single malformed key.
        if (!/^\d{4}-\d{2}-\d{2}$/.test(dk)) continue;
        const arr = input[dk];
        if (!Array.isArray(arr)) continue;
        // Trim each day's array: max 30 entries, each ≤ 40 chars
        const dayArr = arr.slice(0, 30).filter(v => typeof v === 'string' && v.length <= 40);
        if (dayArr.length > 0) cleaned[dk] = dayArr;
      }
      return { value: cleaned };
    }
    if (meditation_log !== undefined) {
      const r = validateDateLog(meditation_log, 'meditation_log');
      if (r.error) return res.status(400).json({ error: r.error });
      updates.meditation_log = r.value;
    }
    if (mobility_log !== undefined) {
      const r = validateDateLog(mobility_log, 'mobility_log');
      if (r.error) return res.status(400).json({ error: r.error });
      updates.mobility_log = r.value;
    }
    // Strict boolean cast — anything truthy → true, falsy → false. No
    // half-states; the DB column is NOT NULL DEFAULT FALSE so we always
    // write a definite value when present in the payload.
    if (analytics_optin !== undefined) {
      updates.analytics_optin = analytics_optin === true || analytics_optin === 'true';
    }
    // Hydration log — JSONB keyed by YYYY-MM-DD with entries[] arrays.
    // We trust the client to trim to last 3 days; here we just enforce
    // size limits so a malformed payload can't bloat the row.
    // Structure: { 'YYYY-MM-DD': { entries: [{ts, ml, type, label?, kcal?, protein?, carbs?, fat?}, ...] } }
    if (hydration_log !== undefined) {
      if (!hydration_log || typeof hydration_log !== 'object' || Array.isArray(hydration_log)) {
        return res.status(400).json({ error: 'hydration_log must be an object' });
      }
      const dateKeys = Object.keys(hydration_log);
      if (dateKeys.length > 10) {
        return res.status(400).json({ error: 'hydration_log: too many date entries (max 10)' });
      }
      const cleaned = {};
      for (const dk of dateKeys) {
        // Validate date key format YYYY-MM-DD
        if (!/^\d{4}-\d{2}-\d{2}$/.test(dk)) continue;
        const day = hydration_log[dk];
        if (!day || typeof day !== 'object' || !Array.isArray(day.entries)) continue;
        if (day.entries.length > 50) {
          return res.status(400).json({ error: 'hydration_log: too many entries per day (max 50)' });
        }
        // Validate each entry
        const okEntries = [];
        for (const e of day.entries) {
          if (!e || typeof e !== 'object') continue;
          if (typeof e.ml !== 'number' || e.ml < 0 || e.ml > 5000) continue;
          // Allowed drink types — anything else gets dropped
          const allowedTypes = ['water', 'tea', 'coffee', 'broth', 'juice', 'smoothie', 'shake'];
          if (typeof e.type !== 'string' || allowedTypes.indexOf(e.type) === -1) continue;
          const sanitised = {
            ts: typeof e.ts === 'number' ? e.ts : Date.now(),
            ml: Math.round(e.ml),
            type: e.type
          };
          if (typeof e.label === 'string' && e.label.length <= 80) sanitised.label = e.label;
          if (typeof e.kcal === 'number' && e.kcal >= 0 && e.kcal <= 2000) sanitised.kcal = Math.round(e.kcal);
          if (typeof e.protein === 'number' && e.protein >= 0 && e.protein <= 200) sanitised.protein = Math.round(e.protein);
          if (typeof e.carbs === 'number' && e.carbs >= 0 && e.carbs <= 500) sanitised.carbs = Math.round(e.carbs);
          if (typeof e.fat === 'number' && e.fat >= 0 && e.fat <= 200) sanitised.fat = Math.round(e.fat);
          okEntries.push(sanitised);
        }
        cleaned[dk] = { entries: okEntries };
      }
      updates.hydration_log = cleaned;
    }

    // No fields → no-op (avoid pointless updated_at bump)
    if (Object.keys(updates).length === 1) {
      return res.json({ ok: true, noop: true });
    }

    const { error } = await supabase
      .from('users')
      .update(updates)
      .eq('id', userData.user.id);

    if (error) {
      console.error('❌ /user/lite-sync failed:', error.message);
      return res.status(500).json({ error: 'Failed to save state' });
    }
    res.json({ ok: true });
  } catch (err) {
    console.error('❌ /user/lite-sync error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── STRETCH POOL ENDPOINT (May 2026, Phase 3) ──────────────────────────
// Same pattern as /user/meal-pool, but premium-only and with a different
// pool shape: each slot is an OBJECT (training/rest), not an array.
//
// Validation rules:
//   - Must be exactly 14 slots
//   - Each slot has type='training' (with pre+post arrays) or type='rest'
//     (with full array). All exercises have name + detail strings.
app.post('/user/stretch-pool', async (req, res) => {
  try {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
    if (!token) return res.status(401).json({ error: 'Missing auth token' });

    const { data: userData, error: userErr } = await supabase.auth.getUser(token);
    if (userErr || !userData?.user) {
      return res.status(401).json({ error: 'Invalid or expired token' });
    }

    // Premium-only — Free + Basic don't get cross-device stretching sync.
    // (Local pool still works for them; they just don't sync between devices.)
    const { data: u } = await supabase
      .from('users').select('tier, status').eq('id', userData.user.id).maybeSingle();
    if (u?.status === 'blocked_voucher_abuse') {
      return res.status(403).json({ error: 'account_blocked', code: 'ACCOUNT_BLOCKED' });
    }
    if (!u || u.tier !== 'premium') {
      return res.status(403).json({ error: 'premium_required', code: 'PREMIUM_REQUIRED' });
    }

    const { stretch_pool, stretch_pool_anchor, stretch_pool_last_refresh, stretch_pool_refresh_count } = req.body || {};

    if (!Array.isArray(stretch_pool) || stretch_pool.length !== 14) {
      return res.status(400).json({ error: 'stretch_pool must be an array of exactly 14 slots' });
    }

    // Per-slot shape validation: training has pre+post, rest has full
    function validateExerciseList(arr, slotIdx, listKey) {
      if (!Array.isArray(arr)) {
        return `stretch_pool[${slotIdx}].${listKey} must be an array`;
      }
      if (arr.length > 20) {
        return `stretch_pool[${slotIdx}].${listKey} too many exercises`;
      }
      for (let i = 0; i < arr.length; i++) {
        const ex = arr[i];
        if (!ex || typeof ex !== 'object') {
          return `stretch_pool[${slotIdx}].${listKey}[${i}] must be an object`;
        }
        if (typeof ex.name !== 'string' || typeof ex.detail !== 'string') {
          return `stretch_pool[${slotIdx}].${listKey}[${i}] missing name/detail`;
        }
      }
      return null;
    }

    for (let s = 0; s < 14; s++) {
      const slot = stretch_pool[s];
      if (!slot || typeof slot !== 'object') {
        return res.status(400).json({ error: `stretch_pool[${s}] must be an object` });
      }
      if (slot.type !== 'training' && slot.type !== 'rest') {
        return res.status(400).json({ error: `stretch_pool[${s}].type must be 'training' or 'rest'` });
      }
      if (slot.type === 'training') {
        const errPre = validateExerciseList(slot.pre, s, 'pre');
        if (errPre) return res.status(400).json({ error: errPre });
        const errPost = validateExerciseList(slot.post, s, 'post');
        if (errPost) return res.status(400).json({ error: errPost });
      } else {
        const errFull = validateExerciseList(slot.full, s, 'full');
        if (errFull) return res.status(400).json({ error: errFull });
      }
    }

    // Size cap: with howTo (steps + cues + mistakes + why), each exercise
    // is ~400 chars. 14 slots × ~10 exercises × 400 = ~56KB. Cap at 128KB
    // to allow for legitimately large pools without rejecting legit cases.
    const serialised = JSON.stringify(stretch_pool);
    if (serialised.length > 128 * 1024) {
      return res.status(400).json({ error: 'stretch_pool payload too large' });
    }

    if (stretch_pool_anchor != null && typeof stretch_pool_anchor !== 'string') {
      return res.status(400).json({ error: 'stretch_pool_anchor must be a string' });
    }
    if (stretch_pool_last_refresh != null && typeof stretch_pool_last_refresh !== 'string') {
      return res.status(400).json({ error: 'stretch_pool_last_refresh must be a string' });
    }
    const refreshCountClean = Number.isFinite(stretch_pool_refresh_count)
      ? Math.max(0, Math.min(1000, Math.floor(stretch_pool_refresh_count)))
      : 0;

    const { error } = await supabase
      .from('users')
      .update({
        stretch_pool,
        stretch_pool_anchor: stretch_pool_anchor || null,
        stretch_pool_last_refresh: stretch_pool_last_refresh || null,
        stretch_pool_refresh_count: refreshCountClean,
        stretch_pool_updated_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      })
      .eq('id', userData.user.id);

    if (error) {
      console.error('❌ stretch-pool POST failed:', error.message);
      return res.status(500).json({ error: 'Failed to save stretch pool' });
    }
    res.json({ ok: true });
  } catch (err) {
    console.error('❌ /user/stretch-pool POST error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── PLAN SYNC ENDPOINT (May 2026 evening) ─────────────────────────────
// Stores the user's generated plan so opening the app on a different
// device doesn't trigger a fresh AI call. Without this endpoint, every
// browser session/tab without a sessionStorage cache regenerates the
// plan from scratch — wasting ~$0.05 + 6sec loader per redundant load.
//
// Available to ALL tiers (including Free) — every user who paid Haiku
// to generate a plan should be able to see it across their devices.
// Validation kept light because we trust our own AI output schema.
app.post('/user/plan', async (req, res) => {
  try {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
    if (!token) return res.status(401).json({ error: 'Missing auth token' });

    const { data: userData, error: userErr } = await supabase.auth.getUser(token);
    if (userErr || !userData?.user) {
      return res.status(401).json({ error: 'Invalid or expired token' });
    }

    const { data: u } = await supabase
      .from('users').select('status').eq('id', userData.user.id).maybeSingle();
    if (u?.status === 'blocked_voucher_abuse') {
      return res.status(403).json({ error: 'account_blocked', code: 'ACCOUNT_BLOCKED' });
    }

    const { plan_data } = req.body || {};
    if (!plan_data || typeof plan_data !== 'object') {
      return res.status(400).json({ error: 'plan_data must be an object' });
    }
    // Spot-check the shape — we expect at minimum a headline + calorie target
    if (typeof plan_data.headline !== 'string') {
      return res.status(400).json({ error: 'plan_data.headline required' });
    }

    // Size cap: typical plan with week + meal_pool reference is ~30KB.
    // Cap at 256KB to allow for legitimately large pools (the meal pool
    // itself goes through /user/meal-pool, but if the client lazily
    // includes it here we still accept up to that size).
    const serialised = JSON.stringify(plan_data);
    if (serialised.length > 256 * 1024) {
      return res.status(400).json({ error: 'plan_data too large' });
    }

    const { error } = await supabase
      .from('users')
      .update({
        plan_data,
        plan_updated_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      })
      .eq('id', userData.user.id);

    if (error) {
      console.error('❌ plan POST failed:', error.message);
      return res.status(500).json({ error: 'Failed to save plan' });
    }
    res.json({ ok: true });
  } catch (err) {
    console.error('❌ /user/plan POST error:', err.message);
    res.status(500).json({ error: err.message });
  }
});


// ── WEBHOOK ───────────────────────────────────────────────────────────
app.post('/webhook', async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;
  try {
    event = stripe.webhooks.constructEvent(req.body, sig, process.env.STRIPE_WEBHOOK_SECRET);
  } catch (err) {
    console.error('❌ Webhook signature verification failed:', err.message);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  console.log(`🔔 Webhook received: ${event.type} (${event.id})`);

  // ── IDEMPOTENCY CHECK (Apr 2026 hardening) ─────────────────────────
  // Stripe occasionally redelivers webhooks (network blips, retries after
  // 5xx responses). Without this guard we would create duplicate auth
  // users, send duplicate welcome emails, and double-count plan_generations.
  // We persist event.id in `webhook_events` and bail out on duplicates.
  // The table only stores ID + type + received_at, so size stays trivial.
  try {
    const { error: insertErr } = await supabase
      .from('webhook_events')
      .insert({
        event_id: event.id,
        event_type: event.type,
        received_at: new Date().toISOString(),
      });
    if (insertErr) {
      // Postgres unique-violation code is '23505'. Anything else → log but
      // process the event anyway (fail-open: better to risk a duplicate
      // than to silently drop a legitimate event).
      if (insertErr.code === '23505') {
        console.log(`↩️  Duplicate webhook event ${event.id} — already processed, skipping`);
        return res.status(200).json({ received: true, duplicate: true });
      }
      console.warn(`⚠️  webhook_events insert failed (fail-open):`, insertErr.message);
    }
  } catch (e) {
    console.warn('⚠️  Idempotency check threw (fail-open):', e.message);
  }

  try {
    if (event.type === 'checkout.session.completed') {
      const session = event.data.object;

      // Email can live in different places depending on Stripe's flow.
      // Try in order: customer_email → customer_details.email → fetch customer object.
      let email = session.customer_email || session.customer_details?.email || null;
      if (!email && session.customer) {
        try {
          const customer = await stripe.customers.retrieve(session.customer);
          email = customer.email;
        } catch (err) {
          console.error('❌ Could not fetch customer from Stripe:', err.message);
        }
      }

      if (!email) {
        console.error('❌ No email found for checkout session', session.id);
        return res.status(200).json({ received: true, warning: 'no email' });
      }

      // Normalize email to lowercase — Supabase Auth stores emails lowercased,
      // and all our email lookups use .toLowerCase() too. Writing a mixed-case
      // email here creates a ghost row the app can never find.
      email = email.toLowerCase().trim();

      const meta = session.metadata || {};
      // Pull real trial_end from the Stripe subscription instead of hardcoding
      // 7 days. With voucher promos (LAUNCH4W = 28d, full-free codes = 90d)
      // the hardcoded 7 was always wrong — users got correct billing from
      // Stripe but the wrong trial_end stored in our users table, so the
      // trial_ending email at day 5 fired way too early.
      let trialEnd = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000); // safe default
      let trialDaysCount = 7;
      if (session.subscription) {
        try {
          const sub = await stripe.subscriptions.retrieve(session.subscription);
          if (sub && sub.trial_end) {
            trialEnd = new Date(sub.trial_end * 1000);
            trialDaysCount = Math.round((trialEnd.getTime() - Date.now()) / 86400000);
            if (trialDaysCount < 1) trialDaysCount = 7; // sanity floor
          }
        } catch (err) {
          console.warn('⚠️  Could not fetch subscription for trial_end:', err.message);
        }
      }

      // ── STEP 1: Create or find Auth user ────────────────────────────
      // Service role bypasses the "Allow signups" toggle, so we can create
      // auth users even with signups disabled for public registration.
      let authUserId = null;
      try {
        // Try to create new auth user. If already exists, catch & look up by email.
        const { data: created, error: createErr } = await supabase.auth.admin.createUser({
          email,
          email_confirm: true, // User confirmed via Stripe payment, no need to re-verify
          user_metadata: {
            name: meta.userName || '',
            source: 'stripe_checkout',
          },
        });

        if (createErr) {
          // "User already registered" is fine — look up the existing one
          if (createErr.message?.toLowerCase().includes('already')) {
            const match = await findAuthUserByEmail(email);
            if (match) {
              authUserId = match.id;
              console.log(`ℹ️  Existing auth user found: ${email} (${authUserId})`);
            } else {
              throw new Error('Auth user reported as existing but not found in list');
            }
          } else {
            throw createErr;
          }
        } else {
          authUserId = created.user.id;
          console.log(`✅ Auth user created: ${email} (${authUserId})`);
        }
      } catch (err) {
        console.error('❌ Auth user creation failed for', email, ':', err.message);
        return res.status(200).json({ received: true, auth_error: err.message });
      }

      // ── STEP 2: Upsert profile row in public.users, keyed by auth id ──
      let parsedGoals = [];
      try {
        if (meta.userGoals) parsedGoals = JSON.parse(meta.userGoals);
        if (!Array.isArray(parsedGoals)) parsedGoals = [];
      } catch (_) { parsedGoals = []; }
      if (parsedGoals.length === 0 && meta.userGoal) parsedGoals = [meta.userGoal];

      // Unpack profile metadata blobs (set in /create-checkout)
      let bio = {};
      let train = {};
      try { if (meta.profileBio) bio = JSON.parse(meta.profileBio) || {}; } catch (_) {}
      try { if (meta.profileTrain) train = JSON.parse(meta.profileTrain) || {}; } catch (_) {}

      // If this user was previously Free, we want to PRESERVE their onboarding
      // data rather than overwrite with empty fields. Load prior row, fall back
      // to metadata values.
      let prior = null;
      try {
        const { data } = await supabase
          .from('users')
          .select('age,gender,weight,dweight,height,sleep,job,commute,stress,level,sessions,dur,equip,al,di,cu,cook,budget,stretch_areas,stretch_dur,train_days')
          .eq('id', authUserId)
          .maybeSingle();
        prior = data || null;
      } catch (_) {}

      // Merge metadata with prior DB row, preferring metadata when present.
      // "Present" means: not null, not undefined, not empty string.
      // This prevents overwriting valid onboarding data with empty checkout data.
      const hasVal = (v) => v !== null && v !== undefined && v !== '';
      const pickNum = (meta, prior, parser) => {
        if (hasVal(meta)) return parser(meta);
        if (hasVal(prior)) return prior;
        return null;
      };
      const pickStr = (meta, prior) => {
        if (hasVal(meta)) return meta;
        if (hasVal(prior)) return prior;
        return null;
      };
      const pickArr = (meta, prior) => {
        if (Array.isArray(meta) && meta.length) return meta;
        if (Array.isArray(prior) && prior.length) return prior;
        return [];
      };

      const userRow = {
        id: authUserId,
        email,
        name: meta.userName || '',
        age: pickNum(bio.age, prior?.age, parseInt),
        gender: pickStr(bio.gender, prior?.gender),
        weight: pickNum(bio.weight, prior?.weight, parseFloat),
        dweight: pickNum(bio.dweight, prior?.dweight, parseFloat),
        height: pickNum(bio.height, prior?.height, parseFloat),
        sleep: pickNum(bio.sleep, prior?.sleep, parseFloat),
        job: pickStr(bio.job, prior?.job),
        commute: pickStr(bio.commute, prior?.commute),
        stress: pickNum(bio.stress, prior?.stress, parseFloat),
        level: pickStr(train.level, prior?.level),
        sessions: pickNum(train.sessions, prior?.sessions, parseInt),
        dur: pickNum(train.dur, prior?.dur, parseInt),
        equip: pickStr(train.equip, prior?.equip),
        al: pickArr(train.al, prior?.al),
        di: pickArr(train.di, prior?.di),
        cu: pickArr(train.cu, prior?.cu),
        cook: pickStr(train.cook, prior?.cook),
        budget: pickNum(train.budget, prior?.budget, parseFloat),
        stretch_areas: pickArr(train.stretchAreas, prior?.stretch_areas),
        stretch_dur: pickNum(train.stretchDur, prior?.stretch_dur, parseInt),
        train_days: pickArr(train.trainDays, prior?.train_days),
        stripe_customer_id: session.customer || null,
        stripe_subscription_id: session.subscription || null,
        plan: meta.plan || 'monthly',
        tier: meta.tier === 'basic' ? 'basic' : 'premium',
        goal: meta.userGoal || '',
        goals: parsedGoals,
        sport: meta.userSport || '',
        lang: (meta.userLang === 'de' || meta.userLang === 'en') ? meta.userLang : 'en',
        plan_generations_used: 0, // Reset usage counter on paid subscription
        plan_generations_window_start: new Date().toISOString(), // New window starts now
        trial_start: new Date().toISOString(),
        trial_end: trialEnd.toISOString(),
        status: 'trial',
        unsubscribed: false,
        // GDPR consent record — Art. 9(2)(a) GDPR requires documented consent
        // for processing health data. We store the fact + timestamp.
        consent_health_data: meta.consentHealthData === 'true',
        consent_terms: meta.consentTerms === 'true',
        consent_at: meta.consentAt || new Date().toISOString(),
        // Strict opt-in default — only TRUE if user actively ticked the
        // optional analytics box on the consent screen. Defaults to FALSE
        // for legacy webhook payloads that don't carry consentAnalytics.
        analytics_optin: meta.consentAnalytics === '1',
      };

      const { data, error } = await supabase
        .from('users')
        .upsert(userRow, { onConflict: 'id' })
        .select();

      if (error) {
        console.error('❌ Supabase upsert failed for', email, ':', error.message);
        return res.status(200).json({ received: true, db_error: error.message });
      }

      console.log(`✅ User profile upserted: ${email} (rows: ${data?.length || 0})`);

      // ── STEP 3: Generate a magic link + send branded welcome email ──
      // We generate the magic link with Supabase admin and embed it in our
      // own welcome email, so the user gets one email with everything.
      let magicLink = null;
      try {
        const { data: linkData, error: linkErr } = await supabase.auth.admin.generateLink({
          type: 'magiclink',
          email,
          options: {
            redirectTo: `${FRONTEND_URL}/`,
          },
        });
        if (linkErr) {
          console.error('❌ Magic link generation failed:', linkErr.message);
        } else {
          magicLink = linkData?.properties?.action_link || null;
        }
      } catch (err) {
        console.error('❌ Magic link exception:', err.message);
      }

      try {
        await sendEmail(email, 'welcome', {
          name: meta.userName || '',
          goal: meta.userGoal || '',
          goals: parsedGoals,
          sport: meta.userSport || '',
          magicLink,
          // Distinguish Basic vs Premium so the email can show the right
          // tier-specific feature list and trial copy. The original code
          // only had a binary "isFree" flag — Basic users would see the
          // generic "paid" path with Premium features advertised, which
          // they don't actually get.
          tier: meta.tier === 'basic' ? 'basic' : 'premium',
          trialDays: trialDaysCount,
          // Pass through voucher code so the welcome email can highlight
          // bonus trial days (e.g. LAUNCH4W → "your code added 21 bonus
          // days"). Without this, users wonder why their trial is longer
          // than 7 days.
          voucherCode: meta.voucherCode || '',
          lang: meta.userLang === 'de' ? 'de' : (meta.userLang === 'en' ? 'en' : undefined),
        });
      } catch (err) {
        console.error('❌ Welcome email failed for', email, ':', err.message);
      }

      // ── STEP 4: Voucher redemption tracking + fingerprint abuse check ──
      // If a voucher was used, record the redemption. Also check if the
      // card fingerprint has been used for this voucher before — if yes,
      // cancel the subscription (abuse detected).
      if (meta.voucherCode) {
        const voucherCode = meta.voucherCode;
        let cardFingerprint = null;
        try {
          // Fetch payment method via subscription → default_payment_method → card.fingerprint
          if (session.subscription) {
            const sub = await stripe.subscriptions.retrieve(session.subscription, {
              expand: ['default_payment_method'],
            });
            const pm = sub.default_payment_method;
            if (pm && pm.card && pm.card.fingerprint) {
              cardFingerprint = pm.card.fingerprint;
            } else if (sub.customer) {
              // Fallback: fetch customer's default payment method
              const customer = await stripe.customers.retrieve(sub.customer);
              const defaultPmId = customer.invoice_settings?.default_payment_method;
              if (defaultPmId) {
                const pm2 = await stripe.paymentMethods.retrieve(defaultPmId);
                if (pm2.card && pm2.card.fingerprint) cardFingerprint = pm2.card.fingerprint;
              }
            }
          }
        } catch (err) {
          console.warn('⚠️  Could not fetch card fingerprint for abuse check:', err.message);
        }

        // Fingerprint-based abuse check
        let abuseDetected = false;
        if (cardFingerprint) {
          try {
            const { data: prior } = await supabase
              .from('voucher_redemptions')
              .select('id, email')
              .eq('voucher_code', voucherCode)
              .eq('card_fingerprint', cardFingerprint)
              .limit(1);
            if (prior && prior.length > 0) {
              abuseDetected = true;
              console.warn(`🚨 VOUCHER ABUSE: ${email} used ${voucherCode} with card previously used by ${prior[0].email}`);
            }
          } catch (e) {
            console.warn('Fingerprint abuse check failed:', e.message);
          }
        }

        if (abuseDetected && session.subscription) {
          // Cancel the subscription — abuse confirmed
          try {
            await stripe.subscriptions.cancel(session.subscription, {
              invoice_now: false,
              prorate: false,
            });
            console.log(`🛑 Subscription cancelled due to voucher abuse: ${session.subscription}`);
            // Mark user as blocked AND downgrade to free tier
            // This prevents continued access to premium features via existing OTP logins
            await supabase.from('users').update({
              status: 'blocked_voucher_abuse',
              tier: 'free',
              stripe_subscription_id: null,
              trial_end: null,
            }).eq('email', email);
            console.log(`🔒 User downgraded to free + blocked: ${email}`);
          } catch (err) {
            console.error('❌ Could not cancel subscription after abuse detection:', err.message);
          }
        } else {
          // Record the redemption
          try {
            const { error: insErr } = await supabase.from('voucher_redemptions').insert({
              voucher_code: voucherCode,
              email: email.toLowerCase(),
              card_fingerprint: cardFingerprint,
              stripe_customer_id: session.customer || null,
              stripe_subscription_id: session.subscription || null,
            });
            if (insErr) console.error('❌ Could not insert voucher redemption:', insErr.message);
            else console.log(`📝 Voucher redemption recorded: ${voucherCode} for ${email}`);
          } catch (err) {
            console.error('❌ Voucher redemption insert exception:', err.message);
          }
        }
      }
    } else if (event.type === 'customer.subscription.deleted') {
      const sub = event.data.object;
      let email = null;
      try {
        const customer = await stripe.customers.retrieve(sub.customer);
        email = customer.email;
      } catch (err) {
        console.error('❌ Could not fetch customer for cancellation:', err.message);
      }
      if (email) {
        // Normalise to lowercase — DB stores normalised emails, Stripe may
        // hand us mixed case which would silently miss the row.
        email = email.toLowerCase().trim();
        // Downgrade tier to free — prevents continued premium access after cancellation
        // Keep status history: if already blocked_voucher_abuse, don't overwrite
        // Also pull `lang` so we can pass it explicitly to sendEmail — without
        // it sendEmail falls back to a goal-text heuristic which has flagged
        // German users as English in the past (Bug D2).
        const { data: existing } = await supabase
          .from('users')
          .select('status,lang,tier')
          .eq('email', email)
          .maybeSingle();
        // Capture the tier BEFORE we downgrade to 'free', so the
        // cancellation_final email can reference what they actually had
        // ("Your Basic plan has ended" vs blanket "Premium has ended"
        // which was confusing for Basic users).
        const cancelledFromTier = existing?.tier || 'premium';
        // Detect if user is being deleted right now via /user/account DELETE.
        // That endpoint sets status to 'pending_deletion' BEFORE calling
        // stripe.subscriptions.cancel(), so when this webhook fires we can
        // tell the cancellation is part of an account deletion and skip the
        // redundant "cancellation_final" email (account_deleted email is
        // sent by the deletion endpoint itself).
        const isAccountDeletion = existing?.status === 'pending_deletion';
        const newStatus = existing?.status === 'blocked_voucher_abuse'
          ? 'blocked_voucher_abuse'
          : (isAccountDeletion ? 'pending_deletion' : 'cancelled');
        const { data: updated, error } = await supabase.from('users').update({
          status: newStatus,
          tier: 'free',
          stripe_subscription_id: null,
          trial_end: null,
          cancel_at: null,
        }).eq('email', email).select('id');
        if (error) console.error('❌ Supabase update (cancelled) failed:', error.message);
        else console.log(`✅ User cancelled + downgraded to free: ${email}`);

        // If the update affected 0 rows, the user row was already deleted
        // (race condition: account-deletion endpoint finished the row delete
        // before this webhook arrived). In that case the cancellation email
        // is redundant — the user already gets the "account deleted" mail.
        const userStillExists = Array.isArray(updated) && updated.length > 0;

        // Email C: Final — subscription has actually ended
        // Skip in three cases:
        //   1. User abuse-blocked (no win-back mail)
        //   2. User row already deleted (account-deletion race)
        //   3. User is mid-deletion (pending_deletion flag set)
        const skipEmail = newStatus === 'blocked_voucher_abuse'
          || !userStillExists
          || isAccountDeletion;
        if (!skipEmail) {
          try {
            // Pass explicit lang to override sendEmail's fallback heuristic.
            // If user.lang is missing the function falls back to goal-text
            // detection, which is unreliable for users without German keyword
            // goals (e.g. someone with goal "endurance" still wants German
            // mail if they signed up in DE).
            const userLang = (existing?.lang === 'de' || existing?.lang === 'en')
              ? existing.lang : 'de';
            await sendEmail(email, 'cancellation_final', { lang: userLang, tier: cancelledFromTier });
            console.log(`📧 Cancellation final → ${email} (${userLang}, was ${cancelledFromTier})`);
          } catch (err) {
            console.error('⚠️  cancellation_final email failed:', err.message);
          }
        } else if (isAccountDeletion || !userStillExists) {
          console.log(`ℹ️  Skipping cancellation_final for ${email}: account deletion in progress`);
        }
        // ── Family Plan: suspend any active membership ─────────────────
        // Premium-only feature, so losing tier must remove the user from
        // their family group. We use 'suspended' (not 'left') so the row
        // is recoverable if they re-subscribe within a grace window — the
        // family tab can show "Anna lost Premium" and offer re-invite.
        // Trigger drops the empty group automatically if this was the last
        // active member; otherwise regenerateFutureMealsAfterMemberChange
        // clears stale meals so they get rebuilt without the suspended user.
        if (userStillExists && updated[0]?.id) {
          try {
            const { data: activeMembership } = await supabase
              .from('family_memberships')
              .select('id, group_id')
              .eq('user_id', updated[0].id)
              .eq('status', 'active')
              .maybeSingle();
            if (activeMembership) {
              await supabase.from('family_memberships')
                .update({ status: 'suspended', left_at: new Date().toISOString() })
                .eq('id', activeMembership.id);
              await supabase.from('users')
                .update({ family_group_id: null })
                .eq('id', updated[0].id);
              // Best-effort: clear stale meals so they regenerate without this user
              setImmediate(() => regenerateFutureMealsAfterMemberChange(activeMembership.group_id).catch(() => {}));
              console.log(`👨‍👩‍👧 Family membership suspended for ${email} (group ${activeMembership.group_id})`);
            }
          } catch (err) {
            console.error('⚠️  Family membership suspend failed:', err.message);
          }
        }
      }
    } else if (event.type === 'customer.subscription.updated') {
      // Detect when user initiates cancellation at period end
      const sub = event.data.object;
      const prev = event.data.previous_attributes || {};
      const nowCancelling = sub.cancel_at_period_end === true;
      const wasCancelling = prev.cancel_at_period_end === true;
      const wasReactivated = wasCancelling && !nowCancelling;
      const justCancelled = nowCancelling && !wasCancelling;

      // ── PLAN-CHANGE DETECTION ────────────────────────────────────────
      // When a user switches Basic ↔ Premium or monthly ↔ annual in the
      // Customer Portal, Stripe fires customer.subscription.updated with
      // the new price under sub.items.data[0].price.id. Without this
      // handler, our DB tier/plan never updates and the user keeps
      // seeing Basic limits even after paying for Premium (or vice versa).
      let priceChanged = false;
      let newTier = null;
      let newPlan = null;
      try {
        const newPriceId = sub.items?.data?.[0]?.price?.id;
        if (newPriceId) {
          const PRICE_BASIC_MONTHLY = process.env.STRIPE_PRICE_BASIC_MONTHLY;
          const PRICE_BASIC_ANNUAL = process.env.STRIPE_PRICE_BASIC_ANNUAL;
          const PRICE_PREMIUM_MONTHLY = process.env.STRIPE_PRICE_PREMIUM_MONTHLY || process.env.STRIPE_PRICE_MONTHLY;
          const PRICE_PREMIUM_ANNUAL = process.env.STRIPE_PRICE_PREMIUM_ANNUAL || process.env.STRIPE_PRICE_ANNUAL;
          if (newPriceId === PRICE_BASIC_MONTHLY) { newTier = 'basic'; newPlan = 'monthly'; }
          else if (newPriceId === PRICE_BASIC_ANNUAL) { newTier = 'basic'; newPlan = 'annual'; }
          else if (newPriceId === PRICE_PREMIUM_MONTHLY) { newTier = 'premium'; newPlan = 'monthly'; }
          else if (newPriceId === PRICE_PREMIUM_ANNUAL) { newTier = 'premium'; newPlan = 'annual'; }
          // Only flag a change if items actually moved (Stripe sends
          // subscription.updated for many reasons — cancellations, trial
          // ending, billing-cycle ticks, metadata edits — and we don't
          // want to redundantly write tier on every one of those events).
          priceChanged = !!prev.items;
        }
      } catch (err) {
        console.warn('⚠️  Plan-change parse failed:', err.message);
      }

      if (priceChanged && newTier && newPlan) {
        let email = null;
        try {
          const customer = await stripe.customers.retrieve(sub.customer);
          email = customer.email ? customer.email.toLowerCase().trim() : null;
        } catch (err) {
          console.error('❌ Could not fetch customer for plan-change:', err.message);
        }
        if (email) {
          // Pull trial_end from the live Stripe sub so the DB stays in sync
          // even when the Customer Portal cleared/extended trial.
          const trialEndIso = sub.trial_end ? new Date(sub.trial_end * 1000).toISOString() : null;
          // Capture prior tier so we can detect Premium→Basic downgrade
          const { data: priorRow } = await supabase
            .from('users').select('id, tier').eq('email', email).maybeSingle();
          const priorTier = priorRow?.tier || null;
          const updates = {
            tier: newTier,
            plan: newPlan,
            status: sub.status === 'trialing' ? 'trial' : 'active',
          };
          if (trialEndIso) updates.trial_end = trialEndIso;
          const { error } = await supabase.from('users').update(updates).eq('email', email);
          if (error) console.error('❌ Plan-change DB update failed:', error.message);
          else console.log(`🔄 Plan changed: ${email} → ${newTier}/${newPlan} (trial_end=${trialEndIso || 'unchanged'})`);
          // ── Family Plan: suspend membership if dropping below Premium ──
          // Family Plan is Premium-only. If the user just moved from
          // Premium to Basic (or anything else), pull them out of the
          // group. Same 'suspended' status as on full cancellation so a
          // future re-upgrade can re-activate them with one click.
          if (priorTier === 'premium' && newTier !== 'premium' && priorRow?.id) {
            try {
              const { data: activeMembership } = await supabase
                .from('family_memberships')
                .select('id, group_id')
                .eq('user_id', priorRow.id)
                .eq('status', 'active')
                .maybeSingle();
              if (activeMembership) {
                await supabase.from('family_memberships')
                  .update({ status: 'suspended', left_at: new Date().toISOString() })
                  .eq('id', activeMembership.id);
                await supabase.from('users')
                  .update({ family_group_id: null })
                  .eq('id', priorRow.id);
                setImmediate(() => regenerateFutureMealsAfterMemberChange(activeMembership.group_id).catch(() => {}));
                console.log(`👨‍👩‍👧 Family membership suspended (downgrade): ${email}`);
              }
            } catch (err) {
              console.error('⚠️  Family suspend on downgrade failed:', err.message);
            }
          }
        }
      }

      if (justCancelled || wasReactivated) {
        let email = null;
        try {
          const customer = await stripe.customers.retrieve(sub.customer);
          email = customer.email;
        } catch (err) {
          console.error('❌ Could not fetch customer for sub.updated:', err.message);
        }
        // Normalise to lowercase — see "Email-Case-Bug" notes
        if (email) email = email.toLowerCase().trim();
        if (email && justCancelled) {
          // Email A: Cancellation confirmed — paid plan continues until period end
          const periodEnd = sub.cancel_at || sub.current_period_end;
          const endDate = periodEnd ? new Date(periodEnd * 1000) : null;
          // Pull lang AND tier so emails read correctly. Tier matters here:
          // a Basic user cancelling shouldn't see "Your Premium ends" in
          // the email body — we pass the actual tier and the template
          // adapts ("Basic" / "Premium" / "Plan" if unknown).
          const { data: existing } = await supabase
            .from('users')
            .select('lang, tier')
            .eq('email', email)
            .maybeSingle();
          const userLang = (existing?.lang === 'de' || existing?.lang === 'en')
            ? existing.lang : 'de';
          const userTier = existing?.tier || 'premium';
          // Mark in DB as pending cancellation + store end date for reminder cron.
          // Reset cancel_reminder_sent so a fresh reminder is queued for this
          // cancellation cycle (relevant if user previously reactivated).
          await supabase.from('users').update({
            status: 'cancelling',
            cancel_at: endDate ? endDate.toISOString() : null,
            cancel_reminder_sent: false,
          }).eq('email', email);

          try {
            const endDateStr = endDate
              ? endDate.toLocaleDateString(userLang === 'de' ? 'de-DE' : 'en-US', { day: '2-digit', month: 'long', year: 'numeric' })
              : '';
            await sendEmail(email, 'cancellation_confirmed', { endDate: endDateStr, lang: userLang, tier: userTier });
            console.log(`📧 Cancellation confirmed → ${email} (ends ${endDateStr}, ${userLang}, ${userTier})`);
          } catch (err) {
            console.error('⚠️  cancellation_confirmed email failed:', err.message);
          }
        }
        if (email && wasReactivated) {
          // User clicked "Don't cancel" in portal → restore status + reset
          // the reminder flag (so a future cancel triggers a fresh reminder).
          await supabase.from('users').update({
            status: 'active',
            cancel_at: null,
            cancel_reminder_sent: false,
          }).eq('email', email);
          console.log(`✅ User un-cancelled (reactivated): ${email}`);
        }
      }
    } else if (event.type === 'invoice.payment_succeeded') {
      const invoice = event.data.object;
      if (invoice.billing_reason === 'subscription_cycle') {
        let email = invoice.customer_email || null;
        if (!email && invoice.customer) {
          try {
            const customer = await stripe.customers.retrieve(invoice.customer);
            email = customer.email;
          } catch (err) {
            console.error('❌ Could not fetch customer for renewal:', err.message);
          }
        }
        if (email) {
          email = email.toLowerCase().trim();
          const { error } = await supabase.from('users').update({ status: 'active' }).eq('email', email);
          if (error) console.error('❌ Supabase update (active) failed:', error.message);
          else console.log(`✅ User renewed: ${email}`);
        }
      }
    } else if (event.type === 'invoice.payment_failed') {
      // ── PAYMENT FAILED ──────────────────────────────────────────────
      // Stripe retries the charge automatically on its dunning schedule
      // (3 attempts over ~3 weeks by default). We don't cancel here —
      // Stripe will fire customer.subscription.deleted at end of dunning
      // if all retries fail. We just notify the user so they can update
      // their card before the subscription gets cancelled.
      const invoice = event.data.object;
      let email = invoice.customer_email || null;
      if (!email && invoice.customer) {
        try {
          const customer = await stripe.customers.retrieve(invoice.customer);
          email = customer.email;
        } catch (err) {
          console.error('❌ Could not fetch customer for payment_failed:', err.message);
        }
      }
      if (email) {
        email = email.toLowerCase().trim();
        // Mark as past_due so the app can show a banner / restrict access
        const { error: updErr } = await supabase
          .from('users')
          .update({ status: 'past_due' })
          .eq('email', email);
        if (updErr) console.error('❌ Supabase update (past_due) failed:', updErr.message);

        // Send email — but only on the first attempt to avoid spamming on
        // every retry. Stripe sets attempt_count starting at 1.
        const attempt = invoice.attempt_count || 1;
        if (attempt === 1) {
          const { data: user } = await supabase
            .from('users')
            .select('name, lang')
            .eq('email', email)
            .maybeSingle();
          await sendEmail(email, 'payment_failed', {
            name: user?.name || '',
            lang: user?.lang || 'de',
          });
          console.log(`📧 Payment-failed email → ${email} (attempt ${attempt})`);
        } else {
          console.log(`ℹ️  Payment failed for ${email} (attempt ${attempt}) — no email (already notified)`);
        }
      }
    } else if (event.type === 'customer.subscription.trial_will_end') {
      // ── TRIAL ENDING SOON ──────────────────────────────────────────
      // Stripe fires this 3 days before trial_end. We send a friendly
      // reminder so the user can cancel cleanly if they don't want to
      // be charged. Required by Apple & Google for subscription apps,
      // also a good-faith signal that builds trust.
      const sub = event.data.object;
      let email = null;
      if (sub.customer) {
        try {
          const customer = await stripe.customers.retrieve(sub.customer);
          email = customer.email;
        } catch (err) {
          console.error('❌ Could not fetch customer for trial_will_end:', err.message);
        }
      }
      if (email) {
        email = email.toLowerCase().trim();
        const { data: user } = await supabase
          .from('users')
          .select('name, lang')
          .eq('email', email)
          .maybeSingle();
        // Compute trial length from Stripe sub timestamps so the email
        // reads correctly for non-default trials (vouchers like LAUNCH4W
        // give 28 days; previously the body said "7-day trial" no matter
        // what, which contradicted the actual end date and confused users).
        let trialDaysCount = null;
        if (sub.trial_start && sub.trial_end) {
          const ms = (sub.trial_end - sub.trial_start) * 1000;
          trialDaysCount = Math.round(ms / (1000 * 60 * 60 * 24));
        }
        await sendEmail(email, 'trial_ending', {
          name: user?.name || '',
          lang: user?.lang || 'de',
          trialEnd: sub.trial_end ? new Date(sub.trial_end * 1000) : null,
          trialDays: trialDaysCount,
        });
        console.log(`📧 Trial-ending email → ${email}`);
      }
    }
  } catch (err) {
    console.error('❌ Webhook handler error:', err.message, err.stack);
    // Still return 200 so Stripe doesn't retry
  }

  res.json({ received: true });
});

// ── EMAIL ─────────────────────────────────────────────────────────────
// Design tokens — kept close to app brand (Barlow Condensed + Signal Red)
// Using table layout + inline styles for Outlook/Gmail compatibility.
const BRAND = {
  ink: '#0E0E0E',      // near-black, matches app --bg
  ink2: '#333333',     // body text
  dim: '#666666',      // secondary text
  faint: '#999999',    // footer/meta
  border: '#E5E5E5',   // subtle divider
  red: '#E8001A',      // Signal Red — matches app --red
  rdk: '#A50013',      // darker red for hover/emphasis
  white: '#FFFFFF',
  light: '#F7F7F7',    // soft background for panels
};

// Fonts — Barlow from Google Fonts falls back gracefully in Outlook
const FONT_BODY = `'Barlow', -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif`;
const FONT_HEAD = `'Barlow Condensed', 'Barlow', -apple-system, 'Segoe UI', Arial, sans-serif`;

function emailHeader() {
  // Black header bar with PEAK logo — red underline like in the app
  return `
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:${BRAND.ink};">
    <tr>
      <td align="center" style="padding:32px 20px 28px;">
        <div style="display:inline-block;text-align:center;">
          <div style="font-family:${FONT_HEAD};font-weight:900;font-size:32px;letter-spacing:7px;color:${BRAND.white};line-height:1;">PEAK</div>
          <div style="width:60px;height:2px;background:${BRAND.red};margin:6px auto 4px;"></div>
          <div style="font-family:${FONT_BODY};font-size:9px;font-weight:700;letter-spacing:3px;color:#888;text-transform:uppercase;">by MJ Performance</div>
        </div>
      </td>
    </tr>
  </table>`;
}

function emailButton(href, label) {
  // Bulletproof button — works in Outlook via VML fallback concept (simplified)
  return `
  <table role="presentation" cellpadding="0" cellspacing="0" border="0" align="center" style="margin:0 auto;">
    <tr>
      <td align="center" bgcolor="${BRAND.red}" style="background:${BRAND.red};">
        <a href="${href}" target="_blank" style="display:inline-block;font-family:${FONT_HEAD};font-size:13px;font-weight:900;letter-spacing:3px;text-transform:uppercase;color:${BRAND.white};text-decoration:none;padding:16px 36px;background:${BRAND.red};">${label}</a>
      </td>
    </tr>
  </table>`;
}

function emailFooter(email) {
  const unsub = `${BACKEND_URL}/unsubscribe?email=${encodeURIComponent(email)}`;
  return `
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:${BRAND.ink};">
    <tr>
      <td style="padding:28px 30px 32px;font-family:${FONT_BODY};font-size:11px;line-height:1.7;color:#888;text-align:center;">
        <p style="margin:0 0 10px;">Du erhältst diese E-Mail, weil du dich bei PEAK registriert hast.<br>You're receiving this because you signed up for PEAK.</p>
        <p style="margin:0 0 14px;">
          <a href="${FRONTEND_URL}/impressum" style="color:#AAA;text-decoration:none;">Impressum</a>
          <span style="color:#555;"> · </span>
          <a href="${FRONTEND_URL}/datenschutz" style="color:#AAA;text-decoration:none;">Datenschutz</a>
          <span style="color:#555;"> · </span>
          <a href="${unsub}" style="color:#AAA;text-decoration:none;">Unsubscribe</a>
        </p>
        <p style="margin:0;color:#666;font-size:10px;letter-spacing:0.5px;">${COMPANY.name}</p>
      </td>
    </tr>
  </table>`;
}

// Branded magic-link email. Matches welcome-mail design. Bilingual via lang param.
function buildOtpEmail(code, email, lang) {
  const de = lang === 'de';
  const L = {
    subject: de ? 'Dein PEAK Login-Code' : 'Your PEAK login code',
    label: de ? '🔐 Login-Code' : '🔐 Login code',
    h1a: de ? 'DEIN CODE' : 'YOUR CODE',
    h1b: de ? 'FÜR PEAK' : 'FOR PEAK',
    intro: de
      ? 'Gib diesen 6-stelligen Code in der PEAK-App ein, um dich anzumelden:'
      : 'Enter this 6-digit code in the PEAK app to sign in:',
    expiry: de
      ? 'Der Code ist 10 Minuten gültig.'
      : 'The code expires in 10 minutes.',
    warning: de
      ? 'Wenn du diesen Code nicht angefordert hast, ignoriere diese E-Mail.'
      : 'If you did not request this code, ignore this email.',
    footer: de
      ? 'PEAK by MJ Performance · Impressum: ' + FRONTEND_URL + '/impressum'
      : 'PEAK by MJ Performance · Legal: ' + FRONTEND_URL + '/impressum',
  };

  // Format code with middle space for readability: "123 456"
  const codePretty = code.slice(0, 3) + ' ' + code.slice(3);

  const html = `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${L.subject}</title></head>
<body style="margin:0;padding:0;background:#F5F5F1;font-family:'Helvetica Neue',Arial,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#F5F5F1;padding:40px 20px"><tr><td align="center">
<table width="560" cellpadding="0" cellspacing="0" style="max-width:560px;background:#fff;border:1px solid #E6E6E1">
  <tr><td style="background:#0E0E0E;padding:28px 32px">
    <div style="color:#fff;font-family:'Helvetica Neue',Arial,sans-serif;font-weight:900;font-size:28px;letter-spacing:4px">PEAK</div>
    <div style="color:#E8001A;font-weight:700;font-size:10px;letter-spacing:3px;margin-top:4px">BY MJ PERFORMANCE</div>
  </td></tr>
  <tr><td style="padding:40px 32px 32px">
    <div style="color:#E8001A;font-weight:900;font-size:11px;letter-spacing:2.5px;margin-bottom:10px;text-transform:uppercase">${L.label}</div>
    <h1 style="margin:0 0 18px;font-size:30px;line-height:1.15;font-weight:900;letter-spacing:1px;color:#0E0E0E">${L.h1a}<br><span style="color:#E8001A">${L.h1b}</span></h1>
    <p style="margin:0 0 28px;font-size:15px;line-height:1.6;color:#3a3a3a">${L.intro}</p>
    <div style="background:#0E0E0E;color:#fff;padding:28px 32px;text-align:center;margin:0 0 22px">
      <div style="font-family:'Courier New',monospace;font-weight:900;font-size:42px;letter-spacing:12px;color:#fff">${codePretty}</div>
    </div>
    <p style="margin:0 0 10px;font-size:12px;color:#666">⏱ ${L.expiry}</p>
    <p style="margin:0;font-size:11px;color:#999;line-height:1.5">${L.warning}</p>
  </td></tr>
  <tr><td style="background:#F5F5F1;padding:18px 32px;border-top:1px solid #E6E6E1">
    <div style="font-size:10px;color:#999;letter-spacing:1.2px;text-align:center">${L.footer}</div>
  </td></tr>
</table></td></tr></table></body></html>`;

  const text = `${L.h1a} ${L.h1b}\n\n${L.intro}\n\n  ${codePretty}\n\n${L.expiry}\n\n${L.warning}\n\n— ${L.footer}`;

  return { subject: L.subject, html, text };
}

function buildMagicLinkEmail(magicLink, email, lang) {
  const de = lang === 'de';
  const L = {
    subject: de ? 'Dein PEAK Login-Link' : 'Your PEAK login link',
    label: de ? '🔐 Login-Link' : '🔐 Login link',
    h1a: de ? 'DEIN LINK' : 'YOUR LINK',
    h1b: de ? 'ZU PEAK' : 'TO PEAK',
    intro: de
      ? 'Klick den Button unten, um dich bei PEAK einzuloggen. Kein Passwort nötig.'
      : 'Click the button below to sign in to PEAK. No password needed.',
    cta: de ? 'Jetzt einloggen' : 'Sign in now',
    expire: de
      ? 'Dieser Link ist 1 Stunde gültig. Falls du diesen Login nicht angefordert hast, ignoriere diese E-Mail.'
      : 'This link expires in 1 hour. If you didn\'t request this login, please ignore this email.',
    fallback: de ? 'Funktioniert der Button nicht? Kopiere diesen Link in deinen Browser:' : 'Button not working? Copy this link into your browser:'
  };
  const html = emailShell(`
    <tr><td>${emailHeader()}</td></tr>
    <tr><td style="padding:48px 40px 8px;">
      <p style="margin:0 0 14px;font-family:${FONT_BODY};font-size:11px;font-weight:700;letter-spacing:3px;color:${BRAND.red};text-transform:uppercase;">${L.label}</p>
      <h1 style="margin:0 0 16px;font-family:${FONT_HEAD};font-weight:900;font-size:38px;line-height:1.05;letter-spacing:1px;text-transform:uppercase;color:${BRAND.ink};">
        ${L.h1a}<br>${L.h1b}
      </h1>
      <p style="margin:0 0 32px;font-family:${FONT_BODY};font-size:15px;line-height:1.65;color:${BRAND.ink2};">${L.intro}</p>
    </td></tr>
    <tr><td style="padding:0 40px 32px;">
      ${emailButton(magicLink, L.cta)}
    </td></tr>
    <tr><td style="padding:0 40px 32px;">
      <p style="margin:0 0 12px;font-family:${FONT_BODY};font-size:12px;line-height:1.6;color:${BRAND.faint};">${L.fallback}</p>
      <p style="margin:0;font-family:${FONT_BODY};font-size:11px;line-height:1.5;color:${BRAND.dim};word-break:break-all;">
        <a href="${magicLink}" style="color:${BRAND.red};text-decoration:underline;">${magicLink}</a>
      </p>
    </td></tr>
    <tr><td style="padding:0 40px 40px;">
      <p style="margin:0;font-family:${FONT_BODY};font-size:12px;line-height:1.6;color:${BRAND.faint};">${L.expire}</p>
    </td></tr>
    <tr><td>${emailFooter(email)}</td></tr>
  `);
  return { subject: L.subject, html };
}

// Shared wrapper — produces a full valid HTML email document
function emailShell(innerHTML) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light only">
<meta name="supported-color-schemes" content="light only">
<link href="https://fonts.googleapis.com/css2?family=Barlow:wght@400;500;700&family=Barlow+Condensed:wght@700;900&display=swap" rel="stylesheet">
<title>PEAK</title>
</head>
<body style="margin:0;padding:0;background:${BRAND.light};font-family:${FONT_BODY};">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:${BRAND.light};">
    <tr>
      <td align="center" style="padding:0;">
        <table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="max-width:600px;width:100%;background:${BRAND.white};">
          ${innerHTML}
        </table>
      </td>
    </tr>
  </table>
</body>
</html>`;
}

async function sendEmail(to, type, data) {
  // Check if user has unsubscribed AND fetch language preference
  let unsubscribed = false;
  let userLang = null;
  let userGoal = '';
  let userGoals = [];
  let userSport = '';
  try {
    const { data: user } = await supabase
      .from('users')
      .select('unsubscribed,goal,goals,sport,lang')
      .eq('email', to)
      .maybeSingle();
    unsubscribed = user?.unsubscribed === true;
    if (user?.goal) userGoal = user.goal;
    if (Array.isArray(user?.goals)) userGoals = user.goals;
    if (user?.sport) userSport = user.sport;
    if (user?.lang === 'de' || user?.lang === 'en') userLang = user.lang;
  } catch (err) {
    console.error('Unsubscribe-check exception:', err.message);
  }
  if (unsubscribed) return;

  // Language detection priority:
  //   1. explicit data.lang
  //   2. user.lang from DB
  //   3. goal-text heuristic
  //   4. fallback 'en'
  const lang = (data?.lang === 'de' || data?.lang === 'en') ? data.lang
    : (userLang ? userLang
    : (userGoal && /abnehmen|aufbauen|gesundheit|energie|ausdauer|muskel|gewicht/i.test(userGoal) ? 'de'
    : 'en'));
  const de = lang === 'de';

  const name = data?.name || '';
  const sport = data?.sport || userSport || '';
  const goal = data?.goal || userGoal || '';
  const goalsList = Array.isArray(data?.goals) && data.goals.length ? data.goals
                  : (Array.isArray(userGoals) && userGoals.length ? userGoals
                  : (goal ? [goal] : []));

  // Build goal headline text (visible in email):
  // - 1 goal: "<goal>"
  // - 2 goals: "<goal1> + <goal2>"
  // - 3 goals: "<goal1> + 2 more"
  let goalHeadline = '';
  if (goalsList.length === 1) goalHeadline = goalsList[0];
  else if (goalsList.length === 2) goalHeadline = goalsList[0] + ' + ' + goalsList[1];
  else if (goalsList.length >= 3) goalHeadline = goalsList[0] + (de ? ' + ' + (goalsList.length - 1) + ' weitere Ziele' : ' + ' + (goalsList.length - 1) + ' more goals');

  // Localised strings
  const L = de ? {
    // Subject branches by tier — Basic and Premium share the "trial" subject;
    // Free uses the cross-device login subject.
    welcomeSubject: (tier) => tier === 'free' ? 'Dein PEAK-Plan ist live — diese Mail ist dein Cross-Device-Login' : 'Willkommen bei PEAK — dein Plan ist bereit',
    welcomeLabel: (n) => 'Willkommen' + (n ? ', ' + n : ''),
    welcomeH1a: 'Dein Plan',
    welcomeH1b: 'ist live.',
    welcomeH1FreeB: 'läuft.',
    welcomeIntro: (tier, goal, sport, trialDays) => {
      if (tier === 'free') {
        return 'Du bist bereits eingeloggt — diese Mail ist dein Backup. Speicher sie, falls du PEAK auf einem anderen Gerät öffnen willst (Handy, Tablet). Klick einfach unten auf den Button und du landest direkt in deinem Plan, ohne Passwort.';
      }
      const days = trialDays && trialDays > 0 ? trialDays : 7;
      // Lead with the auto-login note — same logic as Free, just shorter.
      // Most paid signups happen on the device they'll use day-to-day, so
      // we acknowledge that and frame the email as a backup link, not a
      // "click here to start" CTA. Keeps Welcome consistent across tiers.
      let intro = `Du bist bereits eingeloggt — der Button unten ist dein Backup-Link für andere Geräte (Handy, Tablet). `;
      intro += `Deine ${days}-Tage-Testphase läuft — keine Abbuchung bis Tag ${days + 1}. `;
      if (sport && goal) intro += `Dein individueller ${sport}-Plan ist abgestimmt auf „${goal}".`;
      else if (sport) intro += `Dein individueller ${sport}-Plan ist bereit.`;
      else if (goal) intro += `Maßgeschneidert auf dein Ziel: „${goal}".`;
      else intro += 'KI-gestützte Ernährung, Training und Regeneration — auf dich zugeschnitten.';
      return intro;
    },
    // "What's included" list — three flavours: free, basic, premium.
    // Each list reflects the documented tier matrix (May 2026):
    //   Free    = Vorschau (1 Woche Training, 7 Tage Essensplan)
    //   Basic   = volle Pläne, Tracking, 3 Updates/Mo, Recovery-Plan
    //   Premium = Basic + 12W-Progression, Mobility, Mood, Scanner, Log
    includesFree: 'Dein Gratis-Plan enthält',
    includesBasic: 'In Basic enthalten',
    includesPremium: 'In Premium enthalten',
    f1: (sport) => sport ? `KI-Ernährungsplan für dein ${sport}-Training` : 'KI-Ernährungsplan, passend zu Ziel & Geschmack',
    f2Free: (sport) => sport ? `${sport}-Training — eine Woche zum Reinschnuppern` : 'Trainings-Vorschau für deine Sportart',
    f2Basic: (sport) => sport ? `Voller ${sport}-Trainingsplan, alle Wochen` : 'Voller Trainingsplan für deine Sportart',
    f2Premium: (sport) => sport ? `12-Wochen-${sport}-Programm mit Progression` : '12-Wochen-Programm mit Progression',
    f3Free: '1 Plan-Update / 30 Tage inklusive',
    f3Basic: 'Voller Regenerationsplan: Schlaf, Hydration, Tools',
    f3Premium: 'Mobility, Stretching & Recovery-Tools',
    f4Free: 'Jederzeit Upgrade möglich',
    f4Basic: 'Meal-Tracking & Tagesfortschritt',
    f4Premium: 'Barcode-Scanner, Protokoll & KI-Anpassung',
    stepsTitle: 'Deine ersten 3 Schritte',
    step1: 'Plan öffnen und Essensplan anschauen',
    step2Free: 'Trainings-Tab — Woche 1 ansehen',
    step2Paid: 'Erstes Training starten',
    step3Free: 'Mahlzeit protokollieren für Fortschritts-Tracking',
    step3Paid: 'Regenerations-Protokoll lesen',
    boxFree: '<strong>Gratis-Plan.</strong> Keine Karte, keine Abbuchung. Mit Basic schaltest du den vollen Plan inkl. Regeneration & Tools frei. Mit Premium kommen 12-Wochen-Progression, Mobility, KI-Anpassung und mehr dazu.',
    boxPaid: (trialDays, voucherCode) => {
      const days = trialDays && trialDays > 0 ? trialDays : 7;
      let txt = `<strong>${days} Tage kostenlos.</strong> Erst ab Tag ${days + 1} wird abgebucht.`;
      // If a voucher gave them a longer-than-default trial, call it out so
      // they know why they got the extra time. Helps reduce "warum bin ich
      // erst in 28 Tagen dran?" support questions when LAUNCH4W or partner
      // codes are active.
      if (voucherCode && days > 7) {
        txt += ` Mit deinem Code <strong>${voucherCode}</strong> hast du ${days - 7} Bonustage geschenkt bekommen.`;
      }
      txt += ' Erinnerung 1 Tag vor Ende.';
      return txt;
    },
    ctaOpen: 'Plan öffnen',
    day6Subject: 'Letzter Tag — deine Testphase endet morgen',
    day6Label: 'Letzte 24 Stunden',
    day6H1: (n) => (n ? n + ',<br>' : '') + 'morgen<br>geht es los.',
    day6Body: 'Deine kostenlose Testphase endet morgen. Dein PEAK-Abo startet automatisch — du musst nichts tun, um weiterzumachen.',
    day6Box: '<strong>Kündigen:</strong> PEAK öffnen → Einstellungen → Abonnement → Testphase beenden. In 10 Sekunden erledigt.',
    day6CTA: 'Plan behalten',
    // ── CANCELLATION EMAILS (DE) ──
    // Tier-aware: a Basic user shouldn't read "Premium ends" (was confusing
    // and wrong). Helper renders the correct plan label in either language.
    cancelTierLabel: (tier) => tier === 'basic' ? 'Basic' : 'Premium',
    cancelConfirmedSubject: 'Deine PEAK-Kündigung ist bestätigt',
    cancelConfirmedLabel: 'Kündigung bestätigt',
    cancelConfirmedH1a: 'Schade,',
    cancelConfirmedH1b: 'dass du gehst.',
    cancelConfirmedBody: (endDate, tier) => {
      const label = tier === 'basic' ? 'Basic' : 'Premium';
      return `Deine Kündigung wurde registriert. Dein ${label}-Zugang bleibt bis zum <strong>${endDate}</strong> aktiv — dann wird dein Account automatisch auf den Free-Plan umgestellt.`;
    },
    cancelConfirmedNote: 'Bis dahin kannst du PEAK in vollem Umfang nutzen. Deine Daten, Pläne und Fortschritte bleiben erhalten.',
    cancelConfirmedReactivateBox: '<strong>Wieder aktivieren?</strong> Kein Problem — öffne einfach PEAK und wähle einen Plan. Dein Profil ist gespeichert.',
    cancelConfirmedCTA: 'PEAK öffnen',
    cancelReminderSubject: (days, tier) => {
      const label = tier === 'basic' ? 'Basic' : 'Premium';
      return days === 3 ? `Dein ${label} endet in 3 Tagen` : `Dein ${label} endet bald`;
    },
    cancelReminderLabel: (days, tier) => {
      const label = tier === 'basic' ? 'Basic' : 'Premium';
      return `Noch ${days} Tage ${label}`;
    },
    cancelReminderH1a: 'Nutze deine',
    cancelReminderH1b: 'letzten Tage.',
    cancelReminderBody: (endDate, tier) => {
      const label = tier === 'basic' ? 'Basic' : 'Premium';
      return `Am <strong>${endDate}</strong> endet dein ${label}-Zugang. Bis dahin: volle Power — Pläne anpassen, Workouts tracken, Rezepte checken.`;
    },
    cancelReminderBox: '💡 <strong>Noch nicht sicher?</strong> Du kannst jederzeit zurückkehren — dein Profil und deine Fortschritte bleiben 30 Tage erhalten.',
    cancelReminderCTA: 'Jetzt PEAK nutzen',
    cancelFinalSubject: (tier) => {
      const label = tier === 'basic' ? 'Basic' : 'Premium';
      return `Dein PEAK ${label} ist beendet`;
    },
    cancelFinalLabel: (tier) => `${tier === 'basic' ? 'Basic' : 'Premium'} beendet`,
    cancelFinalH1a: (tier) => tier === 'basic' ? 'Basic' : 'Premium',
    cancelFinalH1b: 'ist beendet.',
    cancelFinalBody: (tier) => {
      const label = tier === 'basic' ? 'Basic' : 'Premium';
      return `Dein ${label}-Abo ist heute ausgelaufen. Dein Profil bleibt gespeichert — du kannst jederzeit upgraden und genau dort weitermachen, wo du aufgehört hast.`;
    },
    cancelFinalReactivate: 'Plan vermissen? Hol ihn dir mit einem Klick zurück.',
    cancelFinalCTA: 'Plan zurückholen',
    accountDeletedSubject: 'Dein PEAK-Konto wurde gelöscht',
    accountDeletedLabel: 'Konto gelöscht',
    accountDeletedH1a: 'Dein Konto',
    accountDeletedH1b: 'ist gelöscht.',
    accountDeletedBody: (name) => (name ? name + ', d' : 'D') + 'ein PEAK-Konto wurde auf deinen Wunsch hin vollständig gelöscht. Alle deine Daten (Profil, Ziele, Fortschritt) wurden aus unserer Datenbank entfernt. Falls du ein Premium-Abo hattest, wurde es beendet.',
    accountDeletedLegal: 'Hinweis: Rechnungen und Zahlungsdaten müssen wir aus steuerrechtlichen Gründen für 10 Jahre aufbewahren (§147 AO). Alle anderen personenbezogenen Daten sind gelöscht.',
    accountDeletedBye: 'Danke, dass du PEAK ausprobiert hast. Du bist jederzeit wieder willkommen.',
    // ── PAYMENT FAILED (DE) ──
    paymentFailedSubject: 'Zahlung fehlgeschlagen — bitte Karte aktualisieren',
    paymentFailedLabel: 'Zahlung fehlgeschlagen',
    paymentFailedH1a: 'Deine Zahlung',
    paymentFailedH1b: 'ging schief.',
    paymentFailedBody: (name) => (name ? name + ', w' : 'W') + 'ir konnten deine letzte Abbuchung nicht durchführen. Häufigster Grund: abgelaufene Karte oder fehlendes Guthaben. Stripe versucht es in den nächsten Tagen automatisch erneut.',
    paymentFailedBox: '<strong>Was du tun kannst:</strong> Öffne PEAK, gehe zu Einstellungen → Abonnement → Zahlungsmethode und hinterlege eine aktuelle Karte. Sobald die Zahlung durchgeht, läuft dein Abo nahtlos weiter.',
    paymentFailedCTA: 'Karte aktualisieren',
    // ── TRIAL ENDING (DE) — fired by Stripe 3 days before trial_end ──
    // Body uses actual trial length from Stripe sub, not a hardcoded "7"
    // — vouchers (LAUNCH4W = 28d) make any fixed number wrong.
    trialEndingSubject: 'Deine Testphase endet in 3 Tagen',
    trialEndingLabel: 'Noch 3 Tage gratis',
    trialEndingH1a: 'In 3 Tagen',
    trialEndingH1b: 'startet dein Abo.',
    trialEndingBody: (name, dateStr, trialDays) => {
      const dur = trialDays && trialDays > 0 ? `${trialDays}-tägige` : '';
      return (name ? name + ', d' : 'D') + 'eine ' + dur + ' Testphase endet ' + (dateStr ? 'am <strong>' + dateStr + '</strong>' : 'in 3 Tagen') + '. Danach startet dein PEAK-Abo automatisch — du musst nichts tun, um weiterzumachen.';
    },
    trialEndingBox: '<strong>Möchtest du nicht weitermachen?</strong> Öffne PEAK → Einstellungen → Abonnement → Testphase beenden. 10 Sekunden, kein Telefonat.',
    trialEndingCTA: 'PEAK öffnen',
  } : {
    welcomeSubject: (tier) => tier === 'free' ? 'Your PEAK plan is live — this email is your cross-device login' : 'Welcome to PEAK — your plan is ready',
    welcomeLabel: (n) => 'Welcome' + (n ? ', ' + n : ''),
    welcomeH1a: 'Your plan is',
    welcomeH1b: 'live.',
    welcomeH1FreeB: 'live.',
    welcomeIntro: (tier, goal, sport, trialDays) => {
      if (tier === 'free') {
        return 'You\'re already logged in — this email is your backup. Save it if you ever want to open PEAK on another device (phone, tablet). Just tap the button below and you\'ll land straight in your plan, no password.';
      }
      const days = trialDays && trialDays > 0 ? trialDays : 7;
      // Same auto-login framing as Free, just compact for paid tiers.
      let intro = `You're already logged in — the button below is your backup link for other devices (phone, tablet). `;
      intro += `Your ${days}-day trial is running — no charge until Day ${days + 1}. `;
      if (sport && goal) intro += `Your custom ${sport} plan is tuned to "${goal}".`;
      else if (sport) intro += `Your custom ${sport} programme is ready.`;
      else if (goal) intro += `Built around your goal: "${goal}".`;
      else intro += 'AI-built nutrition, training and recovery, tuned to you.';
      return intro;
    },
    includesFree: 'Your free plan includes',
    includesBasic: 'Basic includes',
    includesPremium: 'Premium includes',
    f1: (sport) => sport ? `AI nutrition plan for your ${sport} training` : 'AI nutrition plan, matched to goal and taste',
    f2Free: (sport) => sport ? `${sport} training — 1-week preview` : '1-week training preview for your sport',
    f2Basic: (sport) => sport ? `Full ${sport} training plan, every week` : 'Full training plan for your sport',
    f2Premium: (sport) => sport ? `12-week ${sport} programme with progression` : '12-week programme with progression',
    f3Free: '1 plan update / 30 days included',
    f3Basic: 'Full recovery plan: sleep, hydration, tools',
    f3Premium: 'Mobility, stretching & recovery tools',
    f4Free: 'Upgrade any time',
    f4Basic: 'Meal tracking & daily progress',
    f4Premium: 'Barcode scanner, log & AI adaptation',
    stepsTitle: 'Your first 3 steps',
    step1: 'Open your plan and check your meals',
    step2Free: 'Open the Training tab — see Week 1',
    step2Paid: 'Start your first training session',
    step3Free: 'Log a meal to track progress',
    step3Paid: 'Read your recovery protocol',
    boxFree: '<strong>Free plan.</strong> No card, no charge. Basic unlocks the full plan including recovery & tools. Premium adds 12-week progression, mobility, AI adaptation and more.',
    boxPaid: (trialDays, voucherCode) => {
      const days = trialDays && trialDays > 0 ? trialDays : 7;
      let txt = `<strong>${days} days free.</strong> No charge until Day ${days + 1}.`;
      if (voucherCode && days > 7) {
        txt += ` Your code <strong>${voucherCode}</strong> added ${days - 7} bonus days.`;
      }
      txt += ' Reminder 1 day before end.';
      return txt;
    },
    ctaOpen: 'Open my plan',
    day6Subject: 'Final day — your PEAK trial ends tomorrow',
    day6Label: 'Final 24 hours',
    day6H1: (n) => (n ? n + ',<br>' : '') + 'tomorrow<br>it begins.',
    day6Body: 'Your free trial ends tomorrow. Your PEAK subscription begins automatically — no action needed to continue.',
    day6Box: '<strong>To cancel:</strong> Open PEAK → Settings → Subscription → Cancel trial. Done in 10 seconds.',
    day6CTA: 'Keep my plan',
    // ── CANCELLATION EMAILS (EN) ──
    cancelTierLabel: (tier) => tier === 'basic' ? 'Basic' : 'Premium',
    cancelConfirmedSubject: 'Your PEAK cancellation is confirmed',
    cancelConfirmedLabel: 'Cancellation confirmed',
    cancelConfirmedH1a: 'Sorry to',
    cancelConfirmedH1b: 'see you go.',
    cancelConfirmedBody: (endDate, tier) => {
      const label = tier === 'basic' ? 'Basic' : 'Premium';
      return `Your cancellation has been processed. Your ${label} access remains active until <strong>${endDate}</strong> — then your account switches to the Free plan automatically.`;
    },
    cancelConfirmedNote: 'Until then, use PEAK to the fullest. Your data, plans and progress are safe.',
    cancelConfirmedReactivateBox: '<strong>Changed your mind?</strong> No problem — open PEAK and pick a plan. Your profile is saved.',
    cancelConfirmedCTA: 'Open PEAK',
    cancelReminderSubject: (days, tier) => {
      const label = tier === 'basic' ? 'Basic' : 'Premium';
      return days === 3 ? `Your ${label} ends in 3 days` : `Your ${label} ends soon`;
    },
    cancelReminderLabel: (days, tier) => {
      const label = tier === 'basic' ? 'Basic' : 'Premium';
      return `${days} days of ${label} left`;
    },
    cancelReminderH1a: 'Use your',
    cancelReminderH1b: 'final days.',
    cancelReminderBody: (endDate, tier) => {
      const label = tier === 'basic' ? 'Basic' : 'Premium';
      return `Your ${label} access ends on <strong>${endDate}</strong>. Until then: full power — tune plans, track workouts, check recipes.`;
    },
    cancelReminderBox: '💡 <strong>Still deciding?</strong> You can come back anytime — your profile and progress are kept for 30 days.',
    cancelReminderCTA: 'Use PEAK now',
    cancelFinalSubject: (tier) => `Your PEAK ${tier === 'basic' ? 'Basic' : 'Premium'} has ended`,
    cancelFinalLabel: (tier) => `${tier === 'basic' ? 'Basic' : 'Premium'} ended`,
    cancelFinalH1a: (tier) => tier === 'basic' ? 'Basic' : 'Premium',
    cancelFinalH1b: 'has ended.',
    cancelFinalBody: (tier) => {
      const label = tier === 'basic' ? 'Basic' : 'Premium';
      return `Your ${label} subscription ended today. Your profile stays saved — you can upgrade any time and pick up exactly where you left off.`;
    },
    cancelFinalReactivate: 'Missing your plan? Bring it back with one click.',
    cancelFinalCTA: 'Bring back my plan',
    accountDeletedSubject: 'Your PEAK account has been deleted',
    accountDeletedLabel: 'Account deleted',
    accountDeletedH1a: 'Your account',
    accountDeletedH1b: 'is deleted.',
    accountDeletedBody: (name) => (name ? name + ', y' : 'Y') + 'our PEAK account has been fully deleted at your request. All your data (profile, goals, progress) has been removed from our database. If you had a Premium subscription, it has been ended.',
    accountDeletedLegal: 'Note: invoices and payment records must be retained for 10 years for tax-law reasons (German §147 AO). All other personal data has been deleted.',
    accountDeletedBye: 'Thanks for trying PEAK. You\'re always welcome back.',
    // ── PAYMENT FAILED (EN) ──
    paymentFailedSubject: 'Payment failed — please update your card',
    paymentFailedLabel: 'Payment failed',
    paymentFailedH1a: 'Your payment',
    paymentFailedH1b: 'didn\'t go through.',
    paymentFailedBody: (name) => (name ? name + ', w' : 'W') + 'e couldn\'t process your last payment. Most common reason: expired card or insufficient funds. Stripe will retry automatically over the next few days.',
    paymentFailedBox: '<strong>What you can do:</strong> Open PEAK, go to Settings → Subscription → Payment method and add a current card. Once the charge goes through, your subscription continues without interruption.',
    paymentFailedCTA: 'Update card',
    // ── TRIAL ENDING (EN) — fired by Stripe 3 days before trial_end ──
    trialEndingSubject: 'Your trial ends in 3 days',
    trialEndingLabel: '3 days of trial left',
    trialEndingH1a: 'In 3 days',
    trialEndingH1b: 'your plan starts.',
    trialEndingBody: (name, dateStr, trialDays) => {
      const dur = trialDays && trialDays > 0 ? `${trialDays}-day` : '';
      return (name ? name + ', y' : 'Y') + 'our ' + dur + ' trial ends ' + (dateStr ? 'on <strong>' + dateStr + '</strong>' : 'in 3 days') + '. After that, your PEAK subscription starts automatically — you don\'t need to do anything to continue.';
    },
    trialEndingBox: '<strong>Don\'t want to continue?</strong> Open PEAK → Settings → Subscription → End trial. Ten seconds, no phone call.',
    trialEndingCTA: 'Open PEAK',
  };

  // Responsive email CSS: proper mobile breakpoint + padding reduction
  const RESPONSIVE_CSS = `<style>
    @media only screen and (max-width: 600px) {
      .email-pad { padding-left: 24px !important; padding-right: 24px !important; }
      .email-pad-big { padding: 36px 24px 8px !important; }
      .email-h1 { font-size: 32px !important; }
      .email-cta { padding-left: 24px !important; padding-right: 24px !important; }
    }
  </style>`;

  const templates = {
    welcome: {
      subject: L.welcomeSubject(data?.tier || (data?.isFree ? 'free' : 'premium')),
      html: emailShell(RESPONSIVE_CSS + (() => {
        // Resolve tier with backwards-compat: legacy callers pass isFree=true
        // for the Free path; new callers pass tier explicitly. Default to
        // 'premium' if neither is set so paid signups keep working.
        const tier = data?.tier || (data?.isFree ? 'free' : 'premium');
        const isFree = tier === 'free';
        const isBasic = tier === 'basic';
        const isPremium = tier === 'premium';
        // Feature list lookup — pulls the right localised strings per tier.
        const includesLbl = isFree ? L.includesFree : (isBasic ? L.includesBasic : L.includesPremium);
        const f2 = isFree ? L.f2Free(sport) : (isBasic ? L.f2Basic(sport) : L.f2Premium(sport));
        const f3 = isFree ? L.f3Free : (isBasic ? L.f3Basic : L.f3Premium);
        const f4 = isFree ? L.f4Free : (isBasic ? L.f4Basic : L.f4Premium);
        const trialDays = data?.trialDays;
        return `
        <tr><td>${emailHeader()}</td></tr>
        <tr><td class="email-pad-big" style="padding:48px 40px 8px;">
          <p style="margin:0 0 14px;font-family:${FONT_BODY};font-size:11px;font-weight:700;letter-spacing:3px;color:${BRAND.red};text-transform:uppercase;">${L.welcomeLabel(name)}</p>
          <h1 class="email-h1" style="margin:0 0 14px;font-family:${FONT_HEAD};font-weight:900;font-size:38px;line-height:1.05;letter-spacing:1px;text-transform:uppercase;color:${BRAND.ink};">
            ${L.welcomeH1a}<br>${isFree ? L.welcomeH1FreeB : L.welcomeH1b}
          </h1>
          ${goalHeadline ? `<p style="margin:0 0 18px;font-family:${FONT_HEAD};font-size:13px;font-weight:700;letter-spacing:2px;color:${BRAND.red};text-transform:uppercase;">🎯 ${goalHeadline}</p>` : ''}
          <p style="margin:0 0 32px;font-family:${FONT_BODY};font-size:15px;line-height:1.65;color:${BRAND.ink2};">
            ${L.welcomeIntro(tier, goal, sport, trialDays)}
          </p>
        </td></tr>

        <tr><td class="email-pad" style="padding:0 40px 8px;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-top:1px solid ${BRAND.border};">
            <tr><td style="padding:24px 0 8px;">
              <p style="margin:0 0 16px;font-family:${FONT_HEAD};font-size:11px;font-weight:900;letter-spacing:3px;color:${BRAND.ink};text-transform:uppercase;">${includesLbl}</p>
            </td></tr>
            <tr><td style="font-family:${FONT_BODY};font-size:14px;line-height:1.7;color:${BRAND.ink2};padding-bottom:6px;">
              <span style="color:${BRAND.red};font-weight:700;">—</span>&nbsp;&nbsp;${L.f1(sport)}
            </td></tr>
            <tr><td style="font-family:${FONT_BODY};font-size:14px;line-height:1.7;color:${BRAND.ink2};padding-bottom:6px;">
              <span style="color:${BRAND.red};font-weight:700;">—</span>&nbsp;&nbsp;${f2}
            </td></tr>
            <tr><td style="font-family:${FONT_BODY};font-size:14px;line-height:1.7;color:${BRAND.ink2};padding-bottom:6px;">
              <span style="color:${BRAND.red};font-weight:700;">—</span>&nbsp;&nbsp;${f3}
            </td></tr>
            <tr><td style="font-family:${FONT_BODY};font-size:14px;line-height:1.7;color:${BRAND.ink2};padding-bottom:24px;">
              <span style="color:${BRAND.red};font-weight:700;">—</span>&nbsp;&nbsp;${f4}
            </td></tr>
          </table>
        </td></tr>

        <tr><td class="email-pad" style="padding:8px 40px 28px;">
          <p style="margin:0 0 14px;font-family:${FONT_HEAD};font-size:11px;font-weight:900;letter-spacing:3px;color:${BRAND.ink};text-transform:uppercase;">${L.stepsTitle}</p>
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr><td style="padding:8px 0;">
              <table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>
                <td valign="top" style="padding-right:12px;">
                  <div style="width:24px;height:24px;background:${BRAND.red};color:${BRAND.white};font-family:${FONT_HEAD};font-weight:900;font-size:13px;text-align:center;line-height:24px;">1</div>
                </td>
                <td style="font-family:${FONT_BODY};font-size:14px;line-height:1.5;color:${BRAND.ink2};padding-top:2px;">${L.step1}</td>
              </tr></table>
            </td></tr>
            <tr><td style="padding:8px 0;">
              <table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>
                <td valign="top" style="padding-right:12px;">
                  <div style="width:24px;height:24px;background:${BRAND.red};color:${BRAND.white};font-family:${FONT_HEAD};font-weight:900;font-size:13px;text-align:center;line-height:24px;">2</div>
                </td>
                <td style="font-family:${FONT_BODY};font-size:14px;line-height:1.5;color:${BRAND.ink2};padding-top:2px;">${isFree ? L.step2Free : L.step2Paid}</td>
              </tr></table>
            </td></tr>
            <tr><td style="padding:8px 0;">
              <table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>
                <td valign="top" style="padding-right:12px;">
                  <div style="width:24px;height:24px;background:${BRAND.red};color:${BRAND.white};font-family:${FONT_HEAD};font-weight:900;font-size:13px;text-align:center;line-height:24px;">3</div>
                </td>
                <td style="font-family:${FONT_BODY};font-size:14px;line-height:1.5;color:${BRAND.ink2};padding-top:2px;">${isFree ? L.step3Free : L.step3Paid}</td>
              </tr></table>
            </td></tr>
          </table>
        </td></tr>

        <tr><td class="email-pad" style="padding:8px 40px 36px;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:${BRAND.light};border-left:3px solid ${BRAND.red};">
            <tr><td style="padding:18px 22px;font-family:${FONT_BODY};font-size:13px;line-height:1.6;color:${BRAND.ink2};">
              ${isFree ? L.boxFree : L.boxPaid(trialDays, data?.voucherCode || '')}
            </td></tr>
          </table>
        </td></tr>

        <tr><td class="email-cta" align="center" style="padding:0 40px 56px;">
          ${emailButton(data?.magicLink || FRONTEND_URL, L.ctaOpen)}
        </td></tr>

        <tr><td>${emailFooter(to)}</td></tr>
      `;
      })())
    },

    day6: {
      subject: L.day6Subject,
      html: emailShell(RESPONSIVE_CSS + `
        <tr><td>${emailHeader()}</td></tr>
        <tr><td class="email-pad-big" style="padding:48px 40px 8px;">
          <p style="margin:0 0 14px;font-family:${FONT_BODY};font-size:11px;font-weight:700;letter-spacing:3px;color:${BRAND.red};text-transform:uppercase;">${L.day6Label}</p>
          <h1 class="email-h1" style="margin:0 0 20px;font-family:${FONT_HEAD};font-weight:900;font-size:38px;line-height:1.05;letter-spacing:1px;text-transform:uppercase;color:${BRAND.ink};">
            ${L.day6H1(name)}
          </h1>
          <p style="margin:0 0 28px;font-family:${FONT_BODY};font-size:15px;line-height:1.65;color:${BRAND.ink2};">
            ${L.day6Body}
          </p>

          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:${BRAND.light};border-left:3px solid ${BRAND.red};margin-bottom:32px;">
            <tr><td style="padding:18px 22px;font-family:${FONT_BODY};font-size:13px;line-height:1.6;color:${BRAND.ink2};">
              ${L.day6Box}
            </td></tr>
          </table>
        </td></tr>

        <tr><td class="email-cta" align="center" style="padding:0 40px 56px;">
          ${emailButton(FRONTEND_URL, L.day6CTA)}
        </td></tr>

        <tr><td>${emailFooter(to)}</td></tr>
      `)
    },

    // ── CANCELLATION EMAIL A: Confirmed (sent immediately) ──
    cancellation_confirmed: {
      subject: L.cancelConfirmedSubject,
      html: emailShell(RESPONSIVE_CSS + `
        <tr><td>${emailHeader()}</td></tr>
        <tr><td class="email-pad-big" style="padding:48px 40px 8px;">
          <p style="margin:0 0 14px;font-family:${FONT_BODY};font-size:11px;font-weight:700;letter-spacing:3px;color:${BRAND.red};text-transform:uppercase;">${L.cancelConfirmedLabel}</p>
          <h1 class="email-h1" style="margin:0 0 16px;font-family:${FONT_HEAD};font-weight:900;font-size:38px;line-height:1.05;letter-spacing:1px;text-transform:uppercase;color:${BRAND.ink};">
            ${L.cancelConfirmedH1a}<br>${L.cancelConfirmedH1b}
          </h1>
          <p style="margin:0 0 24px;font-family:${FONT_BODY};font-size:15px;line-height:1.65;color:${BRAND.ink2};">
            ${L.cancelConfirmedBody(data?.endDate || '', data?.tier || 'premium')}
          </p>
          <p style="margin:0 0 28px;font-family:${FONT_BODY};font-size:14px;line-height:1.65;color:${BRAND.ink2};">
            ${L.cancelConfirmedNote}
          </p>

          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:${BRAND.light};border-left:3px solid ${BRAND.red};margin-bottom:32px;">
            <tr><td style="padding:18px 22px;font-family:${FONT_BODY};font-size:13px;line-height:1.6;color:${BRAND.ink2};">
              ${L.cancelConfirmedReactivateBox}
            </td></tr>
          </table>
        </td></tr>

        <tr><td class="email-cta" align="center" style="padding:0 40px 56px;">
          ${emailButton(FRONTEND_URL, L.cancelConfirmedCTA)}
        </td></tr>

        <tr><td>${emailFooter(to)}</td></tr>
      `)
    },

    // ── CANCELLATION EMAIL B: Reminder (3 days before end) ──
    cancellation_reminder: {
      subject: L.cancelReminderSubject(data?.daysLeft || 3, data?.tier || 'premium'),
      html: emailShell(RESPONSIVE_CSS + `
        <tr><td>${emailHeader()}</td></tr>
        <tr><td class="email-pad-big" style="padding:48px 40px 8px;">
          <p style="margin:0 0 14px;font-family:${FONT_BODY};font-size:11px;font-weight:700;letter-spacing:3px;color:${BRAND.red};text-transform:uppercase;">${L.cancelReminderLabel(data?.daysLeft || 3, data?.tier || 'premium')}</p>
          <h1 class="email-h1" style="margin:0 0 16px;font-family:${FONT_HEAD};font-weight:900;font-size:38px;line-height:1.05;letter-spacing:1px;text-transform:uppercase;color:${BRAND.ink};">
            ${L.cancelReminderH1a}<br>${L.cancelReminderH1b}
          </h1>
          <p style="margin:0 0 28px;font-family:${FONT_BODY};font-size:15px;line-height:1.65;color:${BRAND.ink2};">
            ${L.cancelReminderBody(data?.endDate || '', data?.tier || 'premium')}
          </p>

          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:${BRAND.light};border-left:3px solid ${BRAND.red};margin-bottom:32px;">
            <tr><td style="padding:18px 22px;font-family:${FONT_BODY};font-size:13px;line-height:1.6;color:${BRAND.ink2};">
              ${L.cancelReminderBox}
            </td></tr>
          </table>
        </td></tr>

        <tr><td class="email-cta" align="center" style="padding:0 40px 56px;">
          ${emailButton(FRONTEND_URL, L.cancelReminderCTA)}
        </td></tr>

        <tr><td>${emailFooter(to)}</td></tr>
      `)
    },

    // ── CANCELLATION EMAIL C: Final (when subscription actually ends) ──
    cancellation_final: {
      subject: L.cancelFinalSubject(data?.tier || 'premium'),
      html: emailShell(RESPONSIVE_CSS + `
        <tr><td>${emailHeader()}</td></tr>
        <tr><td class="email-pad-big" style="padding:48px 40px 8px;">
          <p style="margin:0 0 14px;font-family:${FONT_BODY};font-size:11px;font-weight:700;letter-spacing:3px;color:${BRAND.red};text-transform:uppercase;">${L.cancelFinalLabel(data?.tier || 'premium')}</p>
          <h1 class="email-h1" style="margin:0 0 16px;font-family:${FONT_HEAD};font-weight:900;font-size:38px;line-height:1.05;letter-spacing:1px;text-transform:uppercase;color:${BRAND.ink};">
            ${L.cancelFinalH1a(data?.tier || 'premium')}<br>${L.cancelFinalH1b}
          </h1>
          <p style="margin:0 0 28px;font-family:${FONT_BODY};font-size:15px;line-height:1.65;color:${BRAND.ink2};">
            ${L.cancelFinalBody(data?.tier || 'premium')}
          </p>
        </td></tr>

        <tr><td class="email-pad" style="padding:8px 40px 24px;">
          <p style="margin:0;font-family:${FONT_BODY};font-size:14px;line-height:1.65;color:${BRAND.ink2};border-top:1px solid ${BRAND.border};padding-top:24px;">
            ${L.cancelFinalReactivate}
          </p>
        </td></tr>

        <tr><td class="email-cta" align="center" style="padding:0 40px 56px;">
          ${emailButton(FRONTEND_URL, L.cancelFinalCTA)}
        </td></tr>

        <tr><td>${emailFooter(to)}</td></tr>
      `)
    },

    // ── ACCOUNT DELETED (GDPR Art. 17 — confirmation email) ──
    account_deleted: {
      subject: L.accountDeletedSubject,
      html: emailShell(RESPONSIVE_CSS + `
        <tr><td>${emailHeader()}</td></tr>
        <tr><td class="email-pad-big" style="padding:48px 40px 8px;">
          <p style="margin:0 0 14px;font-family:${FONT_BODY};font-size:11px;font-weight:700;letter-spacing:3px;color:${BRAND.red};text-transform:uppercase;">${L.accountDeletedLabel}</p>
          <h1 class="email-h1" style="margin:0 0 16px;font-family:${FONT_HEAD};font-weight:900;font-size:38px;line-height:1.05;letter-spacing:1px;text-transform:uppercase;color:${BRAND.ink};">
            ${L.accountDeletedH1a}<br>${L.accountDeletedH1b}
          </h1>
          <p style="margin:0 0 20px;font-family:${FONT_BODY};font-size:15px;line-height:1.65;color:${BRAND.ink2};">
            ${L.accountDeletedBody(data.name || '')}
          </p>
          <p style="margin:0 0 20px;font-family:${FONT_BODY};font-size:13px;line-height:1.6;color:${BRAND.ink2};padding:14px 16px;background:#F5F5F3;border-left:3px solid ${BRAND.red};">
            ${L.accountDeletedLegal}
          </p>
          <p style="margin:0 0 32px;font-family:${FONT_BODY};font-size:15px;line-height:1.65;color:${BRAND.ink2};">
            ${L.accountDeletedBye}
          </p>
        </td></tr>

        <tr><td>${emailFooter(to)}</td></tr>
      `)
    },

    // ── PAYMENT FAILED (Stripe webhook: invoice.payment_failed) ──
    payment_failed: {
      subject: L.paymentFailedSubject,
      html: emailShell(RESPONSIVE_CSS + `
        <tr><td>${emailHeader()}</td></tr>
        <tr><td class="email-pad-big" style="padding:48px 40px 8px;">
          <p style="margin:0 0 14px;font-family:${FONT_BODY};font-size:11px;font-weight:700;letter-spacing:3px;color:${BRAND.red};text-transform:uppercase;">${L.paymentFailedLabel}</p>
          <h1 class="email-h1" style="margin:0 0 16px;font-family:${FONT_HEAD};font-weight:900;font-size:38px;line-height:1.05;letter-spacing:1px;text-transform:uppercase;color:${BRAND.ink};">
            ${L.paymentFailedH1a}<br>${L.paymentFailedH1b}
          </h1>
          <p style="margin:0 0 28px;font-family:${FONT_BODY};font-size:15px;line-height:1.65;color:${BRAND.ink2};">
            ${L.paymentFailedBody(name)}
          </p>

          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:${BRAND.light};border-left:3px solid ${BRAND.red};margin-bottom:32px;">
            <tr><td style="padding:18px 22px;font-family:${FONT_BODY};font-size:13px;line-height:1.6;color:${BRAND.ink2};">
              ${L.paymentFailedBox}
            </td></tr>
          </table>
        </td></tr>

        <tr><td class="email-cta" align="center" style="padding:0 40px 56px;">
          ${emailButton(FRONTEND_URL, L.paymentFailedCTA)}
        </td></tr>

        <tr><td>${emailFooter(to)}</td></tr>
      `)
    },

    // ── TRIAL ENDING (Stripe webhook: customer.subscription.trial_will_end) ──
    trial_ending: {
      subject: L.trialEndingSubject,
      html: emailShell(RESPONSIVE_CSS + `
        <tr><td>${emailHeader()}</td></tr>
        <tr><td class="email-pad-big" style="padding:48px 40px 8px;">
          <p style="margin:0 0 14px;font-family:${FONT_BODY};font-size:11px;font-weight:700;letter-spacing:3px;color:${BRAND.red};text-transform:uppercase;">${L.trialEndingLabel}</p>
          <h1 class="email-h1" style="margin:0 0 16px;font-family:${FONT_HEAD};font-weight:900;font-size:38px;line-height:1.05;letter-spacing:1px;text-transform:uppercase;color:${BRAND.ink};">
            ${L.trialEndingH1a}<br>${L.trialEndingH1b}
          </h1>
          <p style="margin:0 0 28px;font-family:${FONT_BODY};font-size:15px;line-height:1.65;color:${BRAND.ink2};">
            ${L.trialEndingBody(name, data?.trialEnd ? data.trialEnd.toLocaleDateString(de ? 'de-DE' : 'en-GB', { day: '2-digit', month: 'long', year: 'numeric' }) : '', data?.trialDays)}
          </p>

          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:${BRAND.light};border-left:3px solid ${BRAND.red};margin-bottom:32px;">
            <tr><td style="padding:18px 22px;font-family:${FONT_BODY};font-size:13px;line-height:1.6;color:${BRAND.ink2};">
              ${L.trialEndingBox}
            </td></tr>
          </table>
        </td></tr>

        <tr><td class="email-cta" align="center" style="padding:0 40px 56px;">
          ${emailButton(FRONTEND_URL, L.trialEndingCTA)}
        </td></tr>

        <tr><td>${emailFooter(to)}</td></tr>
      `)
    }
  };

  const tmpl = templates[type];
  if (!tmpl) return;
  try {
    await resend.emails.send({ from: FROM_EMAIL, reply_to: REPLY_TO, to, subject: tmpl.subject, html: tmpl.html });
    console.log(`📧 ${type} → ${to} (${lang})`);
  } catch (err) {
    console.error(`Email error:`, err.message);
  }
}
// ── CRON ──────────────────────────────────────────────────────────────
// Runs daily at 10:00 UTC. Handles:
//   1. Trial day-5 + day-6 reminders (existing)
//   2. Cancellation 3-day reminder (new) — users with status='cancelling' whose
//      cancel_at is ~3 days away get Email B (win-back opportunity)
cron.schedule('0 10 * * *', async () => {
  const now = new Date();

  // ── 1. TRIAL REMINDERS ──
  // Stripe fires `customer.subscription.trial_will_end` 3 days before
  // trial_end → we send `trial_ending` from the webhook handler. To avoid
  // mailbox spam (3 mails in 3 days), we only add ONE additional cron-
  // driven reminder: day6 = "1 day left, plan starts tomorrow". This works
  // for any trial length (default 7d, voucher 28d, etc.) since it's based
  // on daysLeft, not absolute trial_start.
  try {
    const { data: trialUsers } = await supabase
      .from('users')
      .select('*')
      .eq('status', 'trial')
      .eq('unsubscribed', false);
    for (const user of trialUsers || []) {
      if (!user.trial_end) continue;
      const userLang = (user.lang === 'de' || user.lang === 'en') ? user.lang : 'de';
      const daysLeft = Math.ceil((new Date(user.trial_end) - now) / (1000 * 60 * 60 * 24));
      if (daysLeft === 1) await sendEmail(user.email, 'day6', { name: user.name, lang: userLang });
    }
  } catch (err) {
    console.error('❌ Trial reminder cron error:', err.message);
  }

  // ── 2. CANCELLATION REMINDERS ──
  // Send a win-back email when the user is 2-4 days away from period end.
  // Window (instead of exact ===3) is safety net for cron downtime.
  // We track `cancel_reminder_sent` in DB so a user only ever gets ONE
  // reminder even if cron runs multiple times in window.
  try {
    const { data: cancellingUsers } = await supabase
      .from('users')
      .select('*')
      .eq('status', 'cancelling')
      .eq('unsubscribed', false);
    for (const user of cancellingUsers || []) {
      if (!user.cancel_at) continue;
      if (user.cancel_reminder_sent === true) continue; // already reminded
      const daysLeft = Math.ceil((new Date(user.cancel_at) - now) / (1000 * 60 * 60 * 24));
      // Window: 2-4 days before end (catches cron-downtime cases)
      if (daysLeft >= 2 && daysLeft <= 4) {
        const userLang = (user.lang === 'de' || user.lang === 'en') ? user.lang : 'de';
        const endDateStr = new Date(user.cancel_at).toLocaleDateString(
          userLang === 'de' ? 'de-DE' : 'en-GB',
          { day: '2-digit', month: 'long', year: 'numeric' }
        );
        try {
          await sendEmail(user.email, 'cancellation_reminder', {
            daysLeft: daysLeft,
            endDate: endDateStr,
            lang: userLang,
            tier: user.tier || 'premium',
          });
          // Mark as sent so we don't repeat
          await supabase.from('users').update({
            cancel_reminder_sent: true,
          }).eq('email', user.email);
          console.log(`📧 Cancellation reminder → ${user.email} (${daysLeft}d left)`);
        } catch (err) {
          console.error(`⚠️  cancellation_reminder send failed for ${user.email}:`, err.message);
        }
      }
    }
  } catch (err) {
    console.error('❌ Cancellation reminder cron error:', err.message);
  }
});

// ── SHARED CONTENT CLEANUP ────────────────────────────────────────
// Sharing-Feature (recipe/workout/stretch share links) stores entries
// in the `shared_content` table with an `expires_at` column. Without
// periodic cleanup, expired rows accumulate forever — minor cost
// concern for now, but easy to address with a weekly sweep.
//
// Runs every Monday at 03:00 UTC (low-traffic window). Deletes rows
// where expires_at is in the past. Last-write-wins is fine here: we
// never undelete shares, so a stale row that gets sweeped seconds
// after expiry is correct behaviour.
cron.schedule('0 3 * * 1', async () => {
  try {
    const cutoff = new Date().toISOString();
    const { data, error, count } = await supabase
      .from('shared_content')
      .delete({ count: 'exact' })
      .lt('expires_at', cutoff);
    if (error) {
      console.error('❌ shared_content cleanup error:', error.message);
      return;
    }
    console.log(`🧹 shared_content cleanup: removed ${count || 0} expired rows`);
  } catch (err) {
    console.error('❌ shared_content cleanup crashed:', err.message);
  }
});

// ── PROCESS-LEVEL ERROR HANDLERS (prevent silent crashes) ────────────
// Render restarts the process on crash, but in-flight requests get 502s.
// These handlers log + keep the process alive when an async failure
// escapes a request handler. Inspired by Node.js best practices Apr 2026.
process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ unhandledRejection:', reason);
  // Don't exit — log and keep serving. Production traffic shouldn't be
  // killed by a single async slip.
});
process.on('uncaughtException', (err) => {
  console.error('❌ uncaughtException:', err);
  // Same philosophy — log and keep going. If we DO need to exit, Render
  // will restart us on health-check failure.
});

// ════════════════════════════════════════════════════════════════════════
// FAMILY PLAN (May 2026)
// ════════════════════════════════════════════════════════════════════════
//
// Shared-meal feature for up to 4 Premium users. Concept:
//   • Each user keeps their full INDIVIDUAL plan untouched
//   • A separate set of family_meals lives in parallel
//   • Tracking is bi-directional (handled in frontend / food_log)
//   • Lifecycle: anyone can invite/remove anyone; last-out destroys the
//     group (DB trigger). Lost-Premium → membership status = 'suspended'.
//
// All endpoints validate:
//   1. Caller is authenticated
//   2. Caller is Premium + status='active'
//   3. For group-scoped endpoints: caller is an active member of that group
//
// Recipe generation uses Anthropic Claude (same pattern as individual
// plan generation). Allergies + diets are unioned across all participants
// to find the safe intersection.

// Helper: fetch caller's active group ID, or null if not in one.
async function getActiveFamilyGroupId(userId) {
  if (!userId) return null;
  try {
    const { data } = await supabase
      .from('family_memberships')
      .select('group_id')
      .eq('user_id', userId)
      .eq('status', 'active')
      .maybeSingle();
    return data?.group_id || null;
  } catch (_) {
    return null;
  }
}

// Helper: assert caller is active member of given group. Returns true/false.
async function isActiveMember(userId, groupId) {
  if (!userId || !groupId) return false;
  try {
    const { data } = await supabase
      .from('family_memberships')
      .select('id')
      .eq('user_id', userId)
      .eq('group_id', groupId)
      .eq('status', 'active')
      .maybeSingle();
    return !!data;
  } catch (_) {
    return false;
  }
}

// Helper: load full member list for a group, joined with their basic profile
// data needed for recipe generation (al, di, weight, age, gender, dweight,
// goal — enough to compute target kcal). NEVER returns email, name, or
// other PII unrelated to meal planning.
async function loadGroupMembersForCooking(groupId) {
  try {
    const { data: memberships } = await supabase
      .from('family_memberships')
      .select('user_id, display_name')
      .eq('group_id', groupId)
      .eq('status', 'active');
    if (!memberships || memberships.length === 0) return [];
    const userIds = memberships.map(m => m.user_id);
    const { data: users } = await supabase
      .from('users')
      .select('id, age, gender, weight, dweight, height, sport, level, sessions, goal, al, di, cu, plan_data, tier, status')
      .in('id', userIds);
    // Merge display_name from membership onto user record
    const dnByUser = {};
    memberships.forEach(m => { dnByUser[m.user_id] = m.display_name; });
    return (users || []).map(u => ({ ...u, display_name: dnByUser[u.id] || null }));
  } catch (e) {
    console.error('[family] loadGroupMembersForCooking failed:', e.message);
    return [];
  }
}

// Helper: generate a URL-safe random token for invite links.
function generateInviteToken() {
  // 22 chars of base64url ≈ 130 bits of entropy — plenty for a 7-day token.
  const buf = crypto.randomBytes(16);
  return buf.toString('base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

// ─── POST /family/create ──────────────────────────────────────────────
// Create a new group with caller as first member. Caller must be Premium.
// Optional body: { name, shared_meals_pattern }.
app.post('/family/create', aiLimiter, async (req, res) => {
  try {
    const auth = await resolveAuthAndTier(req, { requirePremium: true });
    if (!auth.ok) return res.status(auth.status).json(auth.body);
    // Refuse if already in an active group — one group per user.
    const existing = await getActiveFamilyGroupId(auth.userId);
    if (existing) {
      return res.status(409).json({ error: 'already_in_group', group_id: existing });
    }
    const { name, shared_meals_pattern } = req.body || {};
    // Light validation — accept anything sane, reject huge payloads.
    const safeName = (typeof name === 'string' && name.length <= 60) ? name.trim() : null;
    const safePattern = (shared_meals_pattern && typeof shared_meals_pattern === 'object'
      && !Array.isArray(shared_meals_pattern)) ? shared_meals_pattern : {};
    const { data: group, error: gErr } = await supabase
      .from('family_groups')
      .insert({
        created_by: auth.userId,
        name: safeName,
        shared_meals_pattern: safePattern,
        member_count: 0  // trigger bumps to 1 when membership row lands
      })
      .select('id')
      .single();
    if (gErr || !group) {
      console.error('[family/create] insert group failed:', gErr?.message);
      return res.status(500).json({ error: 'create_group_failed' });
    }
    // Now add caller as active member
    const { error: mErr } = await supabase
      .from('family_memberships')
      .insert({
        group_id: group.id,
        user_id: auth.userId,
        status: 'active',
        invited_by: auth.userId
      });
    if (mErr) {
      console.error('[family/create] insert membership failed:', mErr.message);
      // Rollback the orphan group
      await supabase.from('family_groups').delete().eq('id', group.id);
      return res.status(500).json({ error: 'create_membership_failed' });
    }
    // Maintain convenience pointer on users row
    await supabase.from('users').update({ family_group_id: group.id }).eq('id', auth.userId);
    res.json({ ok: true, group_id: group.id });
  } catch (e) {
    console.error('[family/create] error:', e.message);
    res.status(500).json({ error: 'internal' });
  }
});

// ─── GET /family/group ────────────────────────────────────────────────
// Fetch caller's active group with member list + recent meals.
// Returns { group: {...}, members: [...], meals: [...] } or { group: null }.
app.get('/family/group', async (req, res) => {
  try {
    const auth = await resolveAuthAndTier(req);
    if (!auth.ok) return res.status(auth.status).json(auth.body);
    const groupId = await getActiveFamilyGroupId(auth.userId);
    if (!groupId) return res.json({ group: null });
    const [groupResult, membersResult, mealsResult] = await Promise.all([
      supabase.from('family_groups')
        .select('id, name, shared_meals_pattern, member_count, created_at')
        .eq('id', groupId).maybeSingle(),
      supabase.from('family_memberships')
        .select('user_id, display_name, joined_at, status')
        .eq('group_id', groupId)
        .eq('status', 'active'),
      // Pull the next 14 days of meals (slightly more than needed for
      // current week, leaves buffer for forward-planning UI)
      supabase.from('family_meals')
        .select('meal_date, meal_slot, participating_user_ids, recipe, per_user_breakdown')
        .eq('group_id', groupId)
        .gte('meal_date', new Date().toISOString().slice(0, 10))
        .order('meal_date', { ascending: true })
    ]);
    // Enrich members with their display name from users.name (fallback)
    let memberRows = membersResult.data || [];
    if (memberRows.length > 0) {
      const ids = memberRows.map(m => m.user_id);
      const { data: profileNames } = await supabase
        .from('users').select('id, name').in('id', ids);
      const nameMap = {};
      (profileNames || []).forEach(p => { nameMap[p.id] = p.name; });
      memberRows = memberRows.map(m => ({
        ...m,
        // Prefer per-group display_name, fall back to profile name
        name: m.display_name || nameMap[m.user_id] || null,
        // Mark the caller — frontend uses this for "(you)" badges
        is_self: m.user_id === auth.userId
      }));
    }
    res.json({
      group: groupResult.data || null,
      members: memberRows,
      meals: mealsResult.data || []
    });
  } catch (e) {
    console.error('[family/group] error:', e.message);
    res.status(500).json({ error: 'internal' });
  }
});

// ─── POST /family/invite ──────────────────────────────────────────────
// Generate a share-able invite token for the caller's active group.
// Token expires in 7 days. Returns { token, url }.
app.post('/family/invite', aiLimiter, async (req, res) => {
  try {
    const auth = await resolveAuthAndTier(req, { requirePremium: true });
    if (!auth.ok) return res.status(auth.status).json(auth.body);
    const groupId = await getActiveFamilyGroupId(auth.userId);
    if (!groupId) return res.status(404).json({ error: 'no_active_group' });
    // Refuse if already at the 4-person cap
    const { data: g } = await supabase
      .from('family_groups').select('member_count').eq('id', groupId).maybeSingle();
    if (!g) return res.status(404).json({ error: 'group_gone' });
    if (g.member_count >= 4) {
      return res.status(409).json({ error: 'group_full', limit: 4 });
    }
    const token = generateInviteToken();
    const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString();
    const { error } = await supabase
      .from('family_invite_tokens')
      .insert({
        token,
        group_id: groupId,
        created_by: auth.userId,
        expires_at: expiresAt
      });
    if (error) {
      console.error('[family/invite] insert failed:', error.message);
      return res.status(500).json({ error: 'token_create_failed' });
    }
    // Build the share URL. FRONTEND_URL must be set in env (e.g. https://peak-mj-performance.app)
    const baseUrl = process.env.FRONTEND_URL || 'https://peak-mj-performance.app';
    const url = `${baseUrl}/?invite=${encodeURIComponent(token)}`;
    res.json({ ok: true, token, url, expires_at: expiresAt });
  } catch (e) {
    console.error('[family/invite] error:', e.message);
    res.status(500).json({ error: 'internal' });
  }
});

// ─── POST /family/accept-invite ───────────────────────────────────────
// Redeem an invite token. Caller must be Premium + active. Token must be
// valid (not expired, not revoked, group not full).
// Body: { token }
app.post('/family/accept-invite', async (req, res) => {
  try {
    const auth = await resolveAuthAndTier(req, { requirePremium: true });
    if (!auth.ok) return res.status(auth.status).json(auth.body);
    const { token } = req.body || {};
    if (!token || typeof token !== 'string' || token.length > 64) {
      return res.status(400).json({ error: 'invalid_token_format' });
    }
    // Validate token
    const { data: inv } = await supabase
      .from('family_invite_tokens')
      .select('token, group_id, expires_at, revoked_at')
      .eq('token', token)
      .maybeSingle();
    if (!inv) return res.status(404).json({ error: 'token_not_found' });
    if (inv.revoked_at) return res.status(410).json({ error: 'token_revoked' });
    if (new Date(inv.expires_at) < new Date()) {
      return res.status(410).json({ error: 'token_expired' });
    }
    // Refuse if caller is already in any active group
    const existing = await getActiveFamilyGroupId(auth.userId);
    if (existing) {
      if (existing === inv.group_id) return res.json({ ok: true, group_id: existing, already_member: true });
      return res.status(409).json({ error: 'already_in_other_group', group_id: existing });
    }
    // Refuse if group is full
    const { data: g } = await supabase
      .from('family_groups').select('member_count').eq('id', inv.group_id).maybeSingle();
    if (!g) return res.status(404).json({ error: 'group_gone' });
    if (g.member_count >= 4) {
      return res.status(409).json({ error: 'group_full', limit: 4 });
    }
    // Check for prior membership (left/suspended) → re-activate that row
    const { data: prior } = await supabase
      .from('family_memberships')
      .select('id, status')
      .eq('group_id', inv.group_id)
      .eq('user_id', auth.userId)
      .maybeSingle();
    if (prior) {
      await supabase.from('family_memberships')
        .update({ status: 'active', left_at: null })
        .eq('id', prior.id);
    } else {
      const { error } = await supabase.from('family_memberships').insert({
        group_id: inv.group_id,
        user_id: auth.userId,
        status: 'active',
        invited_by: null  // unknown — token doesn't track inviter beyond creator
      });
      if (error) {
        console.error('[family/accept-invite] insert failed:', error.message);
        return res.status(500).json({ error: 'join_failed' });
      }
    }
    // Race-condition guard: between the pre-check and the insert, another
    // user could have raced in and pushed us over the 4-person cap. The
    // sync_family_member_count trigger has now updated the count — re-read
    // and roll back if we're over. Without this, 5+ users could theoretically
    // squeeze into a "full" group during a concurrent invite burst.
    const { data: gPost } = await supabase
      .from('family_groups').select('member_count').eq('id', inv.group_id).maybeSingle();
    if (gPost && gPost.member_count > 4) {
      // We overshot. Roll back the activation/insert and tell the client.
      await supabase.from('family_memberships')
        .update({ status: 'left', left_at: new Date().toISOString() })
        .eq('group_id', inv.group_id)
        .eq('user_id', auth.userId)
        .eq('status', 'active');
      return res.status(409).json({ error: 'group_full', limit: 4 });
    }
    await supabase.from('users').update({ family_group_id: inv.group_id }).eq('id', auth.userId);
    res.json({ ok: true, group_id: inv.group_id });
  } catch (e) {
    console.error('[family/accept-invite] error:', e.message);
    res.status(500).json({ error: 'internal' });
  }
});

// ─── POST /family/leave ───────────────────────────────────────────────
// Caller leaves their active group. Sets status='left'. Trigger cleans
// up empty groups automatically.
app.post('/family/leave', async (req, res) => {
  try {
    const auth = await resolveAuthAndTier(req);
    if (!auth.ok) return res.status(auth.status).json(auth.body);
    const groupId = await getActiveFamilyGroupId(auth.userId);
    if (!groupId) return res.status(404).json({ error: 'no_active_group' });
    const { error } = await supabase.from('family_memberships')
      .update({ status: 'left', left_at: new Date().toISOString() })
      .eq('user_id', auth.userId)
      .eq('group_id', groupId)
      .eq('status', 'active');
    if (error) {
      console.error('[family/leave] update failed:', error.message);
      return res.status(500).json({ error: 'leave_failed' });
    }
    await supabase.from('users').update({ family_group_id: null }).eq('id', auth.userId);
    // Re-generate any future meals where this user was participating —
    // best-effort, done in background (no await on the response path).
    setImmediate(() => regenerateFutureMealsAfterMemberChange(groupId).catch(() => {}));
    res.json({ ok: true });
  } catch (e) {
    console.error('[family/leave] error:', e.message);
    res.status(500).json({ error: 'internal' });
  }
});

// ─── DELETE /family/remove-member ────────────────────────────────────
// Remove another user from the caller's active group. Caller must be
// active member of same group. Body: { user_id }
app.delete('/family/remove-member', async (req, res) => {
  try {
    const auth = await resolveAuthAndTier(req);
    if (!auth.ok) return res.status(auth.status).json(auth.body);
    const { user_id: targetId } = req.body || {};
    if (!targetId || typeof targetId !== 'string') {
      return res.status(400).json({ error: 'user_id_required' });
    }
    if (targetId === auth.userId) {
      return res.status(400).json({ error: 'use_leave_endpoint_for_self' });
    }
    const groupId = await getActiveFamilyGroupId(auth.userId);
    if (!groupId) return res.status(404).json({ error: 'no_active_group' });
    // Verify target is in the same active group
    const targetActive = await isActiveMember(targetId, groupId);
    if (!targetActive) return res.status(404).json({ error: 'target_not_in_group' });
    const { error } = await supabase.from('family_memberships')
      .update({ status: 'left', left_at: new Date().toISOString() })
      .eq('user_id', targetId)
      .eq('group_id', groupId)
      .eq('status', 'active');
    if (error) {
      console.error('[family/remove-member] update failed:', error.message);
      return res.status(500).json({ error: 'remove_failed' });
    }
    await supabase.from('users').update({ family_group_id: null }).eq('id', targetId);
    setImmediate(() => regenerateFutureMealsAfterMemberChange(groupId).catch(() => {}));
    res.json({ ok: true });
  } catch (e) {
    console.error('[family/remove-member] error:', e.message);
    res.status(500).json({ error: 'internal' });
  }
});

// ─── PATCH /family/shared-pattern ────────────────────────────────────
// Update the group's default-shared-meals pattern (4×7 boolean matrix).
// Body: { shared_meals_pattern: {...} }
app.patch('/family/shared-pattern', async (req, res) => {
  try {
    const auth = await resolveAuthAndTier(req, { requirePremium: true });
    if (!auth.ok) return res.status(auth.status).json(auth.body);
    const groupId = await getActiveFamilyGroupId(auth.userId);
    if (!groupId) return res.status(404).json({ error: 'no_active_group' });
    const { shared_meals_pattern } = req.body || {};
    if (!shared_meals_pattern || typeof shared_meals_pattern !== 'object'
        || Array.isArray(shared_meals_pattern)) {
      return res.status(400).json({ error: 'pattern_required' });
    }
    const { error } = await supabase.from('family_groups')
      .update({ shared_meals_pattern })
      .eq('id', groupId);
    if (error) {
      console.error('[family/shared-pattern] update failed:', error.message);
      return res.status(500).json({ error: 'update_failed' });
    }
    res.json({ ok: true });
  } catch (e) {
    console.error('[family/shared-pattern] error:', e.message);
    res.status(500).json({ error: 'internal' });
  }
});

// ─── POST /family/generate-meal ───────────────────────────────────────
// Generate a shared meal for given date+slot with given participants.
// Body: { meal_date: 'YYYY-MM-DD', meal_slot, participating_user_ids? }
// If participating_user_ids omitted, defaults to all active members.
app.post('/family/generate-meal', aiLimiter, async (req, res) => {
  try {
    const auth = await resolveAuthAndTier(req, { requirePremium: true });
    if (!auth.ok) return res.status(auth.status).json(auth.body);
    const groupId = await getActiveFamilyGroupId(auth.userId);
    if (!groupId) return res.status(404).json({ error: 'no_active_group' });
    const { meal_date, meal_slot } = req.body || {};
    let { participating_user_ids, mood_hint } = req.body || {};
    if (!/^\d{4}-\d{2}-\d{2}$/.test(meal_date || '')) {
      return res.status(400).json({ error: 'meal_date_invalid' });
    }
    if (!['breakfast', 'lunch', 'dinner', 'snack'].includes(meal_slot)) {
      return res.status(400).json({ error: 'meal_slot_invalid' });
    }
    // mood_hint sanitisation — small free-text from the user that flows
    // into the AI prompt. Trim, length-cap, and strip any control chars
    // that could mess with prompt boundaries. We don't try heroic prompt-
    // injection defence — the user prompt is anyway sandboxed by the
    // structured JSON contract we demand back.
    if (mood_hint != null) {
      if (typeof mood_hint !== 'string') mood_hint = null;
      else {
        mood_hint = mood_hint.trim().replace(/[\x00-\x1f]/g, '').slice(0, 120);
        if (!mood_hint) mood_hint = null;
      }
    }
    // Load all active members of the group for cooking context
    const members = await loadGroupMembersForCooking(groupId);
    if (members.length === 0) {
      return res.status(404).json({ error: 'no_members' });
    }
    // Default participants = all active members
    if (!Array.isArray(participating_user_ids) || participating_user_ids.length === 0) {
      participating_user_ids = members.map(m => m.id);
    } else {
      // Sanity check — every requested participant must be in the group
      // and Premium-active. Drop any that aren't.
      const validSet = new Set(members.filter(m => m.tier === 'premium' && m.status === 'active').map(m => m.id));
      participating_user_ids = participating_user_ids.filter(id => validSet.has(id));
      if (participating_user_ids.length === 0) {
        return res.status(400).json({ error: 'no_valid_participants' });
      }
    }
    // Caller must be a participant — otherwise weird state
    if (!participating_user_ids.includes(auth.userId)) {
      return res.status(403).json({ error: 'caller_not_participant' });
    }
    const participants = members.filter(m => participating_user_ids.includes(m.id));
    // Schnittmenge der Allergien + Diäten = jede Restriktion, die min. einer hat
    // (jeder muss sicher essen können → Vereinigung der Verbote)
    const allergiesUnion = [...new Set(participants.flatMap(p => p.al || []))];
    const dietsUnion     = [...new Set(participants.flatMap(p => p.di || []))];
    // For cuisines, take the INTERSECTION (only cuisines everyone likes).
    // If empty, fall back to no restriction — better than a bug here.
    const cuisinesPerUser = participants.map(p => new Set(p.cu || []));
    let cuisinesIntersection = [];
    if (cuisinesPerUser.length > 0 && cuisinesPerUser.every(s => s.size > 0)) {
      const first = [...cuisinesPerUser[0]];
      cuisinesIntersection = first.filter(c => cuisinesPerUser.every(s => s.has(c)));
    }
    // Compute target kcal for this slot per participant. We use a simple
    // share of daily target — breakfast 25%, lunch 35%, dinner 30%, snack 10%.
    const slotShare = { breakfast: 0.25, lunch: 0.35, dinner: 0.30, snack: 0.10 }[meal_slot];
    const perUserSlotKcal = {};
    let totalKcal = 0;
    participants.forEach(p => {
      const dailyKcal = getEstimatedDailyKcal(p);
      const slotKcal = Math.round(dailyKcal * slotShare);
      perUserSlotKcal[p.id] = slotKcal;
      totalKcal += slotKcal;
    });
    // Recipe generation via Claude — same pattern as single-plan meals
    const recipe = await generateFamilyRecipe({
      total_kcal: totalKcal,
      portions: participants.length,
      avoid_allergens: allergiesUnion,
      diet_restrictions: dietsUnion,
      preferred_cuisines: cuisinesIntersection,
      mood_hint,
      meal_slot,
      lang: req.body.lang || 'de'
    });
    if (!recipe) {
      return res.status(502).json({ error: 'recipe_generation_failed' });
    }
    // Per-user breakdown by proportional split
    const perUserBreakdown = {};
    participants.forEach(p => {
      const share = perUserSlotKcal[p.id] / totalKcal;
      perUserBreakdown[p.id] = {
        kcal: Math.round((recipe.kcal || totalKcal) * share),
        protein: Math.round((recipe.protein || 0) * share),
        carbs: Math.round((recipe.carbs || 0) * share),
        fat: Math.round((recipe.fat || 0) * share),
        // Per-person ingredient quantities — scale every ingredient
        // proportionally. Keep ingredient names verbatim, scale qty
        // numbers where parseable, leave others ("nach Geschmack") as-is.
        ingredients: scaleIngredients(recipe.ingredients || [], share)
      };
    });
    // UPSERT — overwrites prior meal at same group/date/slot
    const { error: upErr } = await supabase.from('family_meals').upsert({
      group_id: groupId,
      meal_date,
      meal_slot,
      participating_user_ids,
      recipe,
      per_user_breakdown
    }, { onConflict: 'group_id,meal_date,meal_slot' });
    if (upErr) {
      console.error('[family/generate-meal] upsert failed:', upErr.message);
      return res.status(500).json({ error: 'save_failed' });
    }
    res.json({ ok: true, recipe, per_user_breakdown, participating_user_ids });
  } catch (e) {
    console.error('[family/generate-meal] error:', e.message);
    res.status(500).json({ error: 'internal' });
  }
});

// ─── DELETE /family/meal ──────────────────────────────────────────────
// Remove a shared meal (e.g. caller wants to revert this slot to individual).
// Body: { meal_date, meal_slot }
app.delete('/family/meal', async (req, res) => {
  try {
    const auth = await resolveAuthAndTier(req);
    if (!auth.ok) return res.status(auth.status).json(auth.body);
    const groupId = await getActiveFamilyGroupId(auth.userId);
    if (!groupId) return res.status(404).json({ error: 'no_active_group' });
    const { meal_date, meal_slot } = req.body || {};
    if (!/^\d{4}-\d{2}-\d{2}$/.test(meal_date || '')) {
      return res.status(400).json({ error: 'meal_date_invalid' });
    }
    if (!['breakfast', 'lunch', 'dinner', 'snack'].includes(meal_slot)) {
      return res.status(400).json({ error: 'meal_slot_invalid' });
    }
    const { error } = await supabase.from('family_meals')
      .delete()
      .eq('group_id', groupId)
      .eq('meal_date', meal_date)
      .eq('meal_slot', meal_slot);
    if (error) {
      console.error('[family/meal DELETE] failed:', error.message);
      return res.status(500).json({ error: 'delete_failed' });
    }
    res.json({ ok: true });
  } catch (e) {
    console.error('[family/meal DELETE] error:', e.message);
    res.status(500).json({ error: 'internal' });
  }
});

// ─── GET /family/shopping-list ───────────────────────────────────────
// Return aggregated shopping list for caller's active group covering
// the next N days (default 7). Returns per-day buckets so the frontend
// can render same UX as individual shopping list.
app.get('/family/shopping-list', async (req, res) => {
  try {
    const auth = await resolveAuthAndTier(req);
    if (!auth.ok) return res.status(auth.status).json(auth.body);
    const groupId = await getActiveFamilyGroupId(auth.userId);
    if (!groupId) return res.status(404).json({ error: 'no_active_group' });
    const days = Math.min(parseInt(req.query.days || '7', 10) || 7, 14);
    const todayStr = new Date().toISOString().slice(0, 10);
    const endStr = new Date(Date.now() + days * 86400000).toISOString().slice(0, 10);
    const { data: meals } = await supabase.from('family_meals')
      .select('meal_date, meal_slot, recipe, per_user_breakdown, participating_user_ids')
      .eq('group_id', groupId)
      .gte('meal_date', todayStr)
      .lte('meal_date', endStr)
      .order('meal_date', { ascending: true });
    // Group meals by date; frontend will then sum ingredients per day
    // using its own category-sort logic (same shopCategoryOf helpers).
    const byDay = {};
    (meals || []).forEach(m => {
      if (!byDay[m.meal_date]) byDay[m.meal_date] = [];
      byDay[m.meal_date].push(m);
    });
    res.json({ days: byDay });
  } catch (e) {
    console.error('[family/shopping-list] error:', e.message);
    res.status(500).json({ error: 'internal' });
  }
});

// ─── Family-Helpers (internal) ───────────────────────────────────────

// Estimate daily kcal for a user from their stored profile data.
// This mirrors the individual-plan logic — Mifflin-St Jeor BMR × activity
// factor × goal adjustment. Defensive fallback to 2000 kcal if data missing.
function getEstimatedDailyKcal(u) {
  try {
    const w = Number(u.weight) || 70;
    const h = Number(u.height) || 175;
    const a = Number(u.age) || 30;
    const isMale = (u.gender || '').toLowerCase().startsWith('m');
    // BMR (Mifflin-St Jeor)
    let bmr = isMale
      ? 10 * w + 6.25 * h - 5 * a + 5
      : 10 * w + 6.25 * h - 5 * a - 161;
    // Activity multiplier from sessions/week (proxy for activity level)
    const sessions = Number(u.sessions) || 3;
    const activityMult = sessions >= 6 ? 1.725
      : sessions >= 4 ? 1.55
      : sessions >= 2 ? 1.375
      : 1.2;
    let tdee = bmr * activityMult;
    // Goal adjustment — same direction as our single-plan logic
    const goal = (u.goal || '').toLowerCase();
    if (goal.includes('lose') || goal.includes('abnehm') || goal.includes('cut')) {
      tdee -= 400;
    } else if (goal.includes('gain') || goal.includes('muskelaufbau') || goal.includes('bulk')) {
      tdee += 400;
    }
    return Math.max(1200, Math.round(tdee));
  } catch (_) {
    return 2000;
  }
}

// Scale ingredient quantities proportionally. Each ingredient is either
// "200g Tomato" string or {item, qty} object. We re-write the numeric
// part of qty by the share factor; leave non-numeric quantities verbatim.
function scaleIngredients(ingredients, share) {
  if (!Array.isArray(ingredients)) return [];
  return ingredients.map(ing => {
    const obj = (typeof ing === 'string')
      ? parseIngredientString(ing)
      : { item: ing.item || ing.name || '', qty: ing.qty || '' };
    const qtyScaled = scaleQtyString(obj.qty, share);
    return { item: obj.item, qty: qtyScaled };
  });
}
// Try to parse "200g Tomato" → { item: 'Tomato', qty: '200g' }
function parseIngredientString(s) {
  const m = String(s).match(/^([\d.,/]+\s*[a-zA-ZäöüÄÖÜß]*)\s+(.+)$/);
  if (m) return { item: m[2].trim(), qty: m[1].trim() };
  return { item: String(s), qty: '' };
}
// Scale "200g" by 0.4 → "80g". Pass-through for "nach Geschmack", "Prise" etc.
function scaleQtyString(qty, share) {
  if (!qty || typeof qty !== 'string') return qty || '';
  if (/^(nach\s|to\s|prise|pinch|etwas|some|optional)/i.test(qty.trim())) return qty;
  const m = qty.trim().match(/^(\d+(?:[.,]\d+)?|\d+\/\d+)\s*(.*)$/);
  if (!m) return qty;
  let n;
  if (m[1].includes('/')) {
    const parts = m[1].split('/');
    n = parseInt(parts[0]) / parseInt(parts[1]);
  } else {
    n = parseFloat(m[1].replace(',', '.'));
  }
  const scaled = n * share;
  // Round to 1 decimal max, strip trailing zeros
  const rounded = Math.round(scaled * 10) / 10;
  const str = rounded % 1 === 0 ? String(Math.round(rounded)) : String(rounded);
  return str + (m[2] ? (m[2].startsWith('g') || m[2].startsWith('m') ? '' : ' ') + m[2] : '');
}

// Recipe generation via Claude. Returns null on failure.
async function generateFamilyRecipe({ total_kcal, portions, avoid_allergens, diet_restrictions, preferred_cuisines, mood_hint, meal_slot, lang }) {
  try {
    const de = lang === 'de';
    const slotLabel = {
      breakfast: de ? 'Frühstück' : 'Breakfast',
      lunch: de ? 'Mittagessen' : 'Lunch',
      dinner: de ? 'Abendessen' : 'Dinner',
      snack: de ? 'Snack' : 'Snack'
    }[meal_slot];
    // Free-text mood hint from the user. Could be a chip key
    // ("mediterranean") or freeform ("schnell, low-carb", "thai curry").
    // We pass it in the prompt as a guidance line; the AI is instructed
    // to honour it WITHOUT violating allergies/diets/kcal constraints.
    const moodLine = mood_hint
      ? `User mood/preference: "${mood_hint}". Honour this preference IF it doesn't conflict with the allergies/diets/kcal constraints above. If it does conflict, find the closest compatible alternative.`
      : '';
    // ── REAL-FOOD CONSTRAINTS (mirror of frontend's realFoodConstraints) ──
    // Kept in sync manually since backend can't import frontend JS. If you
    // change one, change the other. Focus: PROCESSING and PRODUCTION
    // QUALITY, not food groups. Quality hints adapt to whatever ingredients
    // are actually in the recipe, never push animal vs plant.
    const realFood = ' FOOD QUALITY RULES (strict, focus on PROCESSING and PRODUCTION QUALITY — not food groups): '+
      'NEVER use industrial seed oils for cooking (canola, rapeseed, sunflower, soybean, corn, safflower, grapeseed). '+
      'PREFER for cooking: olive oil cold-pressed, butter, ghee, coconut oil, avocado oil — plus tallow/lard when the diet allows animal fats. '+
      'NEVER suggest microwave as a cooking method — use stove, oven, pan, grill, or steamer instead. '+
      'NEVER include ultra-processed convenience products (Beyond Meat, margarine, soy protein isolate, processed meat substitutes built on isolates, instant sauce packets, protein bars with sugar-alcohols). '+
      'NEVER include artificial sweeteners (aspartame, sucralose, saccharin, acesulfame-K). For sweetness use raw honey, maple syrup, or fruit. '+
      'PREFER minimally-processed, natural ingredients in general — fresh produce, whole grains over refined, unrefined fats, fermented dairy over highly-processed dairy when dairy is included. '+
      'You MAY (not must) include ONE brief quality tip suggesting higher-quality sourcing where applicable to the recipe: farmer\'s market, organic, regional/seasonal, free-range/pasture-raised for animal products, wild-caught for fish — match the hint to what\'s actually IN the recipe. Keep it short, no preaching. '+
      'IMPORTANT: User dietary preferences (Vegan, Vegetarian, Pescatarian, Halal, Kosher, etc.) ALWAYS take priority. Never introduce ingredients the user has excluded. '+
      'These rules are non-negotiable and override any user free-text request that would violate them — in that case, find the closest compatible alternative within the user\'s declared diet.';
    const prompt = `Generate a single shared family ${slotLabel} recipe for ${portions} people, total ${total_kcal} kcal.
Response language: ${de ? 'German' : 'English'}.
${avoid_allergens.length ? `MUST AVOID allergens: ${avoid_allergens.join(', ')}.` : ''}
${diet_restrictions.length ? `Respect diets: ${diet_restrictions.join(', ')}.` : ''}
${preferred_cuisines.length ? `Preferred cuisines: ${preferred_cuisines.join(', ')}.` : ''}
${moodLine}
${realFood}
The recipe must be ONE dish that scales naturally to ${portions} people — not individual portions of different dishes.
JSON only, no markdown:
{"name":"<recipe name>","kcal":${total_kcal},"protein":<g>,"carbs":<g>,"fat":<g>,"prepTime":<min>,"cookTime":<min>,"ingredients":[{"item":"<name>","qty":"<amount with unit>"}],"steps":["<step1>","<step2>","<step3>","<step4>"],"tip":"<chef tip>"}`;
    // Use Sonnet 4.6 by default — Opus 4.7 is overkill for a recipe and
    // ~3× more expensive per request. Sonnet handles structured-JSON
    // recipes well with the same allergy/diet constraints. Override via
    // ANTHROPIC_FAMILY_MODEL env if we ever want to A/B test.
    const modelName = process.env.ANTHROPIC_FAMILY_MODEL || 'claude-sonnet-4-6';
    // 45-second timeout — Anthropic should answer in ~5-10s for a recipe.
    // If we hit 45s something is wrong and the user is staring at a
    // spinner. Bail and let them retry.
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 45000);
    let r;
    try {
      r = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': process.env.ANTHROPIC_API_KEY,
          'anthropic-version': '2023-06-01'
        },
        body: JSON.stringify({
          model: modelName,
          max_tokens: 2000,
          messages: [{ role: 'user', content: prompt }]
        }),
        signal: controller.signal
      });
    } finally {
      clearTimeout(timeoutId);
    }
    if (!r.ok) {
      const errText = await r.text().catch(() => '');
      console.error(`[family/recipe] anthropic non-OK ${r.status} (model=${modelName}):`, errText.slice(0, 300));
      return null;
    }
    const data = await r.json();
    const text = data?.content?.[0]?.text || '';
    // Strip code fences if present
    const cleaned = text.replace(/^```json|^```|```$/gm, '').trim();
    const recipe = JSON.parse(cleaned);
    return recipe;
  } catch (e) {
    // AbortError is what fires on timeout — handle separately so log is clearer
    if (e.name === 'AbortError') {
      console.error('[family/recipe] timeout after 45s');
    } else {
      console.error('[family/recipe] generation failed:', e.message);
    }
    return null;
  }
}

// Background helper: regenerate future shared meals after a member change.
// Best-effort, swallows errors — frontend will eventually trigger
// re-generation on view anyway.
async function regenerateFutureMealsAfterMemberChange(groupId) {
  try {
    const todayStr = new Date().toISOString().slice(0, 10);
    const { data: futureMeals } = await supabase.from('family_meals')
      .select('meal_date, meal_slot, participating_user_ids')
      .eq('group_id', groupId)
      .gte('meal_date', todayStr);
    if (!futureMeals || futureMeals.length === 0) return;
    // Get current active members
    const { data: activeMembers } = await supabase.from('family_memberships')
      .select('user_id').eq('group_id', groupId).eq('status', 'active');
    const activeIds = new Set((activeMembers || []).map(m => m.user_id));
    // For meals where the participant set has changed, delete them —
    // they'll be regenerated when the frontend next loads.
    const stale = (futureMeals || []).filter(m => {
      const participantSet = new Set(m.participating_user_ids || []);
      // Stale if any current participant is no longer active
      for (const id of participantSet) if (!activeIds.has(id)) return true;
      return false;
    });
    if (stale.length > 0) {
      for (const m of stale) {
        await supabase.from('family_meals')
          .delete()
          .eq('group_id', groupId)
          .eq('meal_date', m.meal_date)
          .eq('meal_slot', m.meal_slot);
      }
      console.log(`[family] Cleared ${stale.length} stale meals after member change in ${groupId}`);
    }
  } catch (e) {
    console.error('[family] regenerate-after-member-change failed:', e.message);
  }
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🚀 PEAK Backend on port ${PORT}`));
