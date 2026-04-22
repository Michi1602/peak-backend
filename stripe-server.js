const express = require('express');
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);
const { createClient } = require('@supabase/supabase-js');
const { Resend } = require('resend');
const cron = require('node-cron');
const cors = require('cors');

const app = express();
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const resend = new Resend(process.env.RESEND_API_KEY);

const FRONTEND_URL = process.env.FRONTEND_URL || 'https://peak-mj-performance.app';
const BACKEND_URL = process.env.BACKEND_URL || 'https://peak-backend-u52q.onrender.com';
const FROM_EMAIL = 'PEAK <support@mj-performance.net>';

const COMPANY = {
  name: 'MJ Performance',
  address: 'Am Hasel 6, 85139 Wettstetten',
  email: 'support@mj-performance.net',
  website: 'https://peak-mj-performance.app',
  owner: 'Michael Jahn',
};

app.use(cors());
app.use('/webhook', express.raw({ type: 'application/json' }));
app.use(express.json());

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
const legalStyle = `body{font-family:sans-serif;max-width:680px;margin:0 auto;padding:40px 20px;color:#1C1C1A;line-height:1.7}h1{font-size:28px;margin-bottom:8px}h2{font-size:18px;margin-top:32px;margin-bottom:8px}p{margin-bottom:12px}a{color:#2D6A4F}.back{display:inline-block;background:#2D6A4F;color:#fff;padding:10px 20px;border-radius:8px;text-decoration:none;font-size:13px;margin-bottom:24px}ul{padding-left:20px;line-height:2}hr{margin:40px 0;border:none;border-top:1px solid #E8E8E3}.note{color:#999;font-size:13px}`;

app.get('/datenschutz', (req, res) => {
  res.send(`<!DOCTYPE html><html lang="de"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Datenschutzerklärung — PEAK</title><style>${legalStyle}</style></head><body>
<a href="${FRONTEND_URL}" class="back">← Zurück zur App</a>
<h1>Datenschutzerklärung</h1>
<p><strong>Stand:</strong> ${new Date().toLocaleDateString('de-DE')}</p>
<h2>1. Verantwortlicher</h2>
<p>${COMPANY.owner}<br>${COMPANY.name}<br>${COMPANY.address}<br>E-Mail: <a href="mailto:${COMPANY.email}">${COMPANY.email}</a></p>
<h2>2. Erhobene Daten</h2>
<ul><li>Name und E-Mail-Adresse (bei Registrierung)</li><li>Gesundheitliche Angaben (Gewicht, Größe, Ernährungspräferenzen — freiwillig)</li><li>Zahlungsdaten (verarbeitet durch Stripe — wir speichern keine Kartendaten)</li><li>Nutzungsdaten (Bewertungen, Protokolleinträge)</li></ul>
<h2>3. Zweck der Datenverarbeitung</h2>
<p>Wir verarbeiten deine Daten zur Erbringung des personalisierten KI-Coaching-Dienstes, zur Abwicklung von Zahlungen sowie zur Kommunikation (Transaktions-E-Mails, Erinnerungen).</p>
<h2>4. Rechtsgrundlage</h2>
<p>Die Verarbeitung erfolgt auf Basis von Art. 6 Abs. 1 lit. b DSGVO (Vertragserfüllung) und Art. 6 Abs. 1 lit. a DSGVO (Einwilligung).</p>
<h2>5. Drittanbieter</h2>
<ul><li><strong>Stripe</strong> (Zahlungsabwicklung) — stripe.com/de/privacy</li><li><strong>Supabase</strong> (Datenbank) — supabase.com/privacy</li><li><strong>Resend</strong> (E-Mail-Versand) — resend.com/legal/privacy-policy</li><li><strong>Anthropic</strong> (KI-Analyse) — anthropic.com/privacy</li></ul>
<h2>6. Datenspeicherung</h2>
<p>Deine Daten werden so lange gespeichert, wie du ein aktives Konto bei PEAK hast. Nach Kündigung werden persönliche Daten innerhalb von 30 Tagen gelöscht.</p>
<h2>7. Deine Rechte</h2>
<p>Du hast das Recht auf Auskunft, Berichtigung, Löschung und Datenportabilität. Kontakt: <a href="mailto:${COMPANY.email}">${COMPANY.email}</a></p>
<h2>8. Cookies</h2>
<p>PEAK verwendet keine Tracking-Cookies. Es werden ausschließlich technisch notwendige Session-Daten verwendet.</p>
<hr><p class="note"><strong>English version:</strong> <a href="/privacy">/privacy</a></p>
</body></html>`);
});

app.get('/privacy', (req, res) => {
  res.send(`<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Privacy Policy — PEAK</title><style>${legalStyle}</style></head><body>
<a href="${FRONTEND_URL}" class="back">← Back to app</a>
<h1>Privacy Policy</h1>
<p><strong>Last updated:</strong> ${new Date().toLocaleDateString('en-GB')}</p>
<h2>1. Controller</h2>
<p>${COMPANY.owner}<br>${COMPANY.name}<br>${COMPANY.address}<br>Email: <a href="mailto:${COMPANY.email}">${COMPANY.email}</a></p>
<h2>2. Data We Collect</h2>
<ul><li>Name and email address (on registration)</li><li>Health data (weight, height, dietary preferences — voluntary)</li><li>Payment data (processed by Stripe — we do not store card details)</li><li>Usage data (ratings, log entries)</li></ul>
<h2>3. Purpose</h2>
<p>We process your data to provide the personalised AI coaching service, process payments, and send transactional communications.</p>
<h2>4. Legal Basis</h2>
<p>Processing is based on Art. 6(1)(b) GDPR (contract performance) and Art. 6(1)(a) GDPR (consent).</p>
<h2>5. Third Parties</h2>
<ul><li><strong>Stripe</strong> (payments) — stripe.com/privacy</li><li><strong>Supabase</strong> (database) — supabase.com/privacy</li><li><strong>Resend</strong> (email) — resend.com/legal/privacy-policy</li><li><strong>Anthropic</strong> (AI) — anthropic.com/privacy</li></ul>
<h2>6. Data Retention</h2>
<p>Your data is retained for as long as you have an active PEAK account. Personal data is deleted within 30 days of account cancellation.</p>
<h2>7. Your Rights</h2>
<p>You have the right to access, rectification, deletion and data portability. Contact: <a href="mailto:${COMPANY.email}">${COMPANY.email}</a></p>
<h2>8. Cookies</h2>
<p>PEAK does not use tracking cookies. Only technically necessary session data is used.</p>
<hr><p class="note"><strong>Deutsche Version:</strong> <a href="/datenschutz">/datenschutz</a></p>
</body></html>`);
});

app.get('/impressum', (req, res) => {
  res.send(`<!DOCTYPE html><html lang="de"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Impressum — PEAK</title><style>${legalStyle}</style></head><body>
<a href="${FRONTEND_URL}" class="back">← Zurück zur App</a>
<h1>Impressum</h1>
<p><em>Angaben gemäß § 5 TMG</em></p>
<h2>Verantwortlicher</h2>
<p><strong>${COMPANY.owner}</strong><br>${COMPANY.name}<br>${COMPANY.address}</p>
<h2>Kontakt</h2>
<p>E-Mail: <a href="mailto:${COMPANY.email}">${COMPANY.email}</a><br>Website: <a href="${COMPANY.website}">${COMPANY.website}</a></p>
<h2>Haftungsausschluss</h2>
<p>PEAK ersetzt keine medizinische oder ernährungswissenschaftliche Beratung. Bei gesundheitlichen Fragen wende dich bitte an einen Arzt oder Ernährungsberater.</p>
<h2>Urheberrecht</h2>
<p>Die durch den Betreiber erstellten Inhalte und Werke unterliegen dem deutschen Urheberrecht.</p>
<h2>Streitschlichtung</h2>
<p>Die EU-Kommission stellt eine Plattform zur Online-Streitbeilegung bereit: <a href="https://ec.europa.eu/consumers/odr" target="_blank">ec.europa.eu/consumers/odr</a>. Wir nehmen nicht an Streitbeilegungsverfahren teil.</p>
<hr><p class="note"><strong>English:</strong> <a href="/imprint">/imprint</a> · <strong>Datenschutz:</strong> <a href="/datenschutz">/datenschutz</a></p>
</body></html>`);
});

app.get('/imprint', (req, res) => {
  res.send(`<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Legal Notice — PEAK</title><style>${legalStyle}</style></head><body>
<a href="${FRONTEND_URL}" class="back">← Back to app</a>
<h1>Legal Notice</h1>
<h2>Responsible Party</h2>
<p><strong>${COMPANY.owner}</strong><br>${COMPANY.name}<br>${COMPANY.address}</p>
<h2>Contact</h2>
<p>Email: <a href="mailto:${COMPANY.email}">${COMPANY.email}</a><br>Website: <a href="${COMPANY.website}">${COMPANY.website}</a></p>
<h2>Disclaimer</h2>
<p>PEAK does not replace professional medical or nutritional advice. Consult a qualified professional for health concerns.</p>
<hr><p class="note"><strong>Deutsche Version:</strong> <a href="/impressum">/impressum</a> · <strong>Privacy:</strong> <a href="/privacy">/privacy</a></p>
</body></html>`);
});

// ── CREATE CHECKOUT SESSION ───────────────────────────────────────────
// ── CHECK IF EMAIL ALREADY HAS AN ACCOUNT ─────────────────────────────
// Called from Step 7 before redirecting to Stripe Checkout.
// Returns { exists: true/false, hasSubscription: true/false }
app.post('/auth/check-email', async (req, res) => {
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
app.post('/auth/send-login-link', async (req, res) => {
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

const crypto = require('crypto');

function hashCode(code) {
  return crypto.createHash('sha256').update(String(code)).digest('hex');
}

function generateOTP() {
  // 6-digit, zero-padded, avoids leading-zero truncation issues
  return String(crypto.randomInt(0, 1000000)).padStart(6, '0');
}

app.post('/auth/send-otp', async (req, res) => {
  try {
    const { email, lang } = req.body || {};
    if (!email || typeof email !== 'string' || !email.includes('@')) {
      return res.status(400).json({ error: 'Valid email required' });
    }
    const normalizedEmail = email.trim().toLowerCase();
    const emailLang = (lang === 'de' || lang === 'en') ? lang : 'en';

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

app.post('/auth/verify-otp', async (req, res) => {
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
          const { data: listData } = await supabase.auth.admin.listUsers({ perPage: 200 });
          const match = listData?.users?.find(u => (u.email || '').toLowerCase() === normalizedEmail);
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
app.post('/auth/signup-free', async (req, res) => {
  try {
    const { email, userData, consent, lang } = req.body;

    if (!email) return res.status(400).json({ error: 'Email is required' });

    if (!consent || consent.healthData !== true || consent.terms !== true) {
      console.warn(`⚠️ Free signup blocked for ${email}: missing GDPR consent`);
      return res.status(400).json({ error: 'Consent required' });
    }

    const normalizedEmail = email.toLowerCase().trim();

    // Check if account already exists
    try {
      const { data: existing } = await supabase.auth.admin.listUsers();
      const match = existing?.users?.find(u => u.email?.toLowerCase() === normalizedEmail);
      if (match) {
        // Existing account — send them a login link instead
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
            to: normalizedEmail,
            subject: mail.subject,
            html: mail.html,
          });
        }
        return res.json({ success: true, existing: true });
      }
    } catch (e) {
      console.warn('listUsers check failed, proceeding with signup:', e.message);
    }

    // Create new auth user (service role — bypasses signup toggle)
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

    const authUserId = created.user.id;
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

    // Generate magic link + send welcome
    const { data: linkData } = await supabase.auth.admin.generateLink({
      type: 'magiclink',
      email: normalizedEmail,
      options: { redirectTo: `${FRONTEND_URL}/` },
    });
    const magicLink = linkData?.properties?.action_link || null;

    try {
      await sendEmail(normalizedEmail, 'welcome', {
        name: userData?.name || '',
        goal: userData?.goal || '',
        goals: Array.isArray(userData?.goals) ? userData.goals : [],
        sport: userData?.sport || '',
        magicLink,
        isFree: true,
        lang: lang === 'de' ? 'de' : (lang === 'en' ? 'en' : undefined),
      });
    } catch (err) {
      console.error('⚠️  Free welcome email failed:', err.message);
    }

    console.log(`✅ Free signup complete: ${normalizedEmail} (${authUserId})`);
    res.json({ success: true, userId: authUserId });
  } catch (err) {
    console.error('❌ signup-free error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── OPEN STRIPE CUSTOMER PORTAL ───────────────────────────────────────
// Generates a one-time portal session URL for the given email.
// User is redirected there to manage/cancel their subscription.
app.post('/customer-portal', async (req, res) => {
  try {
    const { email } = req.body;
    if (!email) return res.status(400).json({ error: 'Email required' });

    const { data: profile, error: profileErr } = await supabase
      .from('users')
      .select('stripe_customer_id')
      .eq('email', email.toLowerCase().trim())
      .maybeSingle();

    if (profileErr || !profile?.stripe_customer_id) {
      return res.status(404).json({ error: 'No subscription found for this email' });
    }

    const portalSession = await stripe.billingPortal.sessions.create({
      customer: profile.stripe_customer_id,
      return_url: FRONTEND_URL,
    });

    console.log(`✅ Portal session created for ${email}`);
    res.json({ url: portalSession.url });
  } catch (err) {
    console.error('❌ customer-portal error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── AI PROXY ──────────────────────────────────────────────────────────
// Frontend can't call Anthropic directly (CORS + API key must stay server-side).
// This endpoint proxies requests. Max tokens clamped 100-2000 to prevent abuse.
app.post('/ai/generate', async (req, res) => {
  try {
    const { prompt, max_tokens, purpose } = req.body;
    if (!prompt || typeof prompt !== 'string') {
      return res.status(400).json({ error: 'prompt required' });
    }

    // Optional auth check: log who's calling for monitoring
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
    const tokens = Math.min(Math.max(parseInt(max_tokens) || 800, 100), 2000);

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

app.post('/create-checkout', async (req, res) => {
  try {
    const { email, plan, tier, userData, consent, voucher, lang } = req.body;

    if (!email) {
      return res.status(400).json({ error: 'Email is required' });
    }

    if (!consent || consent.healthData !== true || consent.terms !== true) {
      console.warn(`⚠️ Checkout blocked for ${email}: missing GDPR consent`);
      return res.status(400).json({ error: 'Consent required' });
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
      // Automatically show all payment methods enabled in Stripe Dashboard
      // (card, Apple Pay, Google Pay, PayPal, Klarna, SEPA, etc.)
      // No code change needed when you enable/disable methods in Stripe.
      automatic_payment_methods: { enabled: true },
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
app.post('/voucher/validate', async (req, res) => {
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

// ── USER PROFILE (auth-protected) ─────────────────────────────────────
// Frontend calls this after Supabase auth to load full profile data
// (plan, goal, sport, trial_end, status, etc.).
// Uses the user's access token to validate identity, then looks up the
// profile row via service role (bypasses RLS, safe because we verified first).
app.get('/user/profile', async (req, res) => {
  try {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
    if (!token) return res.status(401).json({ error: 'Missing auth token' });

    // Validate token by asking Supabase who this is
    const { data: userData, error: userErr } = await supabase.auth.getUser(token);
    if (userErr || !userData?.user) {
      return res.status(401).json({ error: 'Invalid or expired token' });
    }
    const user = userData.user;

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
      // Edge case: auth user exists but no public.users row (shouldn't happen
      // in normal flow, but handle gracefully rather than crashing)
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

    // Whitelist of editable fields. Anything not in this list is silently ignored.
    // NO access to: id, email, stripe_*, plan, tier, trial_*, status, consent_*, created_at
    const ALLOWED = [
      'name','age','gender','weight','dweight','height','sleep',
      'job','commute','stress',
      'sport','level','sessions','dur','equip',
      'al','di','cu','cook','budget','goal','goals','lang',
    ];
    const updates = {};
    for (const k of ALLOWED) {
      if (req.body[k] !== undefined) updates[k] = req.body[k];
    }

    if (Object.keys(updates).length === 0) {
      return res.status(400).json({ error: 'No valid fields to update' });
    }

    updates.updated_at = new Date().toISOString();

    const { data, error } = await supabase
      .from('users')
      .update(updates)
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

  console.log(`🔔 Webhook received: ${event.type}`);

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

      const meta = session.metadata || {};
      const trialEnd = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000);

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
            const { data: existing, error: listErr } = await supabase.auth.admin.listUsers();
            if (listErr) throw listErr;
            const match = existing?.users?.find(u => u.email?.toLowerCase() === email.toLowerCase());
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
          .select('age,gender,weight,dweight,height,sleep,job,commute,stress,level,sessions,dur,equip,al,di,cu,cook,budget')
          .eq('id', authUserId)
          .maybeSingle();
        prior = data || null;
      } catch (_) {}

      const pickNum = (meta, prior, parser) => {
        if (meta !== null && meta !== undefined && meta !== '') return parser(meta);
        if (prior !== null && prior !== undefined) return prior;
        return null;
      };
      const pickStr = (meta, prior) => {
        if (meta) return meta;
        if (prior) return prior;
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
            // Mark user as blocked
            await supabase.from('users').update({ status: 'blocked_voucher_abuse' }).eq('email', email);
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
        const { error } = await supabase.from('users').update({ status: 'cancelled' }).eq('email', email);
        if (error) console.error('❌ Supabase update (cancelled) failed:', error.message);
        else console.log(`✅ User cancelled: ${email}`);
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
          const { error } = await supabase.from('users').update({ status: 'active' }).eq('email', email);
          if (error) console.error('❌ Supabase update (active) failed:', error.message);
          else console.log(`✅ User renewed: ${email}`);
        }
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
        <p style="margin:0;color:#666;font-size:10px;letter-spacing:0.5px;">${COMPANY.name} · ${COMPANY.address}</p>
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
      ? 'PEAK by MJ Performance · ' + COMPANY.address
      : 'PEAK by MJ Performance · ' + COMPANY.address,
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
    welcomeSubject: (isFree) => isFree ? 'Willkommen bei PEAK — dein Gratis-Plan ist bereit' : 'Willkommen bei PEAK — dein Plan ist bereit',
    welcomeLabel: (n) => 'Willkommen' + (n ? ', ' + n : ''),
    welcomeH1a: 'Dein Plan',
    welcomeH1b: 'ist live.',
    welcomeH1FreeB: 'steht.',
    welcomeIntro: (isFree, goal, sport) => {
      let intro = 'System statt Motivation. Hier startet dein Weg — ';
      if (sport && goal) intro += `dein individueller ${sport}-Plan, abgestimmt auf „${goal}".`;
      else if (sport) intro += `dein individueller ${sport}-Plan.`;
      else if (goal) intro += `maßgeschneidert auf dein Ziel: „${goal}".`;
      else intro += 'KI-gestützte Ernährung, Training und Regeneration — auf dich zugeschnitten.';
      return intro;
    },
    includesFree: 'Dein Gratis-Plan enthält',
    includesPaid: 'In PEAK enthalten',
    f1: (sport) => sport ? `KI-Ernährungsplan für dein ${sport}-Training` : 'KI-Ernährungsplan, passend zu Ziel & Geschmack',
    f2Free: (sport) => sport ? `${sport}-Training — eine Woche zum Reinschnuppern` : 'Trainings-Vorschau für deine Sportart',
    f2Paid: (sport) => sport ? `12-Wochen-${sport}-Programm mit Progression` : '12-Wochen-Programm für deine Sportart',
    f3Free: '3 Plan-Updates inklusive',
    f3Paid: 'Regeneration — Schlaf, Hydration, Mobilität',
    f4Free: 'Jederzeit Upgrade möglich',
    f4Paid: 'Barcode-Scanner & Shopping-Modus',
    stepsTitle: 'Deine ersten 3 Schritte',
    step1: 'Plan öffnen und Essensplan anschauen',
    step2Free: 'Trainings-Tab — Woche 1 ansehen',
    step2Paid: 'Erstes Training starten',
    step3Free: 'Mahlzeit protokollieren für Fortschritts-Tracking',
    step3Paid: 'Regenerations-Protokoll lesen',
    boxFree: '<strong>Gratis-Plan.</strong> Keine Karte, keine Abbuchung. Upgrade jederzeit für unbegrenzte Pläne, volle Trainingsprogression und Regenerations-Tools.',
    boxPaid: '<strong>7 Tage kostenlos.</strong> Erst ab Tag 8 wird abgebucht. Erinnerungen an Tag 5 und 6.',
    ctaOpen: 'Plan öffnen',
    day5Subject: 'Noch 2 Tage — deine PEAK-Testphase',
    day5Label: '48 Stunden übrig',
    day5H1: (n) => (n ? n + ',<br>' : '') + 'deine Testphase<br>endet bald.',
    day5Body: (sport, goal) => {
      const what = sport ? `dein ${sport}-Training, Ernährungsplan und Regenerations-Protokoll` : 'dein Ernährungsplan, Training und Regenerations-Protokoll';
      return `Noch 2 Tage auf deiner 7-Tage-Testphase. ${what} wird pausiert, wenn du nicht weitermachst${goal ? '. Du bist auf dem Weg zu: „' + goal + '".' : '.'}`;
    },
    day5CTA: 'Weitermachen',
    day5Cancel: 'Kündigen: PEAK öffnen → Einstellungen → Abonnement → Kündigen. 10 Sekunden, kein Telefonat.',
    day6Subject: 'Letzter Tag — deine Testphase endet morgen',
    day6Label: 'Letzte 24 Stunden',
    day6H1: (n) => (n ? n + ',<br>' : '') + 'morgen<br>geht es los.',
    day6Body: 'Deine kostenlose Testphase endet morgen. Dein PEAK-Abo startet automatisch — du musst nichts tun, um weiterzumachen.',
    day6Box: '<strong>Kündigen:</strong> PEAK öffnen → Einstellungen → Abonnement → Testphase beenden. In 10 Sekunden erledigt.',
    day6CTA: 'Plan behalten',
  } : {
    welcomeSubject: (isFree) => isFree ? 'Welcome to PEAK — your free plan is ready' : 'Welcome to PEAK — your plan is ready',
    welcomeLabel: (n) => 'Welcome' + (n ? ', ' + n : ''),
    welcomeH1a: 'Your plan is',
    welcomeH1b: 'live.',
    welcomeH1FreeB: 'live.',
    welcomeIntro: (isFree, goal, sport) => {
      let intro = 'System over motivation. This is where it starts — ';
      if (sport && goal) intro += `your custom ${sport} plan, tuned to "${goal}".`;
      else if (sport) intro += `your custom ${sport} programme.`;
      else if (goal) intro += `built around your goal: "${goal}".`;
      else intro += 'AI-built nutrition, training and recovery, tuned to you.';
      return intro;
    },
    includesFree: 'Your free plan includes',
    includesPaid: 'Inside PEAK',
    f1: (sport) => sport ? `AI nutrition plan for your ${sport} training` : 'AI nutrition plan, matched to goal and taste',
    f2Free: (sport) => sport ? `${sport} training — 1-week preview` : '1-week training preview for your sport',
    f2Paid: (sport) => sport ? `12-week ${sport} programme with progression` : 'Personalised programme for your sport',
    f3Free: '3 plan regenerations included',
    f3Paid: 'Recovery protocol — sleep, hydration, mobility',
    f4Free: 'Upgrade any time',
    f4Paid: 'Barcode scanner and shopping mode',
    stepsTitle: 'Your first 3 steps',
    step1: 'Open your plan and check your meals',
    step2Free: 'Open the Training tab — see Week 1',
    step2Paid: 'Start your first training session',
    step3Free: 'Log a meal to track progress',
    step3Paid: 'Read your recovery protocol',
    boxFree: '<strong>Free plan.</strong> No card, no charge. Upgrade any time for unlimited plans, full training progression and recovery tools.',
    boxPaid: '<strong>7 days free.</strong> No charge until Day 8. Reminders on Day 5 and Day 6.',
    ctaOpen: 'Open my plan',
    day5Subject: 'Two days left on your PEAK trial',
    day5Label: '48 hours left',
    day5H1: (n) => (n ? n + ',<br>' : '') + 'your trial<br>ends soon.',
    day5Body: (sport, goal) => {
      const what = sport ? `your ${sport} training, nutrition plan and recovery protocol` : 'your nutrition plan, training and recovery protocol';
      return `Two days remain on your free 7-day trial. ${what} will pause unless you continue${goal ? '. You\'re on the way to: "' + goal + '".' : '.'}`;
    },
    day5CTA: 'Continue my journey',
    day5Cancel: 'To cancel: open PEAK → Settings → Subscription → Cancel. 10 seconds, no phone calls.',
    day6Subject: 'Final day — your PEAK trial ends tomorrow',
    day6Label: 'Final 24 hours',
    day6H1: (n) => (n ? n + ',<br>' : '') + 'tomorrow<br>it begins.',
    day6Body: 'Your free trial ends tomorrow. Your PEAK subscription begins automatically — no action needed to continue.',
    day6Box: '<strong>To cancel:</strong> Open PEAK → Settings → Subscription → Cancel trial. Done in 10 seconds.',
    day6CTA: 'Keep my plan',
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
      subject: L.welcomeSubject(data?.isFree),
      html: emailShell(RESPONSIVE_CSS + `
        <tr><td>${emailHeader()}</td></tr>
        <tr><td class="email-pad-big" style="padding:48px 40px 8px;">
          <p style="margin:0 0 14px;font-family:${FONT_BODY};font-size:11px;font-weight:700;letter-spacing:3px;color:${BRAND.red};text-transform:uppercase;">${L.welcomeLabel(name)}</p>
          <h1 class="email-h1" style="margin:0 0 14px;font-family:${FONT_HEAD};font-weight:900;font-size:38px;line-height:1.05;letter-spacing:1px;text-transform:uppercase;color:${BRAND.ink};">
            ${L.welcomeH1a}<br>${data?.isFree ? L.welcomeH1FreeB : L.welcomeH1b}
          </h1>
          ${goalHeadline ? `<p style="margin:0 0 18px;font-family:${FONT_HEAD};font-size:13px;font-weight:700;letter-spacing:2px;color:${BRAND.red};text-transform:uppercase;">🎯 ${goalHeadline}</p>` : ''}
          <p style="margin:0 0 32px;font-family:${FONT_BODY};font-size:15px;line-height:1.65;color:${BRAND.ink2};">
            ${L.welcomeIntro(data?.isFree, goal, sport)}
          </p>
        </td></tr>

        <tr><td class="email-pad" style="padding:0 40px 8px;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-top:1px solid ${BRAND.border};">
            <tr><td style="padding:24px 0 8px;">
              <p style="margin:0 0 16px;font-family:${FONT_HEAD};font-size:11px;font-weight:900;letter-spacing:3px;color:${BRAND.ink};text-transform:uppercase;">${data?.isFree ? L.includesFree : L.includesPaid}</p>
            </td></tr>
            <tr><td style="font-family:${FONT_BODY};font-size:14px;line-height:1.7;color:${BRAND.ink2};padding-bottom:6px;">
              <span style="color:${BRAND.red};font-weight:700;">—</span>&nbsp;&nbsp;${L.f1(sport)}
            </td></tr>
            <tr><td style="font-family:${FONT_BODY};font-size:14px;line-height:1.7;color:${BRAND.ink2};padding-bottom:6px;">
              <span style="color:${BRAND.red};font-weight:700;">—</span>&nbsp;&nbsp;${data?.isFree ? L.f2Free(sport) : L.f2Paid(sport)}
            </td></tr>
            <tr><td style="font-family:${FONT_BODY};font-size:14px;line-height:1.7;color:${BRAND.ink2};padding-bottom:6px;">
              <span style="color:${BRAND.red};font-weight:700;">—</span>&nbsp;&nbsp;${data?.isFree ? L.f3Free : L.f3Paid}
            </td></tr>
            <tr><td style="font-family:${FONT_BODY};font-size:14px;line-height:1.7;color:${BRAND.ink2};padding-bottom:24px;">
              <span style="color:${BRAND.red};font-weight:700;">—</span>&nbsp;&nbsp;${data?.isFree ? L.f4Free : L.f4Paid}
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
                <td style="font-family:${FONT_BODY};font-size:14px;line-height:1.5;color:${BRAND.ink2};padding-top:2px;">${data?.isFree ? L.step2Free : L.step2Paid}</td>
              </tr></table>
            </td></tr>
            <tr><td style="padding:8px 0;">
              <table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>
                <td valign="top" style="padding-right:12px;">
                  <div style="width:24px;height:24px;background:${BRAND.red};color:${BRAND.white};font-family:${FONT_HEAD};font-weight:900;font-size:13px;text-align:center;line-height:24px;">3</div>
                </td>
                <td style="font-family:${FONT_BODY};font-size:14px;line-height:1.5;color:${BRAND.ink2};padding-top:2px;">${data?.isFree ? L.step3Free : L.step3Paid}</td>
              </tr></table>
            </td></tr>
          </table>
        </td></tr>

        <tr><td class="email-pad" style="padding:8px 40px 36px;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:${BRAND.light};border-left:3px solid ${BRAND.red};">
            <tr><td style="padding:18px 22px;font-family:${FONT_BODY};font-size:13px;line-height:1.6;color:${BRAND.ink2};">
              ${data?.isFree ? L.boxFree : L.boxPaid}
            </td></tr>
          </table>
        </td></tr>

        <tr><td class="email-cta" align="center" style="padding:0 40px 56px;">
          ${emailButton(data?.magicLink || FRONTEND_URL, L.ctaOpen)}
        </td></tr>

        <tr><td>${emailFooter(to)}</td></tr>
      `)
    },

    day5: {
      subject: L.day5Subject,
      html: emailShell(RESPONSIVE_CSS + `
        <tr><td>${emailHeader()}</td></tr>
        <tr><td class="email-pad-big" style="padding:48px 40px 8px;">
          <p style="margin:0 0 14px;font-family:${FONT_BODY};font-size:11px;font-weight:700;letter-spacing:3px;color:${BRAND.red};text-transform:uppercase;">${L.day5Label}</p>
          <h1 class="email-h1" style="margin:0 0 20px;font-family:${FONT_HEAD};font-weight:900;font-size:38px;line-height:1.05;letter-spacing:1px;text-transform:uppercase;color:${BRAND.ink};">
            ${L.day5H1(name)}
          </h1>
          <p style="margin:0 0 32px;font-family:${FONT_BODY};font-size:15px;line-height:1.65;color:${BRAND.ink2};">
            ${L.day5Body(sport, goal)}
          </p>
        </td></tr>

        <tr><td class="email-cta" align="center" style="padding:0 40px 24px;">
          ${emailButton(FRONTEND_URL, L.day5CTA)}
        </td></tr>

        <tr><td class="email-pad" style="padding:0 40px 48px;">
          <p style="margin:0;font-family:${FONT_BODY};font-size:12px;line-height:1.6;color:${BRAND.faint};text-align:center;">
            ${L.day5Cancel}
          </p>
        </td></tr>

        <tr><td>${emailFooter(to)}</td></tr>
      `)
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
    }
  };

  const tmpl = templates[type];
  if (!tmpl) return;
  try {
    await resend.emails.send({ from: FROM_EMAIL, to, subject: tmpl.subject, html: tmpl.html });
    console.log(`📧 ${type} → ${to} (${lang})`);
  } catch (err) {
    console.error(`Email error:`, err.message);
  }
}
// ── CRON ──────────────────────────────────────────────────────────────
cron.schedule('0 10 * * *', async () => {
  const now = new Date();
  const { data: users } = await supabase.from('users').select('*').eq('status', 'trial').eq('unsubscribed', false);
  for (const user of users || []) {
    const daysLeft = Math.ceil((new Date(user.trial_end) - now) / (1000 * 60 * 60 * 24));
    if (daysLeft === 2) await sendEmail(user.email, 'day5', { name: user.name });
    else if (daysLeft === 1) await sendEmail(user.email, 'day6', { name: user.name });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🚀 PEAK Backend on port ${PORT}`));
