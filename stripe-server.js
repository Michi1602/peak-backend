const express = require('express');
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);
const { createClient } = require('@supabase/supabase-js');
const { Resend } = require('resend');
const cron = require('node-cron');
const cors = require('cors');

const app = express();
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const resend = new Resend(process.env.RESEND_API_KEY);

const FRONTEND_URL = process.env.FRONTEND_URL || 'https://peak-frontend.vercel.app';
const BACKEND_URL = process.env.BACKEND_URL || 'https://peak-backend-u52q.onrender.com';
const FROM_EMAIL = 'PEAK <hello@mj-performance.net>';

const COMPANY = {
  name: 'MJ Performance',
  address: 'Am Hasel 6, 85139 Wettstetten',
  email: 'hello@mj-performance.net',
  website: 'https://mj-performance.net',
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
app.post('/create-checkout', async (req, res) => {
  try {
    const { email, plan, userData } = req.body;

    if (!email) {
      return res.status(400).json({ error: 'Email is required' });
    }

    const priceId = plan === 'annual' ? process.env.STRIPE_PRICE_ANNUAL : process.env.STRIPE_PRICE_MONTHLY;
    if (!priceId) {
      console.error('❌ Missing STRIPE_PRICE_* env var for plan:', plan);
      return res.status(500).json({ error: 'Server misconfiguration: price not set' });
    }

    // Consolidated metadata — attached to BOTH session and subscription
    // so the webhook can read it regardless of which event fires.
    const sharedMetadata = {
      userName: userData?.name || '',
      userGoal: userData?.goal || '',
      userSport: userData?.sport || '',
      plan: plan || 'monthly',
    };

    const session = await stripe.checkout.sessions.create({
      mode: 'subscription',
      payment_method_types: ['card'],
      customer_email: email,
      line_items: [{ price: priceId, quantity: 1 }],
      subscription_data: {
        trial_period_days: 7,
        metadata: sharedMetadata,
      },
      success_url: `${FRONTEND_URL}?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${FRONTEND_URL}?cancelled=true`,
      metadata: sharedMetadata,
    });

    console.log(`✅ Checkout session created for ${email} (plan: ${plan})`);
    res.json({ url: session.url, sessionId: session.id });
  } catch (err) {
    console.error('❌ Checkout error:', err.message);
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
      const userRow = {
        id: authUserId,
        email,
        name: meta.userName || '',
        stripe_customer_id: session.customer || null,
        stripe_subscription_id: session.subscription || null,
        plan: meta.plan || 'monthly',
        goal: meta.userGoal || '',
        sport: meta.userSport || '',
        trial_start: new Date().toISOString(),
        trial_end: trialEnd.toISOString(),
        status: 'trial',
        unsubscribed: false,
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
          magicLink,
        });
      } catch (err) {
        console.error('❌ Welcome email failed for', email, ':', err.message);
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
          <a href="${BACKEND_URL}/impressum" style="color:#AAA;text-decoration:none;">Impressum</a>
          <span style="color:#555;"> · </span>
          <a href="${BACKEND_URL}/datenschutz" style="color:#AAA;text-decoration:none;">Datenschutz</a>
          <span style="color:#555;"> · </span>
          <a href="${BACKEND_URL}/privacy" style="color:#AAA;text-decoration:none;">Privacy</a>
          <span style="color:#555;"> · </span>
          <a href="${unsub}" style="color:#AAA;text-decoration:none;">Unsubscribe</a>
        </p>
        <p style="margin:0;color:#666;font-size:10px;letter-spacing:0.5px;">${COMPANY.name} · ${COMPANY.address}</p>
      </td>
    </tr>
  </table>`;
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
  // Check if user has unsubscribed. Wrap in try/catch because .single()
  // throws if 0 rows match, which is fine right after signup (race condition).
  let unsubscribed = false;
  try {
    const { data: user, error } = await supabase
      .from('users')
      .select('unsubscribed')
      .eq('email', to)
      .maybeSingle();
    if (error) console.error('Unsubscribe-check error:', error.message);
    unsubscribed = user?.unsubscribed === true;
  } catch (err) {
    console.error('Unsubscribe-check exception:', err.message);
  }
  if (unsubscribed) return;

  const name = data?.name || '';
  const greeting = name ? name : 'athlete';

  const templates = {
    welcome: {
      subject: 'Welcome to PEAK — your plan is ready',
      html: emailShell(`
        <tr><td>${emailHeader()}</td></tr>
        <tr><td style="padding:48px 40px 8px;">
          <p style="margin:0 0 14px;font-family:${FONT_BODY};font-size:11px;font-weight:700;letter-spacing:3px;color:${BRAND.red};text-transform:uppercase;">Welcome${name ? ', ' + name : ''}</p>
          <h1 style="margin:0 0 20px;font-family:${FONT_HEAD};font-weight:900;font-size:38px;line-height:1.05;letter-spacing:1px;text-transform:uppercase;color:${BRAND.ink};">
            Your plan is<br>live.
          </h1>
          <p style="margin:0 0 32px;font-family:${FONT_BODY};font-size:15px;line-height:1.65;color:${BRAND.ink2};">
            System over motivation. This is where it starts — your AI-built nutrition, training and recovery protocol, tuned to your goal.
          </p>
        </td></tr>

        <tr><td style="padding:0 40px 8px;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-top:1px solid ${BRAND.border};">
            <tr><td style="padding:24px 0 8px;">
              <p style="margin:0 0 16px;font-family:${FONT_HEAD};font-size:11px;font-weight:900;letter-spacing:3px;color:${BRAND.ink};text-transform:uppercase;">Inside PEAK</p>
            </td></tr>
            <tr><td style="font-family:${FONT_BODY};font-size:14px;line-height:1.7;color:${BRAND.ink2};padding-bottom:6px;">
              <span style="color:${BRAND.red};font-weight:700;">—</span>&nbsp;&nbsp;AI nutrition plan, matched to your goal and taste
            </td></tr>
            <tr><td style="font-family:${FONT_BODY};font-size:14px;line-height:1.7;color:${BRAND.ink2};padding-bottom:6px;">
              <span style="color:${BRAND.red};font-weight:700;">—</span>&nbsp;&nbsp;Personalised programme for your sport
            </td></tr>
            <tr><td style="font-family:${FONT_BODY};font-size:14px;line-height:1.7;color:${BRAND.ink2};padding-bottom:6px;">
              <span style="color:${BRAND.red};font-weight:700;">—</span>&nbsp;&nbsp;Recovery protocol — sleep, hydration, stress
            </td></tr>
            <tr><td style="font-family:${FONT_BODY};font-size:14px;line-height:1.7;color:${BRAND.ink2};padding-bottom:24px;">
              <span style="color:${BRAND.red};font-weight:700;">—</span>&nbsp;&nbsp;Barcode scanner and shopping mode
            </td></tr>
          </table>
        </td></tr>

        <tr><td style="padding:8px 40px 36px;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:${BRAND.light};border-left:3px solid ${BRAND.red};">
            <tr><td style="padding:18px 22px;font-family:${FONT_BODY};font-size:13px;line-height:1.6;color:${BRAND.ink2};">
              <strong style="color:${BRAND.ink};">7-day free trial.</strong> No charge until Day 8. We'll remind you on Day 5 and Day 6.
            </td></tr>
          </table>
        </td></tr>

        <tr><td align="center" style="padding:0 40px 56px;">
          ${emailButton(data?.magicLink || FRONTEND_URL, 'Open my plan')}
        </td></tr>

        <tr><td>${emailFooter(to)}</td></tr>
      `)
    },

    day5: {
      subject: 'Two days left on your PEAK trial',
      html: emailShell(`
        <tr><td>${emailHeader()}</td></tr>
        <tr><td style="padding:48px 40px 8px;">
          <p style="margin:0 0 14px;font-family:${FONT_BODY};font-size:11px;font-weight:700;letter-spacing:3px;color:${BRAND.red};text-transform:uppercase;">48 hours left</p>
          <h1 style="margin:0 0 20px;font-family:${FONT_HEAD};font-weight:900;font-size:38px;line-height:1.05;letter-spacing:1px;text-transform:uppercase;color:${BRAND.ink};">
            ${name ? name + ',<br>' : ''}your trial<br>ends soon.
          </h1>
          <p style="margin:0 0 32px;font-family:${FONT_BODY};font-size:15px;line-height:1.65;color:${BRAND.ink2};">
            Two days remain on your free 7-day trial. Your nutrition plan, training programme and recovery protocol will pause unless you continue.
          </p>
        </td></tr>

        <tr><td align="center" style="padding:0 40px 24px;">
          ${emailButton(FRONTEND_URL, 'Continue my journey')}
        </td></tr>

        <tr><td style="padding:0 40px 48px;">
          <p style="margin:0;font-family:${FONT_BODY};font-size:12px;line-height:1.6;color:${BRAND.faint};text-align:center;">
            To cancel: open PEAK → Settings → Subscription → Cancel. 10 seconds, no phone calls.
          </p>
        </td></tr>

        <tr><td>${emailFooter(to)}</td></tr>
      `)
    },

    day6: {
      subject: 'Final day — your PEAK trial ends tomorrow',
      html: emailShell(`
        <tr><td>${emailHeader()}</td></tr>
        <tr><td style="padding:48px 40px 8px;">
          <p style="margin:0 0 14px;font-family:${FONT_BODY};font-size:11px;font-weight:700;letter-spacing:3px;color:${BRAND.red};text-transform:uppercase;">Final 24 hours</p>
          <h1 style="margin:0 0 20px;font-family:${FONT_HEAD};font-weight:900;font-size:38px;line-height:1.05;letter-spacing:1px;text-transform:uppercase;color:${BRAND.ink};">
            ${name ? name + ',<br>' : ''}tomorrow<br>it begins.
          </h1>
          <p style="margin:0 0 28px;font-family:${FONT_BODY};font-size:15px;line-height:1.65;color:${BRAND.ink2};">
            Your free trial ends tomorrow. Your PEAK subscription begins automatically — no action needed to continue.
          </p>

          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:${BRAND.light};border-left:3px solid ${BRAND.red};margin-bottom:32px;">
            <tr><td style="padding:18px 22px;font-family:${FONT_BODY};font-size:13px;line-height:1.6;color:${BRAND.ink2};">
              <strong style="color:${BRAND.ink};">To cancel:</strong> Open PEAK → Settings → Subscription → Cancel trial. Done in 10 seconds.
            </td></tr>
          </table>
        </td></tr>

        <tr><td align="center" style="padding:0 40px 56px;">
          ${emailButton(FRONTEND_URL, 'Keep my plan')}
        </td></tr>

        <tr><td>${emailFooter(to)}</td></tr>
      `)
    }
  };

  const tmpl = templates[type];
  if (!tmpl) return;
  try {
    await resend.emails.send({ from: FROM_EMAIL, to, subject: tmpl.subject, html: tmpl.html });
    console.log(`📧 ${type} → ${to}`);
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
