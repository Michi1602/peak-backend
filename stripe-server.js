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

      const userRow = {
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
        // NOTE: created_at intentionally omitted — Supabase default handles it,
        // and we don't want to overwrite it if the user already exists.
      };

      const { data, error } = await supabase
        .from('users')
        .upsert(userRow, { onConflict: 'email' })
        .select();

      if (error) {
        console.error('❌ Supabase upsert failed for', email, ':', error.message);
        // Return 200 anyway — we don't want Stripe to retry indefinitely
        // on DB issues. We log it and handle manually.
        return res.status(200).json({ received: true, db_error: error.message });
      }

      console.log(`✅ User upserted: ${email} (rows: ${data?.length || 0})`);

      try {
        await sendEmail(email, 'welcome', { name: meta.userName || '' });
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
function emailFooter(email) {
  const unsub = `${BACKEND_URL}/unsubscribe?email=${encodeURIComponent(email)}`;
  return `<div style="margin-top:40px;padding-top:20px;border-top:1px solid #E8E8E3;font-size:12px;color:#999;line-height:1.8">
    <p>Du erhältst diese E-Mail, weil du dich bei PEAK registriert hast. · You're receiving this because you signed up for PEAK.</p>
    <p>
      <a href="${BACKEND_URL}/impressum" style="color:#999">Impressum</a> &nbsp;·&nbsp;
      <a href="${BACKEND_URL}/datenschutz" style="color:#999">Datenschutz</a> &nbsp;·&nbsp;
      <a href="${BACKEND_URL}/privacy" style="color:#999">Privacy Policy</a> &nbsp;·&nbsp;
      <a href="${unsub}" style="color:#999">Abmelden / Unsubscribe</a>
    </p>
    <p>${COMPANY.name} · ${COMPANY.address}</p>
  </div>`;
}

async function sendEmail(to, type, data) {
  const { data: user } = await supabase.from('users').select('unsubscribed').eq('email', to).single().catch(() => ({ data: null }));
  if (user?.unsubscribed) return;

  const logo = `<div style="text-align:center;margin-bottom:28px"><span style="font-family:Georgia,serif;font-size:32px;font-weight:700;color:#1C1C1A">PEAK<span style="color:#2D6A4F">.</span></span></div>`;

  const templates = {
    welcome: {
      subject: '🌱 Welcome to PEAK — Your plan is ready',
      html: `<div style="font-family:sans-serif;max-width:560px;margin:0 auto;padding:40px 20px">${logo}
        <h1 style="font-size:26px;color:#1C1C1A">Welcome to PEAK${data.name ? ', ' + data.name : ''}! 🎉</h1>
        <p style="color:#6B6B68;line-height:1.7">Your personalised plan is live. This is the start of something real.</p>
        <div style="background:#EAF4EE;border-radius:12px;padding:20px;margin:24px 0">
          <p style="margin:0 0 8px;font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;color:#2D6A4F">What's waiting for you</p>
          <ul style="color:#1C1C1A;line-height:2;margin:0;padding-left:20px">
            <li>AI meal plan matched to your goal and taste</li>
            <li>Personalised workout programme for your sport</li>
            <li>Recovery protocol — sleep, hydration, stress tools</li>
            <li>Barcode scanner + live shopping mode</li>
          </ul>
        </div>
        <p style="color:#6B6B68;line-height:1.7"><strong style="color:#1C1C1A">Your free trial runs 7 days.</strong> No charge until Day 8. We remind you on Day 5 and Day 6.</p>
        <div style="text-align:center;margin:28px 0">
          <a href="${FRONTEND_URL}" style="display:inline-block;background:#2D6A4F;color:#fff;font-size:15px;font-weight:700;padding:14px 28px;border-radius:12px;text-decoration:none">Open my plan →</a>
        </div>
        ${emailFooter(to)}</div>`
    },
    day5: {
      subject: '⏰ 2 days left on your free trial',
      html: `<div style="font-family:sans-serif;max-width:560px;margin:0 auto;padding:40px 20px">${logo}
        <h1 style="font-size:26px;color:#1C1C1A">${data.name ? data.name + ', your' : 'Your'} trial ends in 2 days</h1>
        <p style="color:#6B6B68;line-height:1.7">Your 7-day free trial ends in <strong>2 days</strong>. Your meal plan, workouts and recovery protocol will pause unless you continue.</p>
        <div style="text-align:center;margin:28px 0">
          <a href="${FRONTEND_URL}" style="display:inline-block;background:#2D6A4F;color:#fff;font-size:15px;font-weight:700;padding:14px 28px;border-radius:12px;text-decoration:none">Continue my journey →</a>
        </div>
        <p style="color:#6B6B68;font-size:13px">To cancel: open PEAK → Settings → Subscription → Cancel. 10 seconds, no phone calls.</p>
        ${emailFooter(to)}</div>`
    },
    day6: {
      subject: '🔔 Last day of your free trial — tomorrow you\'ll be charged',
      html: `<div style="font-family:sans-serif;max-width:560px;margin:0 auto;padding:40px 20px">${logo}
        <div style="background:#FEF0EC;border-radius:12px;padding:14px 18px;margin-bottom:20px">
          <p style="margin:0;color:#C0392B;font-weight:600">⏰ Your free trial ends tomorrow</p>
        </div>
        <h1 style="font-size:26px;color:#1C1C1A">Your free trial ends tomorrow</h1>
        <p style="color:#6B6B68;line-height:1.7">This is your 24-hour notice. Tomorrow your PEAK subscription begins.</p>
        <p style="color:#6B6B68;line-height:1.7"><strong>To cancel:</strong> Open PEAK → Settings → Subscription → Cancel trial. Done in 10 seconds.</p>
        <div style="text-align:center;margin:28px 0">
          <a href="${FRONTEND_URL}" style="display:inline-block;background:#2D6A4F;color:#fff;font-size:15px;font-weight:700;padding:14px 28px;border-radius:12px;text-decoration:none">Keep my plan →</a>
        </div>
        ${emailFooter(to)}</div>`
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
