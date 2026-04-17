const express = require('express');
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);
const { createClient } = require('@supabase/supabase-js');
const { Resend } = require('resend');
const cron = require('node-cron');
const cors = require('cors');

const app = express();
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const resend = new Resend(process.env.RESEND_API_KEY);

const FRONTEND_URL = process.env.FRONTEND_URL || 'https://lambent-palmier-f397c7.netlify.app';

// ── CORS ──────────────────────────────────────────────────────────────
app.use(cors());

// Raw body for Stripe webhooks — must come before express.json()
app.use('/webhook', express.raw({ type: 'application/json' }));
app.use(express.json());

// ── HEALTH CHECK ──────────────────────────────────────────────────────
app.get('/', (req, res) => {
  res.json({ status: 'PEAK Backend running ✅', time: new Date().toISOString() });
});

// ── CREATE CHECKOUT SESSION ───────────────────────────────────────────
app.post('/create-checkout', async (req, res) => {
  try {
    const { email, plan, userData } = req.body;

    const priceId = plan === 'annual'
      ? process.env.STRIPE_PRICE_ANNUAL
      : process.env.STRIPE_PRICE_MONTHLY;

    const session = await stripe.checkout.sessions.create({
      mode: 'subscription',
      payment_method_types: ['card'],
      customer_email: email,
      line_items: [{ price: priceId, quantity: 1 }],
      subscription_data: {
        trial_period_days: 7,
        metadata: {
          userName: userData?.name || '',
          userGoal: userData?.goal || '',
          userSport: userData?.sport || '',
          plan: plan,
        }
      },
      success_url: `https://lambent-palmier-f397c7.netlify.app?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `https://lambent-palmier-f397c7.netlify.app?cancelled=true`,
      metadata: {
        userName: userData?.name || '',
        userGoal: userData?.goal || '',
        plan: plan,
      }
    });

    res.json({ url: session.url, sessionId: session.id });
  } catch (err) {
    console.error('Checkout error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── STRIPE WEBHOOK ────────────────────────────────────────────────────
app.post('/webhook', async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;

  try {
    event = stripe.webhooks.constructEvent(
      req.body,
      sig,
      process.env.STRIPE_WEBHOOK_SECRET
    );
  } catch (err) {
    console.error('Webhook signature failed:', err.message);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  switch (event.type) {

    case 'checkout.session.completed': {
      const session = event.data.object;
      const email = session.customer_email;
      const name = session.metadata?.userName || 'there';
      const trialStart = new Date();
      const trialEnd = new Date(trialStart.getTime() + 7 * 24 * 60 * 60 * 1000);

      // Save to Supabase
      await supabase.from('users').upsert({
        email,
        name,
        stripe_customer_id: session.customer,
        stripe_subscription_id: session.subscription,
        plan: session.metadata?.plan || 'monthly',
        goal: session.metadata?.userGoal || '',
        trial_start: trialStart.toISOString(),
        trial_end: trialEnd.toISOString(),
        status: 'trial',
        created_at: new Date().toISOString(),
      });

      // Send welcome email
      await sendEmail(email, 'welcome', { name });
      console.log(`✅ New trial started: ${email}`);
      break;
    }

    case 'customer.subscription.deleted': {
      const sub = event.data.object;
      const customer = await stripe.customers.retrieve(sub.customer);
      await supabase
        .from('users')
        .update({ status: 'cancelled' })
        .eq('email', customer.email);
      console.log(`❌ Subscription cancelled: ${customer.email}`);
      break;
    }

    case 'invoice.payment_succeeded': {
      const invoice = event.data.object;
      const customer = await stripe.customers.retrieve(invoice.customer);
      await supabase
        .from('users')
        .update({ status: 'active' })
        .eq('email', customer.email);
      console.log(`💳 Payment succeeded: ${customer.email}`);
      break;
    }
  }

  res.json({ received: true });
});

// ── EMAIL SENDER ──────────────────────────────────────────────────────
async function sendEmail(to, type, data) {
  const emails = {
    welcome: {
      subject: '🌱 Welcome to PEAK — Your plan is ready',
      html: `
        <div style="font-family:sans-serif;max-width:560px;margin:0 auto;padding:40px 20px">
          <h1 style="font-size:28px;color:#1C1C1A">Welcome to PEAK, ${data.name}! 🎉</h1>
          <p style="color:#6B6B68;line-height:1.7">Your personalised plan is live. This is the start of something real.</p>
          <div style="background:#EAF4EE;border-radius:12px;padding:20px;margin:24px 0">
            <p style="margin:0;font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;color:#2D6A4F">What's waiting for you</p>
            <ul style="color:#1C1C1A;line-height:2;margin-top:8px">
              <li>AI meal plan matched to your goal and taste</li>
              <li>Personalised workout programme for your sport</li>
              <li>Recovery protocol — sleep, hydration, stress</li>
              <li>Barcode scanner + live shopping mode</li>
            </ul>
          </div>
          <p style="color:#6B6B68"><strong>Your free trial runs 7 days.</strong> No charge until Day 8. We remind you on Day 5 and Day 6.</p>
          <a href="${FRONTEND_URL}" style="display:inline-block;background:#2D6A4F;color:#fff;padding:14px 28px;border-radius:12px;text-decoration:none;font-weight:700;margin:16px 0">Open my plan →</a>
          <p style="color:#BDBDB8;font-size:12px;margin-top:32px">You're receiving this because you signed up for PEAK.</p>
        </div>
      `
    },
    day5: {
      subject: '⏰ 2 days left on your free trial',
      html: `
        <div style="font-family:sans-serif;max-width:560px;margin:0 auto;padding:40px 20px">
          <h1 style="font-size:28px;color:#1C1C1A">${data.name}, your trial ends in 2 days</h1>
          <p style="color:#6B6B68;line-height:1.7">Your 7-day free trial ends in <strong>2 days</strong>. Everything you've built this week — your meal plan, workouts, and recovery protocol — will pause unless you continue.</p>
          <a href="${FRONTEND_URL}" style="display:inline-block;background:#2D6A4F;color:#fff;padding:14px 28px;border-radius:12px;text-decoration:none;font-weight:700;margin:16px 0">Continue my journey →</a>
          <p style="color:#6B6B68;font-size:13px">Want to cancel? Open PEAK → Settings → Subscription → Cancel trial. 10 seconds, no calls.</p>
          <p style="color:#BDBDB8;font-size:12px;margin-top:32px">You're receiving this because you signed up for PEAK.</p>
        </div>
      `
    },
    day6: {
      subject: '🔔 Last day of your free trial — tomorrow you\'ll be charged',
      html: `
        <div style="font-family:sans-serif;max-width:560px;margin:0 auto;padding:40px 20px">
          <h1 style="font-size:28px;color:#1C1C1A">Your free trial ends tomorrow</h1>
          <p style="color:#6B6B68;line-height:1.7">This is your 24-hour notice. Tomorrow your PEAK subscription begins.</p>
          <p style="color:#6B6B68;line-height:1.7">To cancel: open PEAK → Settings → Subscription → Cancel trial. Done in 10 seconds.</p>
          <p style="color:#6B6B68;line-height:1.7">If you're staying — you don't need to do anything. Your plan continues seamlessly.</p>
          <a href="${FRONTEND_URL}" style="display:inline-block;background:#2D6A4F;color:#fff;padding:14px 28px;border-radius:12px;text-decoration:none;font-weight:700;margin:16px 0">Keep my plan →</a>
          <p style="color:#BDBDB8;font-size:12px;margin-top:32px">You're receiving this because you signed up for PEAK.</p>
        </div>
      `
    }
  };

  const email = emails[type];
  if (!email) return;

  try {
    await resend.emails.send({
      from: 'PEAK <hello@mj-performance.net>',
      to,
      subject: email.subject,
      html: email.html,
    });
    console.log(`📧 ${type} email sent to ${to}`);
  } catch (err) {
    console.error(`Email error (${type}):`, err.message);
  }
}

// ── CRON JOBS — Trial Reminders ───────────────────────────────────────
// Runs every day at 10am UTC
cron.schedule('0 10 * * *', async () => {
  console.log('Running daily trial reminders...');

  const now = new Date();
  const { data: users } = await supabase
    .from('users')
    .select('*')
    .eq('status', 'trial');

  for (const user of users || []) {
    const trialEnd = new Date(user.trial_end);
    const daysLeft = Math.ceil((trialEnd - now) / (1000 * 60 * 60 * 24));

    if (daysLeft === 2) {
      await sendEmail(user.email, 'day5', { name: user.name });
    } else if (daysLeft === 1) {
      await sendEmail(user.email, 'day6', { name: user.name });
    }
  }
});

// ── START ─────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 PEAK Backend running on port ${PORT}`);
  console.log(`📡 Frontend: ${FRONTEND_URL}`);
});
