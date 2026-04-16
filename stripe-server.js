const cron = require('node-cron');
const { runDailyReminders } = require('./cron-reminders');

// Run trial reminders daily at 10am UTC
cron.schedule('0 10 * * *', () => {
  console.log('Running daily reminders...');
  runDailyReminders();
});
// ============================================================
// PEAK APP Ã¢â‚¬â€ STRIPE INTEGRATION SERVER
// Phase 1: Complete payment + webhook handler
// Deploy to: Railway, Render, or Supabase Edge Functions
// ============================================================

const express = require('express');
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);
const { createClient } = require('@supabase/supabase-js');
const { Resend } = require('resend');

const app = express();
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const resend = new Resend(process.env.RESEND_API_KEY);

// Raw body needed for Stripe webhook signature verification
app.use('/webhook', express.raw({ type: 'application/json' }));
app.use(express.json());

// Ã¢â€â‚¬Ã¢â€â‚¬ PRICE IDS (set these in your Stripe dashboard) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
const PRICES = {
  monthly: process.env.STRIPE_PRICE_MONTHLY,  // e.g. price_xxxxx
  annual:  process.env.STRIPE_PRICE_ANNUAL,   // e.g. price_xxxxx
};

// Ã¢â€â‚¬Ã¢â€â‚¬ CREATE CHECKOUT SESSION Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
// Called when user taps "Start free trial" in the app
app.post('/create-checkout', async (req, res) => {
  const { userId, email, planType, appReturnUrl } = req.body;

  try {
    // Create or retrieve Stripe customer
    let customerId;
    const { data: profile } = await supabase
      .from('profiles')
      .select('customer_id')
      .eq('id', userId)
      .single();

    if (profile?.customer_id) {
      customerId = profile.customer_id;
    } else {
      const customer = await stripe.customers.create({
        email,
        metadata: { supabase_user_id: userId },
      });
      customerId = customer.id;
      await supabase.from('profiles').update({ customer_id: customerId }).eq('id', userId);
    }

    // Create subscription with 7-day free trial
    const session = await stripe.checkout.sessions.create({
      customer: customerId,
      payment_method_types: ['card'],
      mode: 'subscription',
      line_items: [{
        price: planType === 'annual' ? PRICES.annual : PRICES.monthly,
        quantity: 1,
      }],
      subscription_data: {
        trial_period_days: 7,
        metadata: { supabase_user_id: userId, plan_type: planType },
      },
      success_url: `${appReturnUrl}?session_id={CHECKOUT_SESSION_ID}&status=success`,
      cancel_url: `${appReturnUrl}?status=cancelled`,
      metadata: { supabase_user_id: userId },
    });

    res.json({ sessionUrl: session.url, sessionId: session.id });
  } catch (err) {
    console.error('Checkout error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Ã¢â€â‚¬Ã¢â€â‚¬ STRIPE WEBHOOK HANDLER Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
// Handles all subscription lifecycle events
app.post('/webhook', async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;

  try {
    event = stripe.webhooks.constructEvent(req.body, sig, process.env.STRIPE_WEBHOOK_SECRET);
  } catch (err) {
    console.error('Webhook signature failed:', err.message);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  console.log(`Stripe event: ${event.type}`);

  switch (event.type) {

    // Ã¢â€â‚¬Ã¢â€â‚¬ TRIAL STARTED Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    case 'customer.subscription.created': {
      const sub = event.data.object;
      const userId = sub.metadata.supabase_user_id;
      const trialEnd = sub.trial_end ? new Date(sub.trial_end * 1000) : null;

      await supabase.from('profiles').update({
        subscription_id: sub.id,
        plan_type: sub.metadata.plan_type || 'pro',
        trial_started_at: new Date(sub.created * 1000).toISOString(),
        trial_ends_at: trialEnd?.toISOString(),
      }).eq('id', userId);

      // Send Day 1 welcome email
      await sendTrialEmail(userId, 'welcome');

      // Schedule Day 5 and Day 6 reminders via Supabase cron
      await scheduleTrialReminders(userId, trialEnd);
      break;
    }

    // Ã¢â€â‚¬Ã¢â€â‚¬ TRIAL WILL END SOON Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    case 'customer.subscription.trial_will_end': {
      // Stripe fires this 3 days before trial ends
      // We handle Day 5/6 via our own scheduler instead
      // but this is a good fallback
      const sub = event.data.object;
      const userId = sub.metadata.supabase_user_id;
      await sendTrialEmail(userId, 'day6');
      break;
    }

    // Ã¢â€â‚¬Ã¢â€â‚¬ SUBSCRIPTION ACTIVE (paid) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    case 'invoice.payment_succeeded': {
      const invoice = event.data.object;
      if (invoice.billing_reason === 'subscription_create') break; // Skip first invoice (trial)
      const sub = await stripe.subscriptions.retrieve(invoice.subscription);
      const userId = sub.metadata.supabase_user_id;
      await supabase.from('profiles').update({
        plan_type: sub.metadata.plan_type || 'pro',
        trial_ends_at: null,
      }).eq('id', userId);
      await sendReceiptEmail(userId, invoice);
      break;
    }

    // Ã¢â€â‚¬Ã¢â€â‚¬ PAYMENT FAILED Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    case 'invoice.payment_failed': {
      const invoice = event.data.object;
      const sub = await stripe.subscriptions.retrieve(invoice.subscription);
      const userId = sub.metadata.supabase_user_id;
      await supabase.from('profiles').update({ plan_type: 'free' }).eq('id', userId);
      await sendPaymentFailedEmail(userId);
      break;
    }

    // Ã¢â€â‚¬Ã¢â€â‚¬ SUBSCRIPTION CANCELLED Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    case 'customer.subscription.deleted': {
      const sub = event.data.object;
      const userId = sub.metadata.supabase_user_id;
      await supabase.from('profiles').update({
        plan_type: 'free',
        subscription_id: null,
      }).eq('id', userId);
      break;
    }

    // Ã¢â€â‚¬Ã¢â€â‚¬ SUBSCRIPTION UPDATED Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    case 'customer.subscription.updated': {
      const sub = event.data.object;
      const userId = sub.metadata.supabase_user_id;
      const isActive = ['active', 'trialing'].includes(sub.status);
      await supabase.from('profiles').update({
        plan_type: isActive ? (sub.metadata.plan_type || 'pro') : 'free',
      }).eq('id', userId);
      break;
    }
  }

  res.json({ received: true });
});

// Ã¢â€â‚¬Ã¢â€â‚¬ CUSTOMER PORTAL (manage/cancel subscription) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
app.post('/customer-portal', async (req, res) => {
  const { userId, returnUrl } = req.body;
  try {
    const { data: profile } = await supabase
      .from('profiles')
      .select('customer_id')
      .eq('id', userId)
      .single();

    const session = await stripe.billingPortal.sessions.create({
      customer: profile.customer_id,
      return_url: returnUrl,
    });
    res.json({ url: session.url });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Ã¢â€â‚¬Ã¢â€â‚¬ CHECK SUBSCRIPTION STATUS Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
app.get('/subscription/:userId', async (req, res) => {
  const { userId } = req.params;
  try {
    const { data: profile } = await supabase
      .from('profiles')
      .select('plan_type, trial_starts_at, trial_ends_at, subscription_id')
      .eq('id', userId)
      .single();

    const now = new Date();
    const trialEnd = profile.trial_ends_at ? new Date(profile.trial_ends_at) : null;
    const daysLeft = trialEnd ? Math.max(0, Math.ceil((trialEnd - now) / (1000 * 60 * 60 * 24))) : null;

    res.json({
      plan: profile.plan_type,
      isActive: ['pro', 'basic', 'annual'].includes(profile.plan_type),
      isTrialing: daysLeft !== null && daysLeft > 0,
      trialDaysLeft: daysLeft,
      trialEndsAt: profile.trial_ends_at,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Ã¢â€â‚¬Ã¢â€â‚¬ EMAIL HELPERS Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
async function getUserEmail(userId) {
  const { data } = await supabase.from('profiles').select('email, name').eq('id', userId).single();
  return data;
}

async function sendTrialEmail(userId, type) {
  const user = await getUserEmail(userId);
  if (!user?.email) return;

  const subjects = {
    welcome: 'Ã°Å¸Å’Â± Welcome to PEAK Ã¢â‚¬â€ Your plan is ready',
    day5:    'Ã¢ÂÂ° 2 days left on your free trial',
    day6:    'Ã°Å¸â€â€ Last day of your free trial Ã¢â‚¬â€ tomorrow you\'ll be charged',
  };

  const bodies = {
    welcome: `Hi ${user.name || 'there'},\n\nYour personalised PEAK plan is live! Your 7-day free trial has started.\n\nYou'll receive a reminder on Day 5 and Day 6 before any charge. Cancel anytime in the app under Settings Ã¢â€ â€™ Subscription.\n\nLet's get started. Ã°Å¸Å¡â‚¬\n\nThe PEAK Team`,
    day5: `Hi ${user.name || 'there'},\n\nYour 7-day free trial ends in 2 days.\n\nTo keep your personalised plan going, your subscription will begin on Day 8. To cancel: open PEAK Ã¢â€ â€™ Settings Ã¢â€ â€™ Subscription Ã¢â€ â€™ Cancel. Takes 10 seconds.\n\nThe PEAK Team`,
    day6: `Hi ${user.name || 'there'},\n\nThis is your 24-hour notice. Your PEAK subscription begins tomorrow.\n\nTo cancel: open PEAK Ã¢â€ â€™ Settings Ã¢â€ â€™ Subscription Ã¢â€ â€™ Cancel trial Ã¢â‚¬â€ before midnight tonight.\n\nIf you're staying, you don't need to do anything.\n\nThe PEAK Team`,
  };

  try {
    await resend.emails.send({
      from: 'PEAK <hello@peak.app>',
      to: user.email,
      subject: subjects[type],
      text: bodies[type],
    });

    await supabase.from('notification_log').insert({
      user_id: userId,
      type: `trial_${type}`,
      channel: 'email',
    });
  } catch (err) {
    console.error('Email send error:', err);
  }
}

async function sendReceiptEmail(userId, invoice) {
  const user = await getUserEmail(userId);
  if (!user?.email) return;
  await resend.emails.send({
    from: 'PEAK <hello@peak.app>',
    to: user.email,
    subject: 'Ã¢Å“â€¦ PEAK Ã¢â‚¬â€ Payment confirmed',
    text: `Hi ${user.name || 'there'},\n\nPayment of ${(invoice.amount_paid / 100).toFixed(2)} ${invoice.currency.toUpperCase()} confirmed. Your PEAK subscription is active.\n\nThe PEAK Team`,
  });
}

async function sendPaymentFailedEmail(userId) {
  const user = await getUserEmail(userId);
  if (!user?.email) return;
  await resend.emails.send({
    from: 'PEAK <hello@peak.app>',
    to: user.email,
    subject: 'Ã¢Å¡ Ã¯Â¸Â PEAK Ã¢â‚¬â€ Payment failed',
    text: `Hi ${user.name || 'there'},\n\nWe couldn't process your payment. Please update your payment method in the app under Settings Ã¢â€ â€™ Subscription.\n\nThe PEAK Team`,
  });
}

// Schedule trial Day 5 + Day 6 reminder emails via Supabase
async function scheduleTrialReminders(userId, trialEnd) {
  if (!trialEnd) return;
  // In production: use Supabase pg_cron, BullMQ, or Inngest
  // For simplicity, we rely on Stripe's trial_will_end webhook (fires 3 days before)
  // and also check daily via a cron job (see cron.js)
  console.log(`Reminders scheduled for user ${userId}, trial ends ${trialEnd}`);
}

// Ã¢â€â‚¬Ã¢â€â‚¬ HEALTH CHECK Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
app.get('/health', (req, res) => res.json({ status: 'ok', timestamp: new Date() }));

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`PEAK API running on port ${PORT}`));

module.exports = app;
