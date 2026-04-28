import express from 'express';
import { chromium } from 'playwright';
import fs, { readFileSync, readdirSync, statSync } from 'fs';
import path, { join } from 'path';
import { spawn, exec as execCb } from 'child_process';
import { promisify } from 'util';
import crypto from 'crypto';

const exec = promisify(execCb);

// ── Configuration ────────────────────────────────────────────────────────────
// ADS_URL: AdsPower local API (loopback-only on Windows host).
// WORKER_TOKENS: comma-separated bearer tokens, each prefixed role:token
//   (e.g. "admin:xxx,claims:yyy,bca:zzz"). Admin token can call /exec. Others
//   are limited to /submit, /status, /adspower/*.
// When absent in dev, auth is bypassed with a warning.
const ADS = process.env.ADS_URL ?? 'http://127.0.0.1:50325';
const OUT = process.env.SCREENSHOT_DIR ?? 'C:\\worker\\screenshots';
const PORT = Number(process.env.PORT ?? 7070);

const TOKEN_ENV = process.env.WORKER_TOKENS ?? '';
const TOKENS = new Map();
for (const entry of TOKEN_ENV.split(',').map((s) => s.trim()).filter(Boolean)) {
  const [role, token] = entry.split(':');
  if (role && token) TOKENS.set(token, role);
}
const AUTH_DISABLED = TOKENS.size === 0;

if (!fs.existsSync(OUT)) fs.mkdirSync(OUT, { recursive: true });

// ── Auth middleware ─────────────────────────────────────────────────────────
function auth(requiredRole = null) {
  return (req, res, next) => {
    if (AUTH_DISABLED) {
      req.role = 'admin';
      return next();
    }
    const header = req.headers.authorization ?? '';
    const token = header.startsWith('Bearer ') ? header.slice(7) : null;
    const role = token ? TOKENS.get(token) : null;
    if (!role) return res.status(401).json({ error: 'unauthorized' });
    if (requiredRole && role !== requiredRole) {
      return res.status(403).json({ error: `role '${requiredRole}' required, got '${role}'` });
    }
    req.role = role;
    next();
  };
}

// ── AdsPower helpers ────────────────────────────────────────────────────────
async function ads(pathname) {
  const r = await fetch(ADS + pathname);
  const j = await r.json();
  if (j.code !== 0) throw new Error('AdsPower: ' + j.msg);
  return j.data;
}

async function adsReachable() {
  try {
    const r = await fetch(`${ADS}/status`);
    return r.ok;
  } catch {
    return false;
  }
}

async function withSession(profileId, fn, useAdsPower = true) {
  if (!useAdsPower || !profileId) {
    const browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
    const ctx = await browser.newContext({ locale: 'en-US', timezoneId: 'America/New_York' });
    const page = await ctx.newPage();
    try { return await fn(page); } finally { await browser.close().catch(() => {}); }
  }
  const start = await ads(`/api/v1/browser/start?user_id=${profileId}`);
  const browser = await chromium.connectOverCDP(start.ws.puppeteer);
  const ctx = browser.contexts()[0] ?? (await browser.newContext());
  const page = ctx.pages()[0] ?? (await ctx.newPage());
  try {
    return await fn(page);
  } finally {
    await browser.close().catch(() => {});
    await fetch(`${ADS}/api/v1/browser/stop?user_id=${profileId}`).catch(() => {});
  }
}

// ── IRS EIN Online Assistant helpers ────────────────────────────────────────
// The IRS EIN Online Assistant (sa.www4.irs.gov/modiein) is only available
// Mon–Fri 07:00–22:00 Eastern Time. Per IRS, the "last accepted minute" is
// 21:59 ET — 22:00 itself is the close. Eastern Time flips between EST and
// EDT; Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York' }) handles
// DST for us. Pure helper: no Playwright, no fetch — safe to unit-test.
//
// Returns { open, reason, next_open_at } where next_open_at is an ISO UTC
// string (or null when already open).
function irsEinOnlineHoursOpen(now = new Date()) {
  // IRS EIN Online Assistant schedule (per irs.gov, current as of 2026-04):
  //   Mon–Fri: 6:00 a.m. – 1:00 a.m. (next day) ET
  //   Sat:     6:00 a.m. – 9:00 p.m.            ET
  //   Sun:     6:00 p.m. – 12:00 a.m.           ET
  //
  // Encoded as: at any (weekday, hour) point in ET, are we open right now?
  // Plus: next-open computation when closed, by stepping forward hour by hour
  // (cheap; the function is rarely called while closed).

  function etPartsAt(d) {
    const fmt = new Intl.DateTimeFormat('en-US', {
      timeZone: 'America/New_York',
      weekday: 'short',
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    });
    const parts = Object.fromEntries(fmt.formatToParts(d).map((p) => [p.type, p.value]));
    const weekdayMap = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
    let h = Number(parts.hour);
    if (h === 24) h = 0;
    return {
      weekday: weekdayMap[parts.weekday],
      hour: h,
      year: Number(parts.year),
      month: Number(parts.month),
      day: Number(parts.day),
    };
  }
  function isOpenAt(weekday, hour) {
    // Mon-Fri main window: 06:00–23:59 (continues into next day until 01:00).
    if (weekday >= 1 && weekday <= 5 && hour >= 6) return true;
    // Tue-Sat 00:00–00:59 — bleed-over from the previous Mon-Fri day.
    if (weekday >= 2 && weekday <= 6 && hour < 1) return true;
    // Saturday own window: 06:00–20:59 (closes at 21:00).
    if (weekday === 6 && hour >= 6 && hour < 21) return true;
    // Sunday own window: 18:00–23:59.
    if (weekday === 0 && hour >= 18) return true;
    return false;
  }

  const cur = etPartsAt(now);
  if (isOpenAt(cur.weekday, cur.hour)) {
    return { open: true, reason: 'open', next_open_at: null };
  }

  // Closed. Step forward hour by hour (max 168 = full week) to find next open.
  let probe = new Date(now);
  let nextOpen = null;
  for (let i = 0; i < 168; i++) {
    probe = new Date(probe.getTime() + 60 * 60 * 1000);
    const p = etPartsAt(probe);
    if (isOpenAt(p.weekday, p.hour)) { nextOpen = p; break; }
  }
  let next_open_at = null;
  if (nextOpen) {
    // Convert the ET (year, month, day, hour) wall-clock back to UTC.
    const naive = Date.UTC(nextOpen.year, nextOpen.month - 1, nextOpen.day, nextOpen.hour, 0, 0);
    const probeRound = etPartsAt(new Date(naive));
    const probed = Date.UTC(probeRound.year, probeRound.month - 1, probeRound.day, probeRound.hour, 0, 0);
    next_open_at = new Date(naive + (naive - probed)).toISOString();
  }

  // Human-friendly reason.
  const reasonMap = (() => {
    const wd = cur.weekday;
    const h = cur.hour;
    if (wd === 0) {
      if (h < 18) return 'Sunday before 18:00 ET — opens at 18:00 ET today';
      return 'Sunday after 24:00 ET'; // unreachable
    }
    if (wd === 6) {
      if (h >= 21) return 'Saturday after 21:00 ET — opens Sunday 18:00 ET';
      if (h >= 1 && h < 6) return 'Saturday before 06:00 ET — opens at 06:00 ET today';
      return 'Saturday closed';
    }
    // Mon-Fri here means 01:00–05:59 (closed gap)
    if (h >= 1 && h < 6) return `weekday early hours — opens at 06:00 ET today`;
    return 'closed';
  })();
  return { open: false, reason: reasonMap, next_open_at };
}

// ── IRS EIN form filler (shared by dryrun + submit) ────────────────────────
// Walks /applyein/ from start to either the Review page (stopAtReview=true)
// or all the way through Submit + EIN Assignment (stopAtReview=false).
//
// SELECTOR STATUS as of 2026-04-24 inspection:
//   * Step 1 (Legal Structure → LLC + Continue) — VERIFIED. Click the
//     <label> for Limited Liability Company (input click does not fire
//     React onChange). Continue is <a role="button" aria-label="Continue">.
//   * Steps 2 (Identity), 3 (Addresses), 4 (Additional Details), Review &
//     Submit, EIN Assignment — UNVERIFIED. Selectors below are educated
//     guesses against IRS EIN documentation and the same React pattern
//     (label-not-input clicks, aria-label="Continue"). They MUST be tuned
//     against a Monday inspect run before live use. TODO markers flag the
//     uncertain selectors.
//
// Payload contract (matches IrsEinDryRunRequest in @spexx/browser-worker):
//   {
//     responsibleParty: {
//       first_name, middle_initial?, last_name, suffix?, ssn_or_itin, title?
//     }
//     business: {
//       legal_name, trade_name?, mailing_address: {street, street2?, city, state, zip},
//       physical_address?, county, state_of_formation, llc_members_count,
//       formation_date (yyyy-mm-dd), reason_for_applying, accounting_year_close_month,
//       expects_employees, naics_code?, business_purpose?, phone?
//     }
//   }
async function fillIrsEinForm(page, payload, { stopAtReview, outDir, tag }) {
  const steps_visited = [];
  const safeLabel = (s) => String(s).replace(/[^a-zA-Z0-9_-]+/g, '_').slice(0, 60);
  async function captureStep(label) {
    const idx = steps_visited.length;
    const file = path.join(outDir, `step-${idx}-${safeLabel(label)}.png`);
    try { await page.screenshot({ path: file, fullPage: true }); } catch { /* ignore */ }
    steps_visited.push({ step_label: label, url: page.url(), screenshot_path: file });
  }
  async function settle() {
    await page.waitForLoadState('domcontentloaded', { timeout: 20000 }).catch(() => {});
    await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(() => {});
    await page.waitForTimeout(1500);
  }
  async function firstVisible(selectors, perTimeout = 3000) {
    for (const sel of selectors) {
      const loc = page.locator(sel).first();
      try { await loc.waitFor({ state: 'visible', timeout: perTimeout }); return loc; }
      catch { /* try next */ }
    }
    return null;
  }
  async function clickContinue(stepName) {
    const btn = await firstVisible([
      'a[aria-label="Continue"]',
      'a[role="button"][aria-label*="Continue" i]',
      'button[aria-label="Continue"]',
      'a:has-text("Continue")',
    ], 8000);
    if (!btn) throw new Error(`fillIrsEinForm[${stepName}]: Continue button not found`);
    await btn.click({ timeout: 8000 });
    await settle();
  }
  // Verify a step has advanced by checking the IRS step indicator.
  // Body text shows "<step> active" for the current step. After advance the
  // current step's "active" goes away and the next step's appears.
  async function waitForStepActive(stepLabel, timeoutMs = 12000) {
    const re = new RegExp(`\\b${stepLabel.replace(/\s+/g, '\\s+')}\\s+active\\b`, 'i');
    const ok = await page.waitForFunction((reSrc) => {
      const t = document.body?.innerText || '';
      return new RegExp(reSrc, 'i').test(t);
    }, re.source, { timeout: timeoutMs }).then(() => true).catch(() => false);
    return ok;
  }

  // Step 0: open the assistant.
  await page.goto('https://sa.www4.irs.gov/applyein/', { waitUntil: 'domcontentloaded', timeout: 45000 });
  await settle();
  await captureStep('assistant-start');
  // Click "Begin Application" to enter the wizard.
  const beginBtn = await firstVisible([
    'a[aria-label="Begin Application"]',
    'a[role="button"]:has-text("Begin Application")',
    'a:has-text("Begin Application")',
    'input[type="submit"][value*="Begin" i]',
    'button:has-text("Begin")',
  ], 8000);
  if (!beginBtn) throw new Error('fillIrsEinForm: Begin Application not found');
  await beginBtn.click();
  await settle();

  // ── Step 1: Legal Structure ──────────────────────────────────────────────
  // Single-page progressive disclosure: clicking LLC reveals member-count +
  // state; filling those reveals reason-for-applying; only then does Continue
  // advance to Step 2. All selectors below are VERIFIED against live IRS
  // 2026-04-25 inspect runs.
  if (!await waitForStepActive('Legal Structure', 15000)) {
    throw new Error('fillIrsEinForm: did not land on Legal Structure step');
  }
  // 1a. Click LLC label (input click does NOT fire React onChange).
  const llcLabel = await firstVisible([
    'label[for="LLClegalStructureInputid"]',
    'label:has-text("Limited Liability Company (LLC)")',
    'label[for^="LLClegalStructure" i]',
  ], 8000);
  if (!llcLabel) throw new Error('fillIrsEinForm[step1]: LLC label not found');
  await llcLabel.click();
  const llcRadio = page.locator('input[type="radio"][value="LLC"]').first();
  await llcRadio.check({ force: true, timeout: 3000 }).catch(() => {});
  await page.waitForTimeout(800);
  if (!await llcRadio.isChecked().catch(() => false)) {
    throw new Error('fillIrsEinForm[step1]: LLC radio did not toggle');
  }

  // 1b. Members of LLC (text input).
  const memberCount = Number(payload.business?.llc_members_count ?? 1);
  if (!Number.isFinite(memberCount) || memberCount < 1) {
    throw new Error('fillIrsEinForm[step1]: business.llc_members_count must be >= 1');
  }
  const membersInput = await firstVisible(['input[name="membersOfLlcInput"]', 'input#membersOfLlcInput'], 6000);
  if (!membersInput) throw new Error('fillIrsEinForm[step1]: membersOfLlcInput not found');
  await membersInput.fill(String(memberCount));

  // 1c. State of physical location (select).
  const step1StateSelect = await firstVisible(['select[name="stateInputControl"]', 'select#stateInputControl'], 6000);
  if (!step1StateSelect) throw new Error('fillIrsEinForm[step1]: stateInputControl not found');
  const physicalState = payload.business?.mailing_address?.state || payload.business?.state_of_formation || 'WY';
  await step1StateSelect.selectOption(physicalState).catch(async () => {
    const stateNames = { WY: 'Wyoming', DE: 'Delaware', FL: 'Florida', CA: 'California', NY: 'New York', TX: 'Texas' };
    const fullName = stateNames[physicalState] || physicalState;
    await step1StateSelect.selectOption({ label: fullName });
  });
  await page.waitForTimeout(800);

  // 1d. Reason for applying (radio). Map app reason codes to IRS option ids.
  // Verified IRS option ids: NEW_BUSINESS, HIRED_EMPLOYEES, BANKING_NEEDS,
  // CHANGING_LEGAL_STRUCTURE, PURCHASED_BUSINESS.
  const reasonMap = {
    started_new_business: 'NEW_BUSINESS',
    'started-new-business': 'NEW_BUSINESS',
    hired_employees: 'HIRED_EMPLOYEES',
    'hired-employees': 'HIRED_EMPLOYEES',
    banking_purposes: 'BANKING_NEEDS',
    'banking-purposes': 'BANKING_NEEDS',
    changed_organization: 'CHANGING_LEGAL_STRUCTURE',
    purchased_business: 'PURCHASED_BUSINESS',
  };
  const reasonCode = reasonMap[payload.business?.reason_for_applying] || 'NEW_BUSINESS';
  const reasonLabel = await firstVisible([
    `label[for="${reasonCode}reasonForApplyingInputControlid"]`,
    `label[for^="${reasonCode}reasonForApplying" i]`,
  ], 4000);
  const reasonRadio = await firstVisible([
    `input[type="radio"][value="${reasonCode}"]`,
    `input[type="radio"][id^="${reasonCode}reasonForApplying" i]`,
  ], 4000);
  if (reasonLabel) await reasonLabel.click().catch(() => {});
  if (reasonRadio) await reasonRadio.check({ force: true, timeout: 3000 }).catch(() => {});
  await page.waitForTimeout(600);

  await captureStep('step1-llc-filled');
  await clickContinue('step1');
  if (!await waitForStepActive('Identity', 15000)) {
    throw new Error('fillIrsEinForm[step1→step2]: did not advance to Identity step');
  }

  // ── Step 2: Identity (responsible party) ─────────────────────────────────
  // Verified selectors from 2026-04-25 inspect:
  //   responsibleSsn, responsibleFirstName, responsibleMiddleName,
  //   responsibleLastName, responsibleSuffix (select),
  //   entityRoleRadioInput (yesentityRoleRadioInputid / no...).
  await captureStep('step2-identity-arrived');

  await page.locator('input[name="responsibleFirstName"]').fill(payload.responsibleParty.first_name);
  await page.locator('input[name="responsibleLastName"]').fill(payload.responsibleParty.last_name);
  if (payload.responsibleParty.middle_initial) {
    await page.locator('input[name="responsibleMiddleName"]').fill(payload.responsibleParty.middle_initial).catch(() => {});
  }
  if (payload.responsibleParty.suffix) {
    await page.locator('select[name="responsibleSuffix"]').selectOption(payload.responsibleParty.suffix).catch(() => {});
  }
  const ssnDigits = String(payload.responsibleParty.ssn_or_itin).replace(/\D/g, '');
  if (ssnDigits.length !== 9) throw new Error('fillIrsEinForm[step2]: SSN must be 9 digits');
  await page.locator('input[name="responsibleSsn"]').fill(ssnDigits);

  // Role: "I am one of the owners, members, or the managing member of this LLC."
  const ownerLabel = await firstVisible([
    'label[for="yesentityRoleRadioInputid"]',
    'label:has-text("one of the owners, members, or the managing member")',
  ], 4000);
  const ownerRadio = await firstVisible([
    'input[type="radio"][id="yesentityRoleRadioInputid"]',
    'input[type="radio"][name="entityRoleRadioInput"][value="yes"]',
  ], 4000);
  if (ownerLabel) await ownerLabel.click().catch(() => {});
  if (ownerRadio) await ownerRadio.check({ force: true, timeout: 3000 }).catch(() => {});

  await page.waitForTimeout(600);
  await captureStep('step2-identity-filled');
  await clickContinue('step2');
  if (!await waitForStepActive('Addresses', 15000)) {
    throw new Error('fillIrsEinForm[step2→step3]: did not advance to Addresses step');
  }

  // ── Step 3: Addresses ─────────────────────────────────────────────────────
  // Verified selectors from 2026-04-25 dryrun #4 form_fields dump:
  //   physicalStreet, physicalCity, physicalZipCode (text)
  //   physicalState (select, 2-letter codes)
  //   thePhone (text, digits only — IRS hint: "Must contain only digits")
  //   otherAddress radio: yesotherAddressid / nootherAddressid
  //     ("Do you have an address different from the above where you want
  //      your mail to be sent?")
  await captureStep('step3-addresses-arrived');

  const addr = payload.business.mailing_address;
  await page.locator('input[name="physicalStreet"]').fill(addr.street);
  await page.locator('input[name="physicalCity"]').fill(addr.city);
  await page.locator('input[name="physicalZipCode"]').fill(addr.zip);
  await page.locator('select[name="physicalState"]').selectOption(addr.state).catch(async () => {
    const stateNames = { WY: 'Wyoming', DE: 'Delaware', FL: 'Florida', CA: 'California', NY: 'New York', TX: 'Texas' };
    const fullName = stateNames[addr.state] || addr.state;
    await page.locator('select[name="physicalState"]').selectOption({ label: fullName });
  });

  // IRS phone hint: "Must contain only digits; do not enter extensions".
  if (payload.business.phone) {
    const phoneDigits = String(payload.business.phone).replace(/\D/g, '');
    await page.locator('input[name="thePhone"]').fill(phoneDigits).catch(() => {});
  }

  // Mailing-different radio. payload.business.physical_address is set when
  // the operator wants a separate mailing address; absence => same as above.
  const useDifferentMailing = !!payload.business?.physical_address;
  const otherTarget = useDifferentMailing ? 'yes' : 'no';
  const otherLabel = await firstVisible([
    `label[for="${otherTarget}otherAddressid"]`,
  ], 4000);
  if (otherLabel) await otherLabel.click().catch(() => {});
  await page.locator(`input[type="radio"][id="${otherTarget}otherAddressid"]`).check({ force: true, timeout: 3000 }).catch(() => {});
  await page.waitForTimeout(500);

  await captureStep('step3-addresses-filled');
  await clickContinue('step3');
  if (!await waitForStepActive('Additional Details', 15000)) {
    // Diagnostic: dump form_fields so we see which radio/select wasn't toggled.
    const ff = await page.evaluate(() => {
      const out = [];
      document.querySelectorAll('input, select').forEach((el) => {
        const t = el.getAttribute('type') || el.tagName.toLowerCase();
        if (t === 'hidden' || t === 'submit' || t === 'button') return;
        const lab = el.id ? document.querySelector(`label[for="${el.id.replace(/"/g, '\"')}"]`)?.textContent?.trim().slice(0, 100) : null;
        out.push({ tag: el.tagName.toLowerCase(), name: el.getAttribute('name'), id: el.getAttribute('id'), type: t, value: el.value, checked: el.checked, label: lab });
      });
      return out;
    }).catch(() => []);
    const errBanner = await page.evaluate(() => {
      const e = document.querySelector('.input-error-message, [class*="error"]');
      return e?.textContent?.trim().slice(0, 300) || null;
    }).catch(() => null);
    throw new Error(`fillIrsEinForm[step3→step4]: did not advance. err_banner=${errBanner} fields=${JSON.stringify(ff).slice(0, 2000)}`);
  }

  // ── Step 4: LLC Details + tax-obligation Yes/No screening ────────────────
  // VERIFIED selectors from 2026-04-25 dryrun #6 form_fields dump.
  // This page is misleadingly titled "Additional Details" but it actually
  // collects: legal name (re-confirm), DBA, county, state, state-of-articles,
  // start month/year, and 5 Yes/No tax-obligation screening questions.
  // NAICS / accounting close month / reason for applying live on Step 5+
  // (a separate page rendered AFTER this one — handled below).
  await captureStep('step4-additional-arrived');

  // 4a. Legal name (re-confirm) + optional DBA.
  await page.locator('input[name="legalNameInput"]').fill(payload.business.legal_name).catch(() => {});
  if (payload.business.trade_name) {
    await page.locator('input[name="dbaNameInput"]').fill(payload.business.trade_name).catch(() => {});
  }

  // 4b. County of LLC's location (text).
  await page.locator('input[name="countyInput"]').fill(payload.business.county || 'Sheridan');

  // 4c. State of LLC + State where Articles of Organization were filed.
  const stateNames = { WY: 'Wyoming', DE: 'Delaware', FL: 'Florida', CA: 'California', NY: 'New York', TX: 'Texas' };
  const stateCode = payload.business.state_of_formation || payload.business.mailing_address.state || 'WY';
  for (const sel of ['stateInput', 'StateFiledArticlesOrganizationInput']) {
    await page.locator(`select[name="${sel}"]`).selectOption(stateCode).catch(async () => {
      await page.locator(`select[name="${sel}"]`).selectOption({ label: stateNames[stateCode] || stateCode });
    });
  }

  // 4d. Business start month + year. The form filed BCAX 2026-04-23.
  const formedOn = new Date(payload.business.formation_date);
  const monthNum = formedOn.getUTCMonth() + 1;
  const yearStr = String(formedOn.getUTCFullYear());
  const monthLabels = ['', 'January', 'February', 'March', 'April', 'May', 'June',
                       'July', 'August', 'September', 'October', 'November', 'December'];
  await page.locator('select[name="startDateMonthInput"]').selectOption(String(monthNum)).catch(async () => {
    await page.locator('select[name="startDateMonthInput"]').selectOption({ label: monthLabels[monthNum] });
  });
  await page.locator('input[name="startDateYearInput"]').fill(yearStr);

  // 4e. Tax-obligation Yes/No radios. For BCAX (a vanilla management
  // consulting / holding entity with no employees) the answer is No to all.
  // Caller can override via payload.business.tax_obligations.
  const taxFlags = {
    highwayVehiclesInput: payload.business?.tax_obligations?.highway_vehicles === true,
    gamblingWagerInput:   payload.business?.tax_obligations?.gambling_wagers === true,
    fileForm720Input:     payload.business?.tax_obligations?.file_form_720 === true,
    atfInput:             payload.business?.tax_obligations?.atf === true,
    hasEmployeesInput:    payload.business?.expects_employees === true,
  };
  for (const [field, isYes] of Object.entries(taxFlags)) {
    const target = isYes ? 'yes' : 'no';
    const lab = await firstVisible([`label[for="${target}${field}id"]`], 3000);
    if (lab) await lab.click().catch(() => {});
    await page.locator(`input[type="radio"][id="${target}${field}id"]`).check({ force: true, timeout: 2500 }).catch(() => {});
    await page.waitForTimeout(120);
  }

  await captureStep('step4-additional-filled');
  await clickContinue('step4');

  // ── Step 4b: Business Category (NAICS-lite) ──────────────────────────────
  // Verified from dryrun #7: a single-page radio with 15 hardcoded categories:
  //   ACCOMMODATIONS, CONSTRUCTION, FINANCE, FOOD_SERVICE, HEALTH_CARE,
  //   INSURANCE, MANUFACTURING, REAL_ESTATE, RENTAL_LEASING, RETAIL,
  //   SOCIAL_ASSISTANCE, TRANSPORTATION, WAREHOUSING, WHOLESALE, OTHER.
  // For BCAX (management consulting + holding) → OTHER. The caller can
  // pass a different mapping via payload.business.irs_category.
  const irsCategory = (payload.business?.irs_category || 'OTHER').toUpperCase();
  const catLabel = await firstVisible([
    `label[for="${irsCategory}entityBusinessCategoryInputid"]`,
  ], 6000);
  if (catLabel) await catLabel.click().catch(() => {});
  await page.locator(`input[type="radio"][id="${irsCategory}entityBusinessCategoryInputid"]`).check({ force: true, timeout: 3000 }).catch(() => {});
  await page.waitForTimeout(1200);

  // When OTHER is selected, a sub-question "Tell us more about your Other
  // activities" appears with options:
  //   CONSULTING, MANUFACTURING, ORGANIZATION (religious/civic etc),
  //   RENTAL, REPAIR, GOODS_SELLER, SERVICE, OTHER.
  // For BCAX (management consulting + holding) → CONSULTING.
  if (irsCategory === 'OTHER') {
    const subKey = (payload.business?.irs_other_activity || 'CONSULTING').toUpperCase();
    const subLabel = await firstVisible([
      `label[for="${subKey}otherInputid"]`,
    ], 4000);
    if (subLabel) await subLabel.click().catch(() => {});
    await page.locator(`input[type="radio"][id="${subKey}otherInputid"]`).check({ force: true, timeout: 3000 }).catch(() => {});
    await page.waitForTimeout(800);

    // When CONSULTING is selected, IRS asks one more nested Yes/No:
    //   "Do you provide operating advice and assistance to businesses and
    //    other organizations?" (name=otherConsultingRadioInput).
    // For management consulting + holding: YES.
    if (subKey === 'CONSULTING') {
      const consultsYes = payload.business?.provides_operating_advice !== false;
      const target = consultsYes ? 'yes' : 'no';
      const lab = await firstVisible([
        `label[for="${target}otherConsultingRadioInputid"]`,
      ], 4000);
      if (lab) await lab.click().catch(() => {});
      await page.locator(`input[type="radio"][id="${target}otherConsultingRadioInputid"]`).check({ force: true, timeout: 3000 }).catch(() => {});
      await page.waitForTimeout(800);

      // Yes opens a free-text "What type of consulting?" field
      // (otherConsultingTypeTextInput). Fill with a sensible default for
      // BCAX-style management consulting; caller can override.
      if (consultsYes) {
        const consultingType = payload.business?.consulting_type || 'Management consulting';
        await page.locator('input[name="otherConsultingTypeTextInput"]').fill(consultingType).catch(() => {});
        await page.waitForTimeout(400);
      }
    }
  }

  await captureStep('step4b-category-selected');
  await clickContinue('step4b');
  await page.waitForTimeout(2000);
  // The next sub-page may ask for a free-text description when OTHER is
  // selected; handler in the next-page block.

  if (!await waitForStepActive('Review.{0,3}Submit', 15000)) {
    const ff = await page.evaluate(() => {
      const out = [];
      document.querySelectorAll('input, select, textarea').forEach((el) => {
        const t = el.getAttribute('type') || el.tagName.toLowerCase();
        if (t === 'hidden' || t === 'submit' || t === 'button') return;
        const lab = el.id ? document.querySelector(`label[for="${el.id.replace(/"/g, '\"')}"]`)?.textContent?.trim().slice(0, 100) : null;
        out.push({ tag: el.tagName.toLowerCase(), name: el.getAttribute('name'), id: el.getAttribute('id'), type: t, value: el.value, checked: el.checked, label: lab });
      });
      return out;
    }).catch(() => []);
    const errBanner = await page.evaluate(() => {
      const e = document.querySelector('.input-error-message, [class*="error-message"]');
      return e?.textContent?.trim().slice(0, 300) || null;
    }).catch(() => null);
    throw new Error(`fillIrsEinForm[step4→review]: did not advance. err_banner=${errBanner} fields=${JSON.stringify(ff).slice(0, 10000)}`);
  }

  // ── Review & Submit ───────────────────────────────────────────────────────
  await captureStep('review-page');
  // Capture a structured snapshot of the review for the operator to verify.
  const review_snapshot = await page.evaluate(() => {
    const out = {};
    // IRS review page typically lists field-label / field-value pairs in dl
    // or table layout. Grab them best-effort.
    document.querySelectorAll('dl, table').forEach((node) => {
      const pairs = [];
      const dts = node.querySelectorAll('dt, th');
      const dds = node.querySelectorAll('dd, td');
      const len = Math.min(dts.length, dds.length);
      for (let i = 0; i < len; i++) {
        const k = (dts[i].textContent || '').trim().slice(0, 100);
        const v = (dds[i].textContent || '').trim().slice(0, 200);
        if (k && v) pairs.push([k, v]);
      }
      if (pairs.length > 0) {
        out[node.tagName.toLowerCase() + '_' + (Object.keys(out).length)] = pairs;
      }
    });
    return out;
  }).catch(() => ({}));

  // Pick EIN Confirmation Letter delivery method on the Review page —
  // do this BEFORE stopAtReview so the dryrun verifies the click worked
  // (review_form_fields should show DIGITAL checked=True).
  // Verified selectors from dryrun #13:
  //   name="confirmationLetterRadioInput"
  //   id="DIGITALconfirmationLetterRadioInputid" / "MAILconfirmationLetterRadioInputid"
  {
    const wantMail = payload.business?.deliver_letter_by_mail === true;
    const target = wantMail ? 'MAIL' : 'DIGITAL';
    const lab = await firstVisible([`label[for="${target}confirmationLetterRadioInputid"]`], 4000);
    if (lab) await lab.click().catch(() => {});
    await page.locator(`input[type="radio"][id="${target}confirmationLetterRadioInputid"]`).check({ force: true, timeout: 3000 }).catch(() => {});
    await page.waitForTimeout(500);
  }

  if (stopAtReview) {
    // Dump form_fields on the Review page so callers can see the
    // delivery-method radio (digital vs mail) name before submit.
    const review_form_fields = await page.evaluate(() => {
      const out = [];
      document.querySelectorAll('input, select, textarea').forEach((el) => {
        const t = el.getAttribute('type') || el.tagName.toLowerCase();
        if (t === 'hidden' || t === 'submit' || t === 'button') return;
        const lab = el.id ? document.querySelector(`label[for="${el.id.replace(/"/g, '\"')}"]`)?.textContent?.trim().slice(0, 200) : null;
        out.push({ tag: el.tagName.toLowerCase(), name: el.getAttribute('name'), id: el.getAttribute('id'), type: t, value: el.value, checked: el.checked, label: lab });
      });
      return out;
    }).catch(() => []);
    return {
      phase: 'review',
      url: page.url(),
      steps_visited,
      review_snapshot,
      review_form_fields,
      review_screenshot_path: steps_visited[steps_visited.length - 1]?.screenshot_path,
    };
  }

  // ── Final Submit ──────────────────────────────────────────────────────────
  // Delivery-method radio was already picked above (before stopAtReview),
  // so dryrun verified it. Just click Submit EIN Request now.
  // Submit EIN Request button. The IRS uses an <a role="button"> here
  // (visible label "Submit EIN Request"); aria-label may differ.
  const submitBtn = await firstVisible([
    'a[aria-label="Submit EIN Request"]',
    'a[aria-label*="Submit EIN" i]',
    'a:has-text("Submit EIN Request")',
    'button:has-text("Submit EIN Request")',
    'a[aria-label="Submit"]',
    'a[aria-label*="Submit" i]',
    'button[aria-label="Submit"]',
    'a:has-text("Submit")',
  ], 8000);
  if (!submitBtn) throw new Error('fillIrsEinForm[review→submit]: Submit button not found');
  await submitBtn.click({ timeout: 8000 });
  await settle();
  if (!await waitForStepActive('EIN Assignment', 30000)) {
    throw new Error('fillIrsEinForm[submit→ein]: did not reach EIN Assignment page (submit may have failed)');
  }
  // High-resolution screenshot of the EIN Assignment page — works as visual
  // proof of the EIN even if PDF capture fails downstream.
  const einAssignmentShot = path.join(outDir, `step-${steps_visited.length}-ein-assignment.png`);
  for (let attempt = 0; attempt < 3; attempt++) {
    try { await page.screenshot({ path: einAssignmentShot, fullPage: true }); break; }
    catch (err) {
      console.warn(`[fillIrsEinForm:ein-assignment] screenshot attempt ${attempt} failed: ${err.message}`);
      await page.waitForTimeout(500);
    }
  }
  steps_visited.push({ step_label: 'ein-assignment', url: page.url(), screenshot_path: einAssignmentShot });

  // Extract the EIN string from the page (XX-XXXXXXX format).
  const ein = await page.evaluate(() => {
    const t = document.body?.innerText || '';
    const m = t.match(/\b(\d{2}-\d{7})\b/);
    return m ? m[1] : null;
  }).catch(() => null);
  if (!ein) throw new Error('fillIrsEinForm: EIN number not extractable from EIN Assignment page');

  // ── CP 575 PDF capture — bulletproofed multi-strategy ────────────────────
  // The IRS confirmation letter is delivered via one of three patterns
  // (varies across browsers / IRS deployments / day):
  //   1. <a href="...pdf"> — direct anchor with PDF href
  //   2. <a onclick="..."> — JS-triggered, fires download event
  //   3. <a target="_blank"> — opens PDF in a new tab/popup
  // We listen for ALL THREE before clicking, then pick whichever fires.
  // Always dump the page state for postmortem so we never lose info.
  let cp575_pdf_base64 = null;
  const ein_assignment_dump = await page.evaluate(() => {
    const out = { anchors: [], buttons: [], onclicks: [], body_text_excerpt: '' };
    document.querySelectorAll('a').forEach((a) => {
      const text = (a.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 120);
      if (!text && !a.getAttribute('href')) return;
      out.anchors.push({
        href: a.getAttribute('href'),
        text,
        ariaLabel: a.getAttribute('aria-label'),
        onclick: a.getAttribute('onclick'),
        target: a.getAttribute('target'),
        id: a.id || null,
        class: ((a.getAttribute('class') || '').slice(0, 80)) || null,
      });
    });
    document.querySelectorAll('button').forEach((b) => {
      const text = (b.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 120);
      if (!text) return;
      out.buttons.push({
        text,
        ariaLabel: b.getAttribute('aria-label'),
        onclick: b.getAttribute('onclick'),
        id: b.id || null,
      });
    });
    out.body_text_excerpt = (document.body?.innerText || '').slice(0, 4000);
    return out;
  }).catch(() => ({ anchors: [], buttons: [], onclicks: [], body_text_excerpt: '' }));

  // Persist the dump as JSON sidecar so it's recoverable even if the response
  // is truncated or lost in transit.
  try {
    fs.writeFileSync(path.join(outDir, 'ein-assignment-dump.json'), JSON.stringify(ein_assignment_dump, null, 2));
  } catch { /* ignore */ }

  try {
    // Multi-selector strategy. Order: most-specific to most-generic. The
    // 2026-04-25 BCAX run found that none of the original 4 selectors
    // matched — hence the much broader list now.
    const candidates = [
      'a[aria-label*="confirmation letter" i]',
      'a[aria-label*="EIN confirmation" i]',
      'a[aria-label*="Click here" i]',
      'a:has-text("CLICK HERE for Your EIN Confirmation Letter (PDF)")',
      'a:has-text("Click here for your EIN Confirmation Letter")',
      'a:has-text("CP 575")',
      'a:has-text("Confirmation Letter")',
      'a:has-text("Download your EIN")',
      'a:has-text("Click here")',
      'button:has-text("Confirmation Letter")',
      'button:has-text("Download")',
      'a[href*=".pdf" i]',
      'a[target="_blank"][href]',
      // last-ditch: any anchor whose text contains "letter"
      'a:has-text("letter")',
    ];
    const cp575Link = await firstVisible(candidates, 8000);
    if (cp575Link) {
      // Set up download AND popup listeners BEFORE click — IRS may use
      // either pattern depending on browser config.
      const downloadPromise = page.waitForEvent('download', { timeout: 18000 }).catch(() => null);
      const popupPromise = page.context().waitForEvent('page', { timeout: 12000 }).catch(() => null);

      // Try clicking; any of the three sources may produce the PDF.
      await cp575Link.click({ timeout: 5000 }).catch(() => {});

      // 1. Direct download event
      const download = await downloadPromise;
      if (download) {
        const dlUrl = download.url();
        await download.cancel().catch(() => {});
        if (dlUrl) {
          try {
            const resp = await page.request.get(dlUrl);
            if (resp.ok()) {
              const buf = Buffer.from(await resp.body());
              if (buf.slice(0, 5).toString() === '%PDF-') {
                cp575_pdf_base64 = buf.toString('base64');
              }
            }
          } catch (err) { console.warn(`[cp575] refetch failed: ${err.message}`); }
        }
      }

      // 2. Popup/new-tab pattern
      if (!cp575_pdf_base64) {
        const popup = await popupPromise;
        if (popup) {
          await popup.waitForLoadState('domcontentloaded', { timeout: 8000 }).catch(() => {});
          const popupUrl = popup.url();
          if (popupUrl && popupUrl !== 'about:blank') {
            try {
              const resp = await page.request.get(popupUrl);
              if (resp.ok()) {
                const buf = Buffer.from(await resp.body());
                if (buf.slice(0, 5).toString() === '%PDF-') {
                  cp575_pdf_base64 = buf.toString('base64');
                }
              }
            } catch (err) { console.warn(`[cp575] popup refetch failed: ${err.message}`); }
          }
          await popup.close().catch(() => {});
        }
      }

      // 3. Direct href fallback
      if (!cp575_pdf_base64) {
        const href = await cp575Link.getAttribute('href').catch(() => null);
        if (href && href !== '#' && !href.startsWith('javascript:')) {
          try {
            const url = href.startsWith('http') ? href : new URL(href, page.url()).toString();
            const resp = await page.request.get(url);
            if (resp.ok()) {
              const buf = Buffer.from(await resp.body());
              if (buf.slice(0, 5).toString() === '%PDF-') {
                cp575_pdf_base64 = buf.toString('base64');
              }
            }
          } catch (err) { console.warn(`[cp575] href fallback failed: ${err.message}`); }
        }
      }
    } else {
      console.warn('[cp575] no candidate link/button matched on EIN Assignment page');
    }
  } catch (err) {
    console.warn(`[fillIrsEinForm] CP 575 download failed: ${err.message}`);
  }

  // Persist the PDF locally too (defense-in-depth — if response transmission
  // drops the base64, we have a copy on disk to recover).
  if (cp575_pdf_base64) {
    try {
      fs.writeFileSync(path.join(outDir, 'cp575.pdf'), Buffer.from(cp575_pdf_base64, 'base64'));
    } catch { /* ignore */ }
  }

  return {
    phase: 'submitted',
    url: page.url(),
    steps_visited,
    ein,
    cp575_pdf_base64,
    ein_assignment_dump,  // always returned — operator can use this to manually retrieve PDF if automated capture failed
  };
}

// ── wyreg helpers ───────────────────────────────────────────────────────────
// Used by wyreg_poll_docs / wyreg_inspect_docs. On AdsPower the Keycloak session
// persists across runs, so ensureLoggedIn is a single-shot check instead of the
// retry-on-bounce loop the app used when it was juggling a cookies.json file.
const WYREG_ACCOUNTS_BASE = 'https://accounts.wyregisteredagent.net';
const WYREG_KEYCLOAK_URL_RE = /id\.wyregisteredagent\.net|login-actions|protocol\/openid-connect/;

async function wyregDoKeycloakLoginIfPresent(page) {
  const passwordVisible = await page
    .locator('#password')
    .waitFor({ state: 'visible', timeout: 8000 })
    .then(() => true)
    .catch(() => false);
  if (!passwordVisible) return;

  const email = process.env.WYREG_USERNAME || '';
  const password = process.env.WYREG_PASSWORD || '';
  if (!email || !password) throw new Error('wyreg login required but WYREG_USERNAME/WYREG_PASSWORD not set on worker');

  // Keystroke typing — React's controlled inputs need real input events to
  // register and the Sign in button stays disabled until validation fires.
  await page.locator('#username').click();
  await page.locator('#username').pressSequentially(email, { delay: 20 });
  await page.locator('#password').click();
  await page.locator('#password').pressSequentially(password, { delay: 20 });

  const rememberChecked = await page.locator('#rememberMe').isChecked().catch(() => false);
  if (!rememberChecked) await page.locator('#rememberMe').check({ force: true }).catch(() => {});

  const btnEnabled = await page.locator('#kc-login:not([disabled])').isVisible({ timeout: 3000 }).catch(() => false);
  if (btnEnabled) {
    await page.locator('#kc-login').click();
  } else {
    await page.evaluate(() => document.getElementById('kc-form')?.submit());
  }

  await page.waitForURL(/accounts\.wyregisteredagent\.net/, { timeout: 30000 }).catch(() => {});
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(1500);
  const stillLogin = await page.locator('#password').isVisible().catch(() => false);
  if (stillLogin) {
    // Dump the failing page to C:\worker\screenshots so the operator can see
    // whether it's a CAPTCHA, 2FA prompt, "Continue as X" screen, or plain error.
    try {
      const shotPath = path.join(OUT, 'wyreg-login-fail.png');
      await page.screenshot({ path: shotPath, fullPage: true });
      const htmlPath = path.join(OUT, 'wyreg-login-fail.html');
      const html = await page.content();
      await fs.promises.writeFile(htmlPath, html);
      const body = await page.evaluate(() => document.body?.innerText || '').catch(() => '');
      console.warn(`[wyreg-login-fail] url=${page.url()}`);
      console.warn(`[wyreg-login-fail] body=${(body || '').slice(0, 600).replace(/\s+/g, ' ')}`);
      console.warn(`[wyreg-login-fail] shot=${shotPath} html=${htmlPath}`);
    } catch (err) {
      console.warn(`[wyreg-login-fail] diagnostic dump failed: ${err.message}`);
    }
    throw new Error('wyreg login failed — check credentials, 2FA, or CAPTCHA');
  }
}

async function wyregEnsureLoggedIn(page) {
  await page.goto(`${WYREG_ACCOUNTS_BASE}/#/dashpanel`, { waitUntil: 'domcontentloaded', timeout: 30000 });
  await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(() => {});
  await wyregDoKeycloakLoginIfPresent(page);

  // Settle check: if we're not on accounts after login, something failed.
  for (let i = 0; i < 15; i++) {
    await page.waitForTimeout(1000);
    const url = page.url();
    if (/accounts\.wyregisteredagent\.net/.test(url) && !WYREG_KEYCLOAK_URL_RE.test(url)) return;
  }
  throw new Error(`wyreg ensureLoggedIn: did not settle on accounts — final URL ${page.url()}`);
}

async function wyregGotoDocumentsPage(page) {
  async function safeEval(fn, fallback) {
    for (let attempt = 0; attempt < 3; attempt++) {
      try { return await page.evaluate(fn); }
      catch (err) {
        const msg = err.message || String(err);
        if (!/context was destroyed|Execution context/i.test(msg)) throw err;
        await page.waitForTimeout(500);
      }
    }
    return fallback;
  }
  async function pageLooksLikeDocs() {
    await page.waitForLoadState('domcontentloaded', { timeout: 5000 }).catch(() => {});
    await page.waitForLoadState('networkidle', { timeout: 8000 }).catch(() => {});
    await page.waitForTimeout(1200);
    const bodyText = await safeEval(() => document.body.innerText || '', '');
    const url = page.url();
    const looksDocs = /documents?|filings?|completed|my documents|no documents/i.test(bodyText);
    const urlDocs = /document|filing/i.test(url);
    const is404 = /page not found|\b404\b/i.test(bodyText);
    return (looksDocs || urlDocs) && !is404;
  }

  // Strict docs-page detector. Dashpanel itself has an "Unread Documents" stat
  // card and mentions "Completed" for orders, so the only reliable signal is a
  // URL that is NOT the dashpanel/-hire-us/-businesses listing AND shows either
  // a PDF filename or an inbox header column.
  const NON_DOCS_URL_RE = /\/#\/(dashpanel|hire-us|services|pending-filings|businesses|account)(\/|\?|$)/;
  async function pageIsDocsInbox() {
    await page.waitForLoadState('domcontentloaded', { timeout: 5000 }).catch(() => {});
    await page.waitForLoadState('networkidle', { timeout: 8000 }).catch(() => {});
    await page.waitForTimeout(1200);
    const url = page.url();
    if (NON_DOCS_URL_RE.test(url) && !/\/businesses\/[^/]+/.test(url)) return false;
    const bodyText = await safeEval(() => document.body.innerText || '', '');
    const hasPdf = /\.pdf\b/i.test(bodyText);
    const hasInboxMarkers = /(filing date|document type|date received|document name|download)/i.test(bodyText);
    const urlHit = /document|filing|inbox|\/report/i.test(url);
    const is404 = /page not found|\b404\b|sasquatch/i.test(bodyText);
    return (hasPdf || hasInboxMarkers || urlHit) && !is404;
  }

  async function waitForUrlChange(from, timeoutMs = 8000) {
    const end = Date.now() + timeoutMs;
    while (Date.now() < end) {
      if (page.url() !== from) return true;
      await page.waitForTimeout(300);
    }
    return false;
  }

  // After landing on the docs inbox URL, the SPA fetches the doc list async
  // and shows "Loading..." in the meantime. Wait for that to disappear before
  // returning so callers don't enumerate an empty skeleton.
  async function waitForDocsContentLoaded() {
    await page.waitForFunction(() => {
      const t = document.body?.innerText || '';
      const loading = /(^|\n)\s*Loading\.\.\.\s*(\n|$)/i.test(t);
      if (loading) return false;
      // Either we see a .pdf filename, a "no documents" empty state, or a
      // recognisable table/row marker — any is a signal the list resolved.
      return /\.pdf\b|no documents|date received|filing date|document type/i.test(t);
    }, { timeout: 20000 }).catch(() => {});
    await page.waitForTimeout(1000);
  }

  // Strategy 1: dashpanel's "Unread Documents" stat card has a sibling "View"
  // action. Use Playwright locators (not page.evaluate-based .click) so Vue
  // v-on handlers fire and routing actually happens.
  const dashpanelUrl = page.url();
  try {
    const unreadCard = page.locator('div, article, section, li').filter({ hasText: /unread documents/i }).filter({ hasText: /documents not yet opened|^\d+$/i }).first();
    if (await unreadCard.count() > 0) {
      const viewBtn = unreadCard.locator('a, button, [role="link"]').filter({ hasText: /^\s*view\s*$/i }).first();
      if (await viewBtn.count() > 0) {
        await viewBtn.click({ timeout: 5000 });
        if (await waitForUrlChange(dashpanelUrl, 8000)) {
          if (await pageIsDocsInbox()) {
            await waitForDocsContentLoaded();
            console.log(`[wyregGotoDocumentsPage] settled via Unread Documents → View → ${page.url()}`);
            return;
          }
          console.log(`[wyregGotoDocumentsPage] Unread View click landed at ${page.url()} but not a docs inbox`);
        } else {
          console.log(`[wyregGotoDocumentsPage] Unread View click did not change URL from dashpanel`);
        }
      } else {
        console.log(`[wyregGotoDocumentsPage] Unread Documents card found but no View button inside`);
      }
    }
  } catch (err) {
    console.warn(`[wyregGotoDocumentsPage] Unread click path failed: ${err.message}`);
  }

  // Strategy 2: per-business detail. wyreg routes docs under /#/businesses/<id>
  // with a Documents tab. Go via the businesses list and open the first entity.
  try {
    await page.goto(`${WYREG_ACCOUNTS_BASE}/#/businesses`, { waitUntil: 'domcontentloaded', timeout: 15000 });
    await page.waitForLoadState('networkidle', { timeout: 8000 }).catch(() => {});
    await page.waitForTimeout(1500);
    const bizListUrl = page.url();

    // Try a real anchor first, then a Playwright text-click on an LLC tile.
    let clicked = false;
    const anchor = page.locator('a[href*="#/businesses/"]').filter({ hasNotText: /^\s*view all\s*$/i }).first();
    if (await anchor.count() > 0) {
      await anchor.click({ timeout: 5000 });
      clicked = true;
    } else {
      const tile = page.getByText(/\bLLC\b|\bL\.L\.C\.\b|\bInc\.?\b|\bCorp\.?\b/).first();
      if (await tile.count() > 0) {
        await tile.click({ timeout: 5000 });
        clicked = true;
      }
    }
    if (clicked) {
      await waitForUrlChange(bizListUrl, 8000);
      console.log(`[wyregGotoDocumentsPage] entered business detail → ${page.url()}`);
      await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => {});
      await page.waitForTimeout(1500);

      // Click the Documents tab/sub-nav
      const docsTab = page.locator('a, button, [role="tab"], [role="link"]').filter({ hasText: /^\s*documents?\s*$/i }).first();
      if (await docsTab.count() > 0) {
        const beforeTab = page.url();
        await docsTab.click({ timeout: 5000 }).catch(() => {});
        await waitForUrlChange(beforeTab, 5000);
        console.log(`[wyregGotoDocumentsPage] Documents tab clicked → ${page.url()}`);
      } else {
        console.log(`[wyregGotoDocumentsPage] no Documents tab on business detail`);
      }
      if (await pageIsDocsInbox()) {
        await waitForDocsContentLoaded();
        console.log(`[wyregGotoDocumentsPage] settled on business-detail docs → ${page.url()}`);
        return;
      }
    }
  } catch (err) {
    console.warn(`[wyregGotoDocumentsPage] business-detail path failed: ${err.message}`);
  }

  // Strategy 3: generic nav-bar link whose own text equals "Documents" / "Filings".
  const navClicked = await safeEval(() => {
    const candidates = Array.from(document.querySelectorAll('a, button, [role="link"], [role="menuitem"]'));
    const match = candidates.find((el) => /^(documents|my documents|filings|my filings)$/i.test((el.textContent || '').trim()));
    if (match) { match.click(); return true; }
    return false;
  }, false);
  if (navClicked && await pageIsDocsInbox()) {
    await waitForDocsContentLoaded();
    console.log(`[wyregGotoDocumentsPage] settled via nav click → ${page.url()}`);
    return;
  }

  // Strategy 4: speculative direct routes (most are sasquatch 404s but cheap).
  const routes = [
    `${WYREG_ACCOUNTS_BASE}/#/documents`,
    `${WYREG_ACCOUNTS_BASE}/#/dashpanel/documents`,
    `${WYREG_ACCOUNTS_BASE}/#/my-documents`,
    `${WYREG_ACCOUNTS_BASE}/#/filings`,
  ];
  for (const url of routes) {
    try {
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 15000 });
      if (await pageIsDocsInbox()) {
        await waitForDocsContentLoaded();
        console.log(`[wyregGotoDocumentsPage] settled on ${url}`);
        return;
      }
    } catch (err) {
      console.warn(`[wyregGotoDocumentsPage] ${url} failed: ${err.message}`);
    }
  }
  throw new Error(`wyreg gotoDocumentsPage: no documents view found — last URL ${page.url()}`);
}

async function wyregEnumerateDocRowsOnce(page) {
  return page.evaluate(() => {
    const rowSelectors = [
      '.p-datatable-tbody > tr',
      '.p-dataview .p-dataview-content > div',
      '[data-test="document-row"]',
      'tr[data-doc-id]',
      'li[data-doc-id]',
    ];
    const seen = new Set();
    const candidateRows = [];
    for (const sel of rowSelectors) {
      document.querySelectorAll(sel).forEach((el) => {
        if (!seen.has(el)) { seen.add(el); candidateRows.push(el); }
      });
    }
    // wyreg's /#/documents page renders each doc as a card containing the
    // labels Jurisdiction / Type / Received / Status — find those.
    if (candidateRows.length === 0) {
      const all = document.querySelectorAll('[class*="card"], [class*="row"], [class*="item"], li, article, div');
      all.forEach((el) => {
        if (seen.has(el)) return;
        const text = (el.textContent || '').trim();
        // Require ALL of these labels present and the card not be huge (avoid root container)
        if (/\bJurisdiction\b/i.test(text)
            && /\bType\b/i.test(text)
            && /\bReceived\b/i.test(text)
            && /\bStatus\b/i.test(text)
            && text.length < 600) {
          seen.add(el);
          candidateRows.push(el);
        }
      });
    }
    // Legacy PDF-filename fallback (keeps old behaviour for other layouts)
    if (candidateRows.length === 0) {
      document.querySelectorAll('[class*="card"], [class*="row"], [class*="item"], tr, li').forEach((el) => {
        const text = el.textContent || '';
        if (/\.pdf\b/i.test(text) && !seen.has(el)) { seen.add(el); candidateRows.push(el); }
      });
    }
    const results = [];
    candidateRows.forEach((row, idx) => {
      const rowEl = row;
      const text = rowEl.innerText || rowEl.textContent || '';

      // Try to pull a "Type" label value (e.g. "Initial Resolution") — that's the
      // document kind on wyreg's card layout and becomes our filename stem.
      let docType = null;
      const typeMatch = text.match(/\bType\b\s*\n+\s*([^\n]{2,80})/);
      if (typeMatch) docType = typeMatch[1].trim();

      let filename = '';
      const pdfLink = Array.from(rowEl.querySelectorAll('a, button')).find((el) => /\.pdf\b/i.test(el.textContent || ''));
      if (pdfLink) {
        filename = (pdfLink.textContent || '').trim();
      } else {
        const m = text.match(/([A-Za-z0-9._-]+\.pdf)\b/i);
        if (m) filename = m[1];
      }
      if (!filename && docType) {
        // Derive filename from Type when no explicit filename is shown
        const slug = docType.replace(/[^A-Za-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
        filename = `${slug}.pdf`;
      }
      if (!filename) {
        const hasDownload = rowEl.querySelector('a[download], a[href*=".pdf"], button[aria-label*="download" i], button[title*="download" i]');
        if (!hasDownload) return;
        const nameEl = rowEl.querySelector('[class*="filename"], [class*="name"], [class*="title"]');
        filename = nameEl?.textContent?.trim() || `document-${idx + 1}.pdf`;
      }

      const idAttr =
        rowEl.getAttribute('data-doc-id') ||
        rowEl.getAttribute('data-id') ||
        rowEl.getAttribute('data-row-id') ||
        rowEl.id ||
        null;
      const docId = idAttr || `row-${idx}-${text.slice(0, 32).replace(/\s+/g, '_')}`;

      let entityNameHint = null;
      const entityMatch = text.match(/([A-Z][A-Za-z0-9 &.,'\-]{1,60}\b(?:LLC|L\.L\.C\.|Inc\.?|Corp\.?|Corporation|Ltd\.?))/);
      if (entityMatch) entityNameHint = entityMatch[1].trim();

      // Prefer the value under the "Received" label on wyreg cards.
      let filingDateHint = null;
      const recvMatch = text.match(/\bReceived\b\s*\n+\s*([^\n]{3,40})/);
      if (recvMatch) filingDateHint = recvMatch[1].trim();
      if (!filingDateHint) {
        const dateMatch = text.match(/\b(20\d{2}-[01]\d-[0-3]\d|[01]?\d\/[0-3]?\d\/20\d{2}|[A-Z][a-z]{2,8} \d{1,2},? 20\d{2})\b/);
        if (dateMatch) filingDateHint = dateMatch[1];
      }

      const orderAttr = rowEl.getAttribute('data-order-id');
      const orderMatch = !orderAttr ? text.match(/\border[\s#:]*([A-Z0-9-]{5,32})\b/i) : null;
      const upstreamOrderId = orderAttr || (orderMatch ? orderMatch[1] : null);

      const anchorHref =
        rowEl.querySelector('a[href*=".pdf"]')?.href ||
        rowEl.querySelector('a[download]')?.href ||
        null;

      // Two possible click targets:
      //   1. An explicit download control on the row (preferred, rare)
      //   2. The row itself — wyreg's card layout routes to a Document Details
      //      page on row-click; that page exposes an actual "Download" button.
      let clickSelector = null;
      let openDetailSelector = null;
      const explicitDownload = rowEl.querySelector('a[href*=".pdf"], a[download], button[aria-label*="download" i], button[title*="download" i], button[class*="download" i], [data-test*="download"]');
      if (explicitDownload) {
        const marker = `wyreg-doc-click-${docId.replace(/[^a-zA-Z0-9_-]/g, '_')}`;
        explicitDownload.setAttribute('data-wyreg-click', marker);
        clickSelector = `[data-wyreg-click="${marker}"]`;
      } else {
        // Mark the row for row-click-into-details flow
        const rowMarker = `wyreg-doc-row-${docId.replace(/[^a-zA-Z0-9_-]/g, '_')}`;
        rowEl.setAttribute('data-wyreg-row', rowMarker);
        openDetailSelector = `[data-wyreg-row="${rowMarker}"]`;
      }

      results.push({
        docId,
        filename,
        entityNameHint,
        filingDateHint,
        upstreamOrderId,
        downloadHref: anchorHref,
        downloadClickSelector: clickSelector,
        openDetailSelector,
      });
    });
    return results;
  });
}

async function wyregEnumerateDocRows(page) {
  for (let attempt = 0; attempt < 3; attempt++) {
    try { return await wyregEnumerateDocRowsOnce(page); }
    catch (err) {
      const msg = err.message || String(err);
      if (!/context was destroyed|Execution context/i.test(msg)) throw err;
      await page.waitForTimeout(1000);
    }
  }
  return wyregEnumerateDocRowsOnce(page);
}

async function wyregDownloadDocRow(page, row) {
  // Strategy 1: direct PDF href (rare but cleanest)
  if (row.downloadHref) {
    try {
      const resp = await page.request.get(row.downloadHref);
      if (resp.ok()) return Buffer.from(await resp.body());
      console.warn(`[wyregDownloadDocRow] href fetch ${resp.status()} for ${row.filename}`);
    } catch (err) {
      console.warn(`[wyregDownloadDocRow] href fetch threw for ${row.filename}: ${err.message}`);
    }
  }

  // Strategy 2: explicit download control on the row — click + intercept.
  if (row.downloadClickSelector) {
    try {
      const [download] = await Promise.all([
        page.waitForEvent('download', { timeout: 20000 }),
        page.locator(row.downloadClickSelector).first().click({ force: true }),
      ]);
      return await drainDownload(download);
    } catch (err) {
      console.warn(`[wyregDownloadDocRow] click-download failed for ${row.filename}: ${err.message}`);
    }
  }

  // Strategy 3: row-click routes to /#/documents/<id> (Document Details page).
  // That page has a real "Download" button. Caller must pre-navigate back to
  // the list before calling with the next row — the list DOM re-renders on
  // each list visit so enumerator markers are single-use.
  if (row.openDetailSelector) {
    try {
      const listUrl = page.url();
      await page.locator(row.openDetailSelector).first().click({ timeout: 8000 });
      const changed = await (async () => {
        const end = Date.now() + 10000;
        while (Date.now() < end) {
          if (page.url() !== listUrl) return true;
          await page.waitForTimeout(300);
        }
        return false;
      })();
      if (!changed) {
        console.warn(`[wyregDownloadDocRow] row click did not navigate for ${row.filename}`);
        return null;
      }
      await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => {});
      await page.waitForTimeout(1500);

      const downloadBtn = page.locator('a, button').filter({ hasText: /^\s*download\s*$/i }).first();
      if (await downloadBtn.count() === 0) {
        console.warn(`[wyregDownloadDocRow] no Download button on detail page for ${row.filename}`);
        return null;
      }
      // Capture the download URL via the browser's download event, then fetch
      // with page.request (which inherits session cookies). CDP-attached
      // Playwright can't reliably drain download streams, but refetching the
      // URL that triggered the download works because our auth cookies are
      // already in the context.
      const [download] = await Promise.all([
        page.waitForEvent('download', { timeout: 30000 }),
        downloadBtn.click({ timeout: 8000 }),
      ]);
      const dlUrl = download.url();
      await download.cancel().catch(() => {}); // don't care about the browser-side save
      if (!dlUrl) {
        console.warn(`[wyregDownloadDocRow] download event had no URL for ${row.filename}`);
        return null;
      }
      const resp = await page.request.get(dlUrl);
      if (!resp.ok()) {
        console.warn(`[wyregDownloadDocRow] refetch ${resp.status()} for ${row.filename} at ${dlUrl}`);
        return null;
      }
      return Buffer.from(await resp.body());
    } catch (err) {
      console.warn(`[wyregDownloadDocRow] detail-page download failed for ${row.filename}: ${err.message.slice(0, 120)}`);
    }
  }

  return null;
}

async function drainDownload(download) {
  try {
    const p = await download.path();
    if (!p) return null;
    const buf = await fs.promises.readFile(p);
    try { await fs.promises.unlink(p); } catch { /* ignore */ }
    return buf;
  } catch (err) {
    console.warn(`[drainDownload] ${err.message}`);
    return null;
  }
}

// ── Portal handlers ─────────────────────────────────────────────────────────
// Each handler: async (page, payload) → result object. Handlers receive a
// Playwright Page already wired to the profile (AdsPower or bypass).
const handlers = {
  async peek(page, { body, claimId }) {
    await page.goto('https://help.peek.com/hc/en-us/requests/new', { waitUntil: 'domcontentloaded' });
    await page.fill('#request_anonymous_requester_email', 'alex@specchio.xyz').catch(() => {});
    await page.fill('#request_subject', 'Refund dispute — Marlin Espadas shuttle delay 28/01/2026');
    await page.fill('#request_description', body);
    const shot = path.join(OUT, `${claimId}-peek-preview.png`);
    await page.screenshot({ path: shot, fullPage: true });
    return { portal: 'peek', preview: shot, note: 'submit left manual pending captcha verification' };
  },
  async btb(page, { body, claimId, submit = false }) {
    await page.goto('https://www.belizetourismboard.org/contact/', { waitUntil: 'networkidle', timeout: 60000 });
    await page.waitForTimeout(3000);
    const tryFill = async (sel, val) => {
      for (const s of sel) {
        if (await page.locator(s).count()) { await page.locator(s).first().fill(val); return s; }
      }
      return null;
    };
    const nameSel  = await tryFill(['input[name="your-name"]','input[name*="name"]','#name'], 'Alessandro Specchio');
    const emailSel = await tryFill(['input[name="your-email"]','input[type="email"]'], 'alex@specchio.xyz');
    const subjSel  = await tryFill(['input[name="your-subject"]','input[name*="subject"]'], 'Formal Complaint — Marlin Espadas Ltd shuttle delay (28 Jan 2026)');
    const msgSel   = await tryFill(['textarea[name="your-message"]','textarea[name*="message"]','textarea'], body);
    const before = path.join(OUT, `${claimId}-btb-filled.png`);
    await page.screenshot({ path: before, fullPage: true });
    let confirmation = null;
    if (submit) {
      const btn = page.locator('button[type="submit"],input[type="submit"],.wpcf7-submit').first();
      if (await btn.count()) {
        await btn.click();
        await page.waitForTimeout(8000);
        const okText = await page.locator('.wpcf7-response-output, .wpcf7-mail-sent-ok').first().textContent().catch(() => null);
        confirmation = okText?.trim() || 'submitted (no explicit confirm banner)';
      }
    }
    const after = path.join(OUT, `${claimId}-btb-after.png`);
    await page.screenshot({ path: after, fullPage: true });
    return { portal: 'btb', filled: { nameSel, emailSel, subjSel, msgSel }, before, after, confirmation };
  },
  async profeco(page, { claimId }) {
    await page.goto('https://concilianet.profeco.gob.mx/Concilianet/inicio.jsp', { waitUntil: 'domcontentloaded' });
    const shot = path.join(OUT, `${claimId}-profeco-preview.png`);
    await page.screenshot({ path: shot, fullPage: true });
    return { portal: 'profeco', preview: shot, note: 'Concilianet requires e.firma — manual step' };
  },
  async webdenuncia(page, { claimId }) {
    await page.goto('https://webdenuncia.profeco.gob.mx/', { waitUntil: 'domcontentloaded' });
    const shot = path.join(OUT, `${claimId}-webdenuncia-preview.png`);
    await page.screenshot({ path: shot, fullPage: true });
    return { portal: 'webdenuncia', preview: shot, note: 'PROFECO online complaint — no e.firma required' };
  },
  async enac(page, { claimId }) {
    await page.goto('https://www.enac.gov.it/passeggeri/reclami', { waitUntil: 'domcontentloaded' });
    const shot = path.join(OUT, `${claimId}-enac-preview.png`);
    await page.screenshot({ path: shot, fullPage: true });
    return { portal: 'enac', preview: shot, note: 'ENAC passenger complaint — free web form for EU261' };
  },
  async inspect(page, { claimId, url }) {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });
    await page.waitForTimeout(2000);
    const shot = path.join(OUT, `${claimId}-inspect.png`);
    await page.screenshot({ path: shot, fullPage: true });
    const html = await page.content();
    fs.writeFileSync(shot.replace('.png', '.html'), html);
    return { portal: 'inspect', preview: shot, htmlDump: shot.replace('.png', '.html') };
  },
  async xcover_status(page, payload) {
    // Back-compat alias → xcover_read_status.
    return handlers.xcover_read_status(page, payload);
  },

  // ── xCover: open dashboard, locate claim row by ref, open detail ──────────
  async xcover_read_status(page, { claimId, claimRef }) {
    const ref = claimRef ?? '';
    await page.goto('https://www.xcover.com/en/protection/claims', { waitUntil: 'domcontentloaded', timeout: 60000 });
    await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
    const dashShot = path.join(OUT, `${claimId}-xcover-dashboard.png`);
    await page.screenshot({ path: dashShot, fullPage: true });

    // Try to open the claim detail by matching the ref text.
    let opened = false;
    if (ref) {
      const rowLink = page.locator(`a:has-text("${ref}"), [role="row"]:has-text("${ref}") a, a[href*="${ref}"]`).first();
      if (await rowLink.count().catch(() => 0)) {
        await rowLink.click({ timeout: 10000 }).catch(() => {});
        await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
        opened = true;
      }
    }
    // Fallback: some accounts deep-link at claim.xcover.com/<ref>
    if (!opened && ref) {
      await page.goto(`https://claim.xcover.com/${ref}`, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
      await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
      opened = true;
    }

    const detailShot = path.join(OUT, `${claimId}-xcover-read_status.png`);
    await page.screenshot({ path: detailShot, fullPage: true });
    const bodyText = await page.evaluate(() => document.body.innerText || '').catch(() => '');
    const statusMatch = bodyText.match(/status[:\s]+([A-Za-z_ ]{3,40})/i);
    const missingMatch = bodyText.match(/(missing|required)[^\n]{0,40}documents?[:\s]+([^\n]+)/i);
    const missingDocs = missingMatch?.[2]?.split(/,|;|•/).map((s) => s.trim()).filter(Boolean) ?? [];
    return {
      portal: 'xcover_read_status',
      claimRef: ref,
      opened,
      dashboard: dashShot,
      preview: detailShot,
      status: statusMatch?.[1]?.trim() ?? 'unknown',
      missingDocs,
      rawText: bodyText.slice(0, 3000),
    };
  },

  // ── xCover: fill a reply / comment body. Does NOT submit unless submit=true.
  async xcover_reply(page, { claimId, claimRef, body, submit = false }) {
    const ref = claimRef ?? '';
    if (ref) await page.goto(`https://claim.xcover.com/${ref}`, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
    await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});

    // Try a sequence of stable selectors for the reply affordance.
    const openSel = [
      'button:has-text("Reply")',
      'button:has-text("Add comment")',
      'button:has-text("Add a comment")',
      'button:has-text("Message")',
      '[data-testid*="reply" i]',
      '[data-testid*="comment" i]',
    ];
    for (const s of openSel) {
      const loc = page.locator(s).first();
      if (await loc.count().catch(() => 0)) { await loc.click({ timeout: 4000 }).catch(() => {}); break; }
    }
    await page.waitForTimeout(800);

    const fieldSel = [
      'textarea[name*="reply" i]',
      'textarea[name*="comment" i]',
      'textarea[placeholder*="reply" i]',
      'textarea[placeholder*="message" i]',
      'div[contenteditable="true"]',
      'textarea',
    ];
    let filledSel = null;
    for (const s of fieldSel) {
      const loc = page.locator(s).first();
      if (await loc.count().catch(() => 0)) {
        await loc.click({ timeout: 3000 }).catch(() => {});
        await loc.fill(body ?? '').catch(async () => { await page.keyboard.type(body ?? '', { delay: 20 }); });
        filledSel = s; break;
      }
    }
    const filledShot = path.join(OUT, `${claimId}-xcover-reply-filled.png`);
    await page.screenshot({ path: filledShot, fullPage: true });

    let confirmation = null;
    if (submit && filledSel) {
      const btn = page.locator('button:has-text("Send"), button:has-text("Submit"), button:has-text("Post"), button[type="submit"]').first();
      if (await btn.count().catch(() => 0)) {
        await btn.click({ timeout: 5000 }).catch(() => {});
        await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
        confirmation = 'submitted (no explicit confirm banner parsed)';
      }
    }
    const afterShot = path.join(OUT, `${claimId}-xcover-reply-after.png`);
    await page.screenshot({ path: afterShot, fullPage: true });
    return {
      portal: 'xcover_reply',
      claimRef: ref,
      filledSel,
      submitted: Boolean(submit && filledSel && confirmation),
      preview: filledShot,
      after: afterShot,
      confirmation,
      note: submit ? 'submit requested' : 'filled only — explicit submit=true required to send',
    };
  },

  // ── xCover: attach a document. Accepts base64 in payload. ─────────────────
  async xcover_upload_doc(page, { claimId, claimRef, fileBase64, filename, contentType }) {
    const ref = claimRef ?? '';
    if (!fileBase64 || !filename) {
      return { portal: 'xcover_upload_doc', error: 'fileBase64 and filename required' };
    }
    if (ref) await page.goto(`https://claim.xcover.com/${ref}`, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
    await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});

    // Reveal an upload button that may hide the <input type="file">.
    const triggerSel = [
      'button:has-text("Upload")',
      'button:has-text("Attach")',
      'button:has-text("Add document")',
      'label[for*="file" i]',
      '[data-testid*="upload" i]',
    ];
    for (const s of triggerSel) {
      const loc = page.locator(s).first();
      if (await loc.count().catch(() => 0)) { await loc.click({ timeout: 3000 }).catch(() => {}); break; }
    }

    const buffer = Buffer.from(fileBase64, 'base64');
    const fileInput = page.locator('input[type="file"]').first();
    let attached = false;
    if (await fileInput.count().catch(() => 0)) {
      await fileInput.setInputFiles({ name: filename, mimeType: contentType || 'application/octet-stream', buffer });
      attached = true;
    }
    await page.waitForTimeout(1500);
    const shot = path.join(OUT, `${claimId}-xcover-upload.png`);
    await page.screenshot({ path: shot, fullPage: true });
    return {
      portal: 'xcover_upload_doc',
      claimRef: ref,
      filename,
      sizeBytes: buffer.byteLength,
      attached,
      preview: shot,
      note: attached ? 'file attached via input[type=file]; user confirms submit' : 'no file input found — TODO: map upload modal',
    };
  },

  // ── wyreg (wyregisteredagent.net) — docs inbox polling + inspection ──────
  // Ported from bca-app/state-filing-worker wyreg.ts. Credentials come from
  // WYREG_USERNAME / WYREG_PASSWORD on this host. AdsPower keeps the Keycloak
  // session warm across runs, so login is a one-shot check instead of the
  // retry-on-bounce loop the app used when it was re-hydrating cookies itself.
  async wyreg_poll_docs(page, { jobId, entity_name_filter } = {}) {
    const tag = jobId ?? 'poll';
    await wyregEnsureLoggedIn(page);
    // Dashpanel needs time to render the Unread Documents card before we
    // try clicking it — otherwise gotoDocumentsPage falls through every
    // strategy and throws.
    await page.waitForTimeout(2500);
    await wyregGotoDocumentsPage(page);
    const listUrl = page.url();
    const initialRows = await wyregEnumerateDocRows(page);
    console.log(`[wyreg_poll_docs:${tag}] enumerated ${initialRows.length} candidate rows at ${listUrl}`);

    // Build a stable identity per-row so we can dedupe across re-enumerations:
    // wyreg rows have no persistent id we can extract, so use
    // (filename + entityNameHint + filingDateHint) as a fingerprint.
    const rowFingerprint = (r) => `${r.entityNameHint}|${r.filename}|${r.filingDateHint}`;

    // Optional client-side filter: only pull docs for the named entity (BCAX etc).
    const matchesFilter = (r) => !entity_name_filter
      || (r.entityNameHint || '').toLowerCase().includes(String(entity_name_filter).toLowerCase());

    const seen = new Set();
    const plan = initialRows.filter(matchesFilter).map(rowFingerprint);
    console.log(`[wyreg_poll_docs:${tag}] plan (${plan.length}): ${plan.slice(0, 5).join(' | ')}${plan.length > 5 ? ' …' : ''}`);

    const docs = [];
    // Each iteration: re-enumerate on the fresh list DOM, pick the first
    // planned row we haven't processed yet, download, then come back to list.
    // page.reload() on the same hash route does not force Vue to re-render
    // the list; the reliable path is to round-trip through the dashpanel
    // and re-invoke the "Unread Documents -> View" click we know works.
    for (let i = 0; i < plan.length; i++) {
      if (i > 0) {
        await page.goto(`${WYREG_ACCOUNTS_BASE}/#/dashpanel`, { waitUntil: 'domcontentloaded', timeout: 15000 }).catch(() => {});
        await page.waitForLoadState('networkidle', { timeout: 8000 }).catch(() => {});
        await page.waitForTimeout(2000);
        await wyregGotoDocumentsPage(page);
      }
      const fresh = await wyregEnumerateDocRows(page);
      console.log(`[wyreg_poll_docs:${tag}] iter ${i}: fresh rows=${fresh.length}, matching=${fresh.filter(matchesFilter).length}, seen=${seen.size}`);
      const row = fresh.find((r) => matchesFilter(r) && !seen.has(rowFingerprint(r)));
      if (!row) {
        console.warn(`[wyreg_poll_docs:${tag}] no remaining rows match plan at iter ${i} — stopping`);
        break;
      }
      seen.add(rowFingerprint(row));

      const buffer = await wyregDownloadDocRow(page, row);
      if (!buffer || buffer.length === 0) {
        console.warn(`[wyreg_poll_docs:${tag}] skipping ${row.filename} — download failed`);
        continue;
      }
      if (buffer.slice(0, 5).toString() !== '%PDF-') {
        console.warn(`[wyreg_poll_docs:${tag}] skipping ${row.filename} — not a PDF (got ${buffer.slice(0, 20).toString('hex')})`);
        continue;
      }
      console.log(`[wyreg_poll_docs:${tag}] downloaded ${row.filename} (${buffer.length} bytes) for ${row.entityNameHint}`);
      docs.push({
        upstream_doc_id: row.docId,
        filename: row.filename,
        upstream_order_id: row.upstreamOrderId,
        entity_name_hint: row.entityNameHint,
        filing_date_hint: row.filingDateHint,
        buffer_base64: buffer.toString('base64'),
      });
    }
    return { portal: 'wyreg_poll_docs', url: page.url(), docs };
  },

  async wyreg_inspect_docs(page, { jobId } = {}) {
    const tag = jobId ?? 'inspect';
    const routes_attempted = [];
    await wyregEnsureLoggedIn(page);
    await page.waitForTimeout(2000);
    const atLogin = await page.locator('#password').isVisible({ timeout: 1000 }).catch(() => false);
    routes_attempted.push({
      url: page.url(),
      settled: !atLogin,
      reason: atLogin ? 'session expired — landed on Keycloak login' : 'dashpanel loaded via ensureLoggedIn',
    });
    // Also try to navigate to the docs inbox so inspect reflects the same page
    // that poll_docs scrapes. Don't throw on failure — operator still wants the
    // nav dump either way.
    try {
      await wyregGotoDocumentsPage(page);
      routes_attempted.push({ url: page.url(), settled: true, reason: 'wyregGotoDocumentsPage succeeded' });
    } catch (err) {
      routes_attempted.push({ url: page.url(), settled: false, reason: `wyregGotoDocumentsPage failed: ${(err?.message || String(err)).slice(0, 200)}` });
    }
    for (let i = 0; i < 8; i++) {
      await page.waitForLoadState('networkidle', { timeout: 3000 }).catch(() => {});
      await page.waitForTimeout(800);
      const bodyOk = await page.evaluate(() => (document.body?.innerText || '').length > 0).catch(() => false);
      if (bodyOk) break;
    }

    const rows = await wyregEnumerateDocRows(page).catch(() => []);

    const nav_links = await page.evaluate(() => {
      const seen = new Set();
      const out = [];
      document.querySelectorAll('a, button, [role="link"], [role="menuitem"]').forEach((el) => {
        const he = el;
        if (he.offsetParent === null && (he.offsetWidth === 0 || he.offsetHeight === 0)) return;
        const text = (he.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 80);
        if (!text) return;
        const href = el.href || null;
        const key = `${el.tagName}|${text}|${href || ''}`;
        if (seen.has(key)) return;
        seen.add(key);
        out.push({ tag: el.tagName.toLowerCase(), text, href, class: (he.className || '').slice(0, 80) });
      });
      return out;
    }).catch(() => []);

    const business_tiles = await page.evaluate(() => {
      const out = [];
      document.querySelectorAll('[class*="card"], [class*="tile"], [class*="business"], li, article').forEach((el) => {
        const he = el;
        const text = (he.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 200);
        if (!/\b(LLC|L\.L\.C\.|Inc\.?|Corp\.?|Corporation|Ltd\.?)\b/i.test(text)) return;
        const anchor = el.querySelector('a[href]');
        out.push({ text, href: anchor?.href || null });
      });
      return out.slice(0, 40);
    }).catch(() => []);

    async function withRetry(label, fn, fallback) {
      for (let attempt = 0; attempt < 3; attempt++) {
        try { return await fn(); }
        catch (err) {
          const msg = err.message || String(err);
          console.log(`[wyreg_inspect_docs:${tag}] ${label} attempt ${attempt} failed: ${msg.slice(0, 120)}`);
          if (!/navigating|context was destroyed|Execution context/i.test(msg)) throw err;
          await page.waitForTimeout(1500);
        }
      }
      return fallback;
    }

    const screenshotBuf = await withRetry('screenshot', () => page.screenshot({ fullPage: true }), Buffer.alloc(0));
    const html = await withRetry('html', () => page.content(), '');
    const bodyText = await withRetry('body-text', () => page.evaluate(() => document.body?.innerText || ''), '');

    const shotName = `wyreg-inspect-${tag}-${Date.now()}.png`;
    try { fs.writeFileSync(path.join(OUT, shotName), screenshotBuf); } catch { /* ignore */ }

    return {
      portal: 'wyreg_inspect_docs',
      url: page.url(),
      rows,
      nav_links,
      business_tiles,
      screenshot: shotName,
      screenshot_base64: screenshotBuf.toString('base64'),
      html_excerpt: html.slice(0, 40_000),
      body_text_excerpt: bodyText.slice(0, 8_000),
      routes_attempted,
    };
  },

  // ── IRS EIN Online Assistant: discovery-only inspect handler ─────────────
  // Walks the public marketing page → /modiein/individual/index.jsp → Begin
  // Application → legal-structure page → selects LLC → stops on the
  // number-of-members question. Never fills any text input, never submits.
  // Screenshots each meaningful step. Returns the final page's form fields,
  // nav links, HTML/body excerpts, and the operating-hours check so the
  // operator can see what's on the screen. Phase 2 (dryrun/submit) will
  // personalize from here.
  async irs_apply_ein_inspect(page, { jobId } = {}) {
    const tag = jobId || 'adhoc';
    const outDir = path.join(OUT, 'irs-ein-inspect', tag);
    fs.mkdirSync(outDir, { recursive: true });
    const steps_visited = [];
    const hours = irsEinOnlineHoursOpen();
    if (!hours.open) {
      throw new Error(`IRS EIN online application closed: ${hours.reason}. next_open_at=${hours.next_open_at}`);
    }

    const safeLabel = (s) => String(s).replace(/[^a-zA-Z0-9_-]+/g, '_').slice(0, 60);
    async function captureStep(label) {
      const idx = steps_visited.length;
      const file = path.join(outDir, `step-${idx}-${safeLabel(label)}.png`);
      try { await page.screenshot({ path: file, fullPage: true }); } catch { /* ignore */ }
      steps_visited.push({ step_label: label, url: page.url(), screenshot_path: file });
    }
    async function settle() {
      await page.waitForLoadState('domcontentloaded', { timeout: 20000 }).catch(() => {});
      await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(() => {});
      await page.waitForTimeout(1500);
    }
    // Try locators in sequence; return the first one that's visible. Short
    // timeout per candidate so we don't sit for 30s on a dead selector.
    async function firstVisible(selectors, perTimeout = 3000) {
      for (const sel of selectors) {
        const loc = page.locator(sel).first();
        try {
          await loc.waitFor({ state: 'visible', timeout: perTimeout });
          return loc;
        } catch { /* try next */ }
      }
      return null;
    }

    let stoppedEarly = false;
    let stopReason = null;
    try {
      // Step 1: IRS marketing / info page.
      await page.goto(
        'https://www.irs.gov/businesses/small-businesses-self-employed/apply-for-an-employer-identification-number-ein-online',
        { waitUntil: 'domcontentloaded', timeout: 45000 },
      );
      await settle();
      await captureStep('irs-landing');

      // Step 2: click "Apply Online Now" / "Begin Application Now". IRS has
      // historically shifted between both wordings; match either.
      const applyLink = await firstVisible([
        'a:has-text("Apply Online Now")',
        'a:has-text("Begin Application Now")',
        'a:has-text("Apply Online")',
        'a[href*="modiein"]',
      ], 10000);
      if (!applyLink) {
        stoppedEarly = true;
        stopReason = 'unexpected — no Apply Online Now link on IRS landing';
      } else {
        await applyLink.click({ timeout: 8000 }).catch(() => {});
        await settle();
        await captureStep('ein-assistant-start');
      }

      // Step 3: on the assistant start page, click "Begin Application".
      if (!stoppedEarly) {
        const beginBtn = await firstVisible([
          'input[type="submit"][value*="Begin" i]',
          'button:has-text("Begin Application")',
          'a:has-text("Begin Application")',
          'input[type="button"][value*="Begin" i]',
        ], 10000);
        if (!beginBtn) {
          stoppedEarly = true;
          stopReason = 'unexpected — no Begin Application button on assistant start page';
        } else {
          await beginBtn.click({ timeout: 8000 }).catch(() => {});
          await settle();
          await captureStep('legal-structure-select');
        }
      }

      // Step 4: select "Limited Liability Company (LLC)" radio + Continue.
      // Real IRS selectors captured from /applyein/legalStructure on 2026-04-24:
      //   radio: input[name="legalStructureInput"][id^="LLClegalStructureInp"]
      //   continue: <a role="button" aria-label="Continue">Continue</a>
      // Continue is an anchor styled as a button, so input[type=submit] selectors
      // don't match — use aria-label directly.
      if (!stoppedEarly) {
        // IRS /applyein/ uses a custom React radio component where clicking
        // the input element does NOT update component state — only clicking
        // the LABEL or a styled wrapper fires the onChange. Try label first,
        // fall back to forcing a check on the underlying input.
        const llcLabel = await firstVisible([
          'label:has-text("Limited Liability Company (LLC)")',
          'label[for="LLClegalStructureInputid"]',
          'label[for^="LLClegalStructure" i]',
        ], 8000);
        const llcRadio = await firstVisible([
          'input[type="radio"][id^="LLClegalStructure" i]',
          'input[type="radio"][value="LLC"]',
          'input[type="radio"][value*="LLC" i]',
        ], 5000);
        if (!llcLabel && !llcRadio) {
          stoppedEarly = true;
          stopReason = 'unexpected — no LLC radio or label on legal-structure page';
        } else {
          // Prefer label click — fires React onChange. Fall through to .check
          // on the input as a belt-and-suspenders fallback.
          if (llcLabel) {
            await llcLabel.click({ timeout: 8000 }).catch(() => {});
          }
          if (llcRadio) {
            await llcRadio.check({ force: true, timeout: 5000 }).catch(() => {});
          }
          // Give the form a moment to register the selection before advancing.
          await page.waitForTimeout(800);
          // Verify the radio actually became checked — if not, abort early
          // with a clear message instead of clicking Continue and getting
          // a "Selection is required" error.
          const radioChecked = llcRadio
            ? await llcRadio.isChecked().catch(() => false)
            : await page.locator('input[type="radio"][value="LLC"]').first().isChecked().catch(() => false);
          if (!radioChecked) {
            stoppedEarly = true;
            stopReason = 'unexpected — LLC label/radio click did not toggle the radio (React onChange not firing)';
            await captureStep('llc-click-no-effect');
          }

          // Selecting LLC reveals two MORE required fields on the same page:
          //   - membersOfLlcInput (text): "How many member(s) are in the LLC?"
          //   - stateInputControl (select): physical-location state
          // Both must be filled before Continue advances. For the inspect
          // handler we use safe defaults (1 member, Wyoming) — these are
          // never submitted to IRS, just used to advance to Step 2 so we
          // can dump its form structure.
          if (!stoppedEarly) {
            await page.waitForTimeout(800);
            const membersInput = await firstVisible([
              'input[name="membersOfLlcInput"]',
              'input#membersOfLlcInput',
              'input[id*="member" i][type="text"]',
            ], 6000);
            if (membersInput) {
              await membersInput.fill('1').catch(() => {});
            } else {
              console.warn('[inspect:step1] membersOfLlcInput not found — Continue may fail validation');
            }
            const stateSelect = await firstVisible([
              'select[name="stateInputControl"]',
              'select#stateInputControl',
              'select[name*="state" i]',
            ], 6000);
            if (stateSelect) {
              // Try common Wyoming codes; IRS forms vary on value format.
              await stateSelect.selectOption('WY').catch(async () => {
                await stateSelect.selectOption({ label: 'Wyoming' }).catch(() => {});
              });
            } else {
              console.warn('[inspect:step1] stateInputControl not found — Continue may fail validation');
            }
            await page.waitForTimeout(800);

            // After member count + state are filled, IRS reveals a 3rd
            // required sub-field on the same page: "Why are you applying for
            // an EIN?" radio (reasonForApplyingInputControl). Pick "Started
            // a new business" for inspect — never submitted, just to advance.
            const reasonLabel = await firstVisible([
              'label[for="NEW_BUSINESSreasonForApplyingInputControlid"]',
              'label[for^="NEW_BUSINESSreasonForApplying" i]',
              'label:has-text("Started a new business"):not(:has(*))',
            ], 4000);
            const reasonRadio = await firstVisible([
              'input[type="radio"][id^="NEW_BUSINESSreasonForApplying" i]',
              'input[type="radio"][value="NEW_BUSINESS"]',
            ], 4000);
            if (reasonLabel) await reasonLabel.click().catch(() => {});
            if (reasonRadio) await reasonRadio.check({ force: true, timeout: 3000 }).catch(() => {});
            await page.waitForTimeout(600);
          }

          const continueBtn = !stoppedEarly ? await firstVisible([
            'a[aria-label="Continue"]',
            'a[role="button"][aria-label*="Continue" i]',
            'button[aria-label="Continue"]',
            'a:has-text("Continue")',
            'input[type="submit"][value*="Continue" i]',
            'button:has-text("Continue")',
          ], 10000) : null;
          if (!stoppedEarly && !continueBtn) {
            stoppedEarly = true;
            stopReason = 'unexpected — no Continue button after LLC selection';
          } else if (!stoppedEarly && continueBtn) {
            const beforeAdvanceUrl = page.url();
            await continueBtn.click({ timeout: 8000 }).catch(() => {});
            await settle();
            // Verify we actually moved off Step 1. IRS /applyein/ is a SPA —
            // URL often stays similar but the step indicator body text flips
            // from "Legal Structure active" to "Identity active".
            const advanced = await page.waitForFunction(() => {
              const t = document.body?.innerText || '';
              // If we see "Identity active" somewhere that means we moved on.
              // "Legal Structure active" still being present means we're stuck.
              return /\bIdentity\s+active\b/i.test(t)
                && !/\bLegal\s+Structure\s+active\b/i.test(t);
            }, { timeout: 10000 }).then(() => true).catch(() => false);
            if (!advanced) {
              stoppedEarly = true;
              stopReason = `stuck on Legal Structure after LLC+Continue — URL ${page.url()}`;
              await captureStep('step1-stuck');
            } else {
              await captureStep('step2-identity');
            }
          }
        }
      }
    } catch (err) {
      stoppedEarly = true;
      stopReason = `unexpected — navigation error: ${(err?.message || String(err)).slice(0, 200)}`;
      try { await captureStep('error'); } catch { /* ignore */ }
    }

    const step_label = stoppedEarly ? (stopReason || 'unexpected') : 'step2-identity';

    // Dump the final page state. Best-effort — errors are swallowed so the
    // operator still gets whatever we captured.
    const form_fields = await page.evaluate(() => {
      function nearestLabel(el) {
        // <label for=id>
        if (el.id) {
          const l = document.querySelector(`label[for="${CSS.escape(el.id)}"]`);
          if (l) return (l.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 200) || null;
        }
        // Ancestor <label>
        let cur = el.parentElement;
        for (let i = 0; i < 5 && cur; i++, cur = cur.parentElement) {
          if (cur.tagName === 'LABEL') {
            return (cur.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 200) || null;
          }
        }
        // Walk up a few ancestors looking for short text content near the input.
        cur = el.parentElement;
        for (let i = 0; i < 3 && cur; i++, cur = cur.parentElement) {
          const txt = (cur.textContent || '').trim().replace(/\s+/g, ' ');
          if (txt && txt.length < 240) return txt.slice(0, 200);
        }
        return null;
      }
      const out = [];
      document.querySelectorAll('input, select, textarea').forEach((raw) => {
        const el = raw;
        const tag = el.tagName.toLowerCase();
        // Skip hidden inputs — operator doesn't need to eyeball CSRF tokens.
        const type = el.getAttribute('type');
        if (tag === 'input' && (type === 'hidden' || type === 'submit' || type === 'button' || type === 'image' || type === 'reset')) return;
        const entry = {
          tag,
          name: el.getAttribute('name'),
          id: el.getAttribute('id'),
          type: tag === 'input' ? (type || 'text') : null,
          label: nearestLabel(el),
          placeholder: el.getAttribute('placeholder'),
          value: el.value ?? null,
        };
        if (tag === 'select') {
          entry.options = Array.from(el.options || []).map((o) => ({
            value: o.value,
            text: (o.textContent || '').trim().slice(0, 200),
            selected: !!o.selected,
          }));
        }
        out.push(entry);
      });
      return out;
    }).catch(() => []);

    const nav_links = await page.evaluate(() => {
      const seen = new Set();
      const out = [];
      document.querySelectorAll('a, button, input[type="submit"], input[type="button"]').forEach((raw) => {
        const el = raw;
        if (el.offsetParent === null && (el.offsetWidth === 0 || el.offsetHeight === 0)) return;
        const tag = el.tagName.toLowerCase();
        const text = (el.value || el.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 120);
        if (!text) return;
        const href = el.getAttribute('href') || null;
        const key = `${tag}|${text}|${href || ''}`;
        if (seen.has(key)) return;
        seen.add(key);
        out.push({ tag, text, href });
      });
      return out;
    }).catch(() => []);

    const body_text_excerpt = await page.evaluate(() => (document.body?.innerText || '').slice(0, 8000)).catch(() => '');
    const html_excerpt = await page.evaluate(() => document.documentElement.outerHTML.slice(0, 40000)).catch(() => '');

    return {
      portal: 'irs_apply_ein_inspect',
      url: page.url(),
      step_label,
      steps_visited,
      form_fields,
      nav_links,
      body_text_excerpt,
      html_excerpt,
      operating_hours: hours,
    };
  },

  // ─── irs_apply_ein_dryrun ───────────────────────────────────────────────
  // Fill the IRS EIN Assistant from Step 1 through Step 4, stopping on the
  // Review & Submit page. Returns a confirmation_token (caller-side opaque
  // value to pair with a future irs_apply_ein_submit), screenshots of every
  // step, and the visible Review page summary so an operator can verify
  // before triggering the live submit. NEVER clicks the final Submit.
  async irs_apply_ein_dryrun(page, { payload, confirmation_token, jobId } = {}) {
    const tag = jobId || confirmation_token || 'dryrun';
    const outDir = path.join(OUT, 'irs-ein', tag);
    fs.mkdirSync(outDir, { recursive: true });
    const hours = irsEinOnlineHoursOpen();
    if (!hours.open) throw new Error(`IRS EIN online closed: ${hours.reason}`);
    if (!payload) throw new Error('irs_apply_ein_dryrun: payload required');
    if (!confirmation_token) throw new Error('irs_apply_ein_dryrun: confirmation_token required (generate caller-side, return to operator for the submit step)');
    const result = await fillIrsEinForm(page, payload, { stopAtReview: true, outDir, tag });
    return {
      portal: 'irs_apply_ein_dryrun',
      confirmation_token,
      operating_hours: hours,
      ...result,
    };
  },

  // ─── irs_apply_ein_submit ───────────────────────────────────────────────
  // Re-fill the form end to end and click Submit. Returns the EIN string
  // and CP 575 PDF as base64. The IRS allows one EIN per responsible-party
  // SSN per 24h window — the caller MUST gate this behind a fresh dryrun
  // confirmation_token + operator approval. Worker does not enforce that
  // gate: app side (state-filing-worker) does.
  async irs_apply_ein_submit(page, { payload, confirmation_token, jobId } = {}) {
    const tag = jobId || confirmation_token || 'submit';
    const outDir = path.join(OUT, 'irs-ein', tag);
    fs.mkdirSync(outDir, { recursive: true });
    const hours = irsEinOnlineHoursOpen();
    if (!hours.open) throw new Error(`IRS EIN online closed: ${hours.reason}`);
    if (!payload) throw new Error('irs_apply_ein_submit: payload required');
    if (!confirmation_token) throw new Error('irs_apply_ein_submit: confirmation_token required (must match the dryrun token the operator approved)');
    const result = await fillIrsEinForm(page, payload, { stopAtReview: false, outDir, tag });
    return {
      portal: 'irs_apply_ein_submit',
      confirmation_token,
      operating_hours: hours,
      ...result,
    };
  },

  // ── consumidor.gov.br (ported from Claims HQ govbr adapter) ──────────────
  async govbr(page, { claimId, action, body, companyName, bookingRef, desiredOutcome }) {
    const dashboard = 'https://www.consumidor.gov.br/pages/principal/';
    if (action === 'read_status') {
      // After consumidor.gov.br UI rebuild (2026-04 onward), authenticated
      // user's reclamação inbox lives at /pages/reclamacao/consumidor/consultar/
      // (was /pages/reclamacao/minhas/ — now 404). Requires fresh session
      // cookies; refresh via Cookie-Editor → /api/portals/credentials/govbr/from-cookie-editor
      await page.goto('https://consumidor.gov.br/pages/reclamacao/consumidor/consultar/', { waitUntil: 'domcontentloaded', timeout: 60000 });
      await page.waitForTimeout(3500);
      const shot = path.join(OUT, `${claimId}-govbr-status.png`);
      await page.screenshot({ path: shot, fullPage: true });
      const html = await page.content();
      // Check for login redirect (session dead)
      const loggedOut = /login\.obrigatorio|Acesso como Consumidor|Entrar com gov\.br/.test(html) && !/Minhas|reclamac/i.test(html);
      if (loggedOut) {
        return { portal: 'govbr', action, preview: shot, error: 'session expired — re-paste cookies via Cookie-Editor' };
      }
      // Try multiple selectors: rows, list items, cards
      const rows = await page.locator('table tr, .reclamacao-item, [class*="reclamacao"], [class*="card"]').allInnerTexts().catch(() => []);
      return { portal: 'govbr', action, preview: shot, url: page.url(), rows: rows.slice(0, 30) };
    }
    if (action === 'submit_new' || !action) {
      await page.goto('https://www.consumidor.gov.br/pages/reclamacao/nova/', { waitUntil: 'domcontentloaded', timeout: 60000 });
      await page.waitForTimeout(2000);
      const before = path.join(OUT, `${claimId}-govbr-form-loaded.png`);
      await page.screenshot({ path: before, fullPage: true });
      await page.locator('input[name*="empresa"], input[placeholder*="empresa" i]').first().fill(companyName ?? '').catch(() => {});
      await page.locator('input[name*="protocolo"], input[name*="pedido"], input[placeholder*="pedido" i]').first().fill(bookingRef ?? '').catch(() => {});
      await page.locator('textarea[name*="relato"], textarea[name*="descricao"]').first().fill(body ?? '').catch(() => {});
      await page.locator('textarea[name*="pedido"], input[name*="solicitacao"]').first().fill(desiredOutcome ?? '').catch(() => {});
      const filled = path.join(OUT, `${claimId}-govbr-filled.png`);
      await page.screenshot({ path: filled, fullPage: true });
      return { portal: 'govbr', action: 'submit_new', before, filled, note: 'form filled; user must click enviar to submit' };
    }
    return { portal: 'govbr', error: `unknown govbr action: ${action}` };
  },

  // ── Wise: open another business account under an existing membership ─────
  // Requires the AdsPower profile to be already logged into Wise as the
  // membership owner. Walks Steps 1–7 of the "Open another business account"
  // flow deterministically. Stops at Step 8 (director ID + docs) and returns
  // the captured state so the caller can either complete it manually or
  // dispatch the upload step in a follow-up call once docs are attached.
  //
  // Payload shape:
  //   { jobId, entity: {
  //       company_type: 'LLC' | 'Inc' | ...
  //       legal_name, dba?: string,
  //       ein,
  //       address: { street, city, state, zip, country: 'US' }
  //       trading_address: { street, city, state, zip, country },
  //       industry: { sector, industry, size },
  //       website, wise_plan: 'advanced'|'basic', wise_purpose: 'both'|... }
  //     stop_after_step?: 7   // default 7; for dryrun set to 1..7 to inspect
  //   }
  async wise_open_business_account(page, payload = {}) {
    const tag = payload.jobId || 'adhoc';
    const entity = payload.entity || {};
    const stopAfter = payload.stop_after_step ?? 7;
    const outDir = path.join(OUT, 'wise-open-business', tag);
    fs.mkdirSync(outDir, { recursive: true });
    const steps_visited = [];
    const errors = [];

    const safeLabel = (s) => String(s).replace(/[^a-zA-Z0-9_-]+/g, '_').slice(0, 60);
    async function captureStep(label) {
      const idx = steps_visited.length;
      const file = path.join(outDir, `step-${idx}-${safeLabel(label)}.png`);
      try { await page.screenshot({ path: file, fullPage: true }); } catch { /* ignore */ }
      steps_visited.push({ step_label: label, url: page.url(), screenshot_path: file });
    }
    async function settle() {
      await page.waitForLoadState('domcontentloaded', { timeout: 20000 }).catch(() => {});
      await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(() => {});
      await page.waitForTimeout(800);
    }
    async function firstVisible(selectors, perTimeout = 3000) {
      for (const sel of selectors) {
        const loc = typeof sel === 'function' ? sel() : page.locator(sel).first();
        try {
          await loc.waitFor({ state: 'visible', timeout: perTimeout });
          return loc;
        } catch { /* try next */ }
      }
      return null;
    }
    // Wise uses combobox-like custom selects. Strategy: click the combobox by
    // its accessible label, then click an option whose text matches.
    async function pickFromCombobox(labelRe, optionText) {
      const candidates = [
        () => page.getByRole('combobox', { name: labelRe }),
        () => page.getByLabel(labelRe),
        `label:has-text("${optionText}")`,
      ];
      const combo = await firstVisible(candidates, 4000);
      if (!combo) throw new Error(`combobox with label ${labelRe} not visible`);
      await combo.click({ timeout: 5000 });
      await page.waitForTimeout(300);
      // Type to filter the dropdown if it accepts text
      try { await combo.fill(optionText); } catch { /* not all comboboxes accept fill */ }
      await page.waitForTimeout(300);
      const opt = await firstVisible([
        `[role="option"]:has-text("${optionText}")`,
        `li:has-text("${optionText}")`,
        `button:has-text("${optionText}")`,
      ], 4000);
      if (!opt) throw new Error(`option "${optionText}" not visible after opening ${labelRe}`);
      await opt.click({ timeout: 5000 });
    }
    async function fillByLabel(labelRe, value) {
      const inp = await firstVisible([
        () => page.getByLabel(labelRe),
        `input[aria-label*="${labelRe.source ?? labelRe}" i]`,
      ], 4000);
      if (!inp) throw new Error(`input ${labelRe} not visible`);
      await inp.fill(String(value ?? ''));
    }
    async function clickPrimary(textRe) {
      const btn = await firstVisible([
        () => page.getByRole('button', { name: textRe }),
        `button:has-text("${textRe.source ?? textRe}")`,
      ], 5000);
      if (!btn) throw new Error(`primary button ${textRe} not visible`);
      await btn.click({ timeout: 5000 });
    }

    // Verify session is alive on Wise — bail early if not.
    await page.goto('https://wise.com/home', { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
    await settle();
    const onLogin = /\/login|\/(?:registration|auth)/.test(page.url());
    if (onLogin) {
      throw new Error(`Wise session expired — log in manually on AdsPower profile, then re-run job ${tag}`);
    }
    await captureStep('home');

    // Entry point: "Open another business account" is reachable from the profile
    // switcher dropdown. URL is reasonably stable.
    await page.goto('https://wise.com/registration?type=business', { waitUntil: 'domcontentloaded', timeout: 45000 });
    await settle();
    await captureStep('registration-entry');

    try {
      // ── Step 1: country ───────────────────────────────────────────────────
      const country = entity.address?.country === 'US' ? 'United States' : entity.address?.country;
      await pickFromCombobox(/where is your business located/i, country);
      await clickPrimary(/Continue/i);
      await settle();
      await captureStep('after-step-1-country');
      if (stopAfter < 2) return ok();

      // ── Step 2: company type, legal name, EIN ─────────────────────────────
      const companyTypeText = entity.company_type === 'LLC'
        ? 'Limited Liability Company (LLC)'
        : entity.company_type === 'Inc'
          ? 'For-Profit Corporation (Inc)'
          : entity.company_type;
      await pickFromCombobox(/company type/i, companyTypeText);
      await fillByLabel(/Registered business name/i, entity.legal_name);
      if (entity.dba) await fillByLabel(/Trading Business Name/i, entity.dba);
      await fillByLabel(/EIN number/i, entity.ein);
      await clickPrimary(/Save and continue/i);
      await settle();
      await captureStep('after-step-2-business-details');
      if (stopAfter < 3) return ok();

      // ── Step 3: registered address ────────────────────────────────────────
      // Country is usually pre-selected from step 1; only re-pick if mismatch.
      await fillByLabel(/^Address$/i, entity.address?.street);
      await fillByLabel(/^City$/i, entity.address?.city);
      if (entity.address?.state) {
        await pickFromCombobox(/^State$/i, entity.address.state).catch(async () => {
          await fillByLabel(/^State$/i, entity.address.state);
        });
      }
      await fillByLabel(/ZIP code|Postal code/i, entity.address?.zip);
      await clickPrimary(/Save and continue/i);
      await settle();
      await captureStep('after-step-3-address');
      if (stopAfter < 4) return ok();

      // ── Step 4: industry ──────────────────────────────────────────────────
      await pickFromCombobox(/Business sector/i, entity.industry?.sector);
      await pickFromCombobox(/^Industry$/i, entity.industry?.industry);
      await pickFromCombobox(/Size of your business/i, entity.industry?.size);
      await clickPrimary(/Save and continue/i);
      await settle();
      await captureStep('after-step-4-industry');
      if (stopAfter < 5) return ok();

      // ── Step 5: website ───────────────────────────────────────────────────
      await fillByLabel(/Business website|social media|e-commerce link/i, entity.website);
      await clickPrimary(/^Continue$/i);
      await settle();
      await captureStep('after-step-5-website');
      if (stopAfter < 6) return ok();

      // ── Step 6: plan ──────────────────────────────────────────────────────
      // Click "Get Advanced" or "Get Basic" depending on the entity preference.
      const planLabel = entity.wise_plan === 'basic' ? /Get Basic/i : /Get Advanced/i;
      await clickPrimary(planLabel);
      await settle();
      await captureStep('after-step-6-plan');
      if (stopAfter < 7) return ok();

      // ── Step 7: purpose ───────────────────────────────────────────────────
      const purposeLabel = {
        both: 'Both paying suppliers and contractors, and receiving money for goods and services',
        receiving: 'Receiving money for services / products',
        paying: 'Paying suppliers / contractors / employees',
        intra_company: 'Making intra-company transfers',
      }[entity.wise_purpose ?? 'both'];
      await pickFromCombobox(/using Wise for/i, purposeLabel);
      await clickPrimary(/Continue|Save and continue/i);
      await settle();
      await captureStep('after-step-7-purpose');

      // Step 8+ (director identity, source of funds, $31 card payment, doc
      // upload, ToS, final review) is intentionally not implemented yet —
      // capture the current state and return so the operator can finish, OR
      // we extend this handler in a follow-up after the BCAX dryrun.
      await captureStep('paused-at-step-8-todo');
      return ok();
    } catch (e) {
      errors.push({ step: steps_visited.length, message: e.message, stack: e.stack?.split('\n').slice(0, 4).join('\n') });
      await captureStep(`error-${e.message.slice(0, 30)}`);
      return ok();
    }

    function ok() {
      return {
        portal: 'wise_open_business_account',
        job_id: tag,
        completed_steps: steps_visited.length,
        steps_visited,
        stopped_at_step: steps_visited[steps_visited.length - 1]?.step_label,
        errors,
        // Phase 2: parse the wise URL once a profile is created — the URL
        // becomes /home/<membershipId>/<businessProfileId>.
        wise_profile_id: parseWiseProfileIdFromUrl(page.url()),
      };
    }
  },
};

// Best-effort: extract the new business profile id from the post-creation URL
// (Wise routes to /home/<membershipId>/<businessProfileId>/...). Returns null
// if we're not on a recognizable URL yet.
function parseWiseProfileIdFromUrl(url) {
  try {
    const m = String(url).match(/\/home\/(P\d+)\/(\d+)/);
    return m ? m[2] : null;
  } catch {
    return null;
  }
}

// Best-effort: extract Mercury account id from URL like /accounts/<id>/...
function parseMercuryAccountIdFromUrl(url) {
  try {
    const m = String(url).match(/\/accounts\/([a-zA-Z0-9_-]+)/);
    return m ? m[1] : null;
  } catch {
    return null;
  }
}

// Add Mercury handler to the same handlers object.
handlers.mercury_open_business_account = async function (page, payload = {}) {
  const tag = payload.jobId || 'adhoc';
  const entity = payload.entity || {};
  const stopAfterStep = payload.stop_after_step ?? 99; // run until human-needed step
  const outDir = path.join(OUT, 'mercury-open-business', tag);
  fs.mkdirSync(outDir, { recursive: true });
  const steps_visited = [];
  const errors = [];

  const safeLabel = (s) => String(s).replace(/[^a-zA-Z0-9_-]+/g, '_').slice(0, 60);
  async function captureStep(label) {
    const idx = steps_visited.length;
    const file = path.join(outDir, `step-${idx}-${safeLabel(label)}.png`);
    try { await page.screenshot({ path: file, fullPage: true }); } catch { /* ignore */ }
    steps_visited.push({ step_label: label, url: page.url(), screenshot_path: file });
  }
  async function settle() {
    await page.waitForLoadState('domcontentloaded', { timeout: 20000 }).catch(() => {});
    await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(() => {});
    await page.waitForTimeout(800);
  }
  async function firstVisible(selectors, perTimeout = 3000) {
    for (const sel of selectors) {
      const loc = typeof sel === 'function' ? sel() : page.locator(sel).first();
      try {
        await loc.waitFor({ state: 'visible', timeout: perTimeout });
        return loc;
      } catch { /* try next */ }
    }
    return null;
  }
  async function fillByLabel(labelRe, value) {
    const inp = await firstVisible([
      () => page.getByLabel(labelRe),
      `input[aria-label*="${labelRe.source ?? labelRe}" i]`,
      `input[name*="${labelRe.source ?? labelRe}" i]`,
    ], 4000);
    if (!inp) throw new Error(`input ${labelRe} not visible`);
    await inp.fill(String(value ?? ''));
  }
  async function clickPrimary(textRe) {
    const btn = await firstVisible([
      () => page.getByRole('button', { name: textRe }),
      `button:has-text("${textRe.source ?? textRe}")`,
    ], 5000);
    if (!btn) throw new Error(`primary button ${textRe} not visible`);
    await btn.click({ timeout: 5000 });
  }

  // Verify Mercury session is alive — bail if at /login.
  await page.goto('https://app.mercury.com/', { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
  await settle();
  if (/\/login|\/signin|\/auth/.test(page.url())) {
    throw new Error(`Mercury session expired — log in manually on AdsPower profile, then re-run job ${tag}`);
  }
  await captureStep('home');

  try {
    // Mercury entry point for "open another account" — exact URL TBD during
    // first dryrun. Try the most likely paths.
    const entryCandidates = [
      'https://app.mercury.com/accounts/new',
      'https://app.mercury.com/onboarding/business',
      'https://app.mercury.com/team/onboard-new-entity',
    ];
    for (const url of entryCandidates) {
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
      await settle();
      if (!/404|not.found/i.test(await page.title().catch(() => ''))) break;
    }
    await captureStep('mercury-onboarding-entry');

    // Steps below are best-effort placeholders. The real Mercury onboarding
    // flow varies — capture screenshots during the BCAX dryrun and drop in
    // the right selectors. Until then, the handler walks as far as it can
    // and stops at the first unrecognized form.
    if (stopAfterStep < 1) return ok();

    // Likely first ask: legal entity name
    await fillByLabel(/legal name|business name|company name/i, entity.legal_name).catch(() => {});
    await fillByLabel(/EIN|tax id/i, entity.ein).catch(() => {});
    await captureStep('legal-name-and-ein');
    if (stopAfterStep < 2) return ok();

    await clickPrimary(/Continue|Next/i).catch(() => {});
    await settle();
    await captureStep('after-step-1');

    // Address
    await fillByLabel(/address|street/i, entity.address?.street).catch(() => {});
    await fillByLabel(/city/i, entity.address?.city).catch(() => {});
    await fillByLabel(/state/i, entity.address?.state).catch(() => {});
    await fillByLabel(/zip|postal/i, entity.address?.zip).catch(() => {});
    await captureStep('address-filled');
    if (stopAfterStep < 3) return ok();

    await clickPrimary(/Continue|Next/i).catch(() => {});
    await settle();
    await captureStep('after-address');

    // Director identity, source of funds, expected volume — these are the
    // human-needed steps (selfie/ID upload, YubiKey, 1Password). Worker
    // captures the screen and returns; user finishes manually OR in a
    // follow-up after we decode the rest.
    await captureStep('paused-awaiting-human');
    return ok();
  } catch (e) {
    errors.push({ step: steps_visited.length, message: e.message });
    await captureStep(`error-${e.message.slice(0, 30)}`);
    return ok();
  }

  function ok() {
    return {
      portal: 'mercury_open_business_account',
      job_id: tag,
      completed_steps: steps_visited.length,
      steps_visited,
      stopped_at_step: steps_visited[steps_visited.length - 1]?.step_label,
      errors,
      mercury_account_id: parseMercuryAccountIdFromUrl(page.url()),
      // Mercury sometimes returns a magic link for director liveness/selfie
      // mid-flow. We surface anything that looks like a verification URL so
      // the BCA app can forward it to the customer.
      possible_kyc_url: extractKycLinkFromBody(),
    };

    function extractKycLinkFromBody() {
      // Quick best-effort scan: read text content for an https link that
      // mentions "verify" or "kyc". Enough to bootstrap the forwarding flow.
      try {
        const text = ''; // future: await page.evaluate(...)
        const m = text.match(/https?:\/\/[^\s)"']+(verify|kyc|identity)[^\s)"']*/i);
        return m ? m[0] : null;
      } catch {
        return null;
      }
    }
  }
};

// ── WhatsApp Web (personal account, paired once via QR on a dedicated AdsPower profile) ──
// Sends a message to a phone number through web.whatsapp.com. Phone is normalized
// to digits-only (no leading +). Caller MUST run this against an AdsPower profile
// where the user has scanned the QR code on their phone at least once.
handlers.wa_send = async function (page, { to, body, claimId }) {
  if (!to) throw new Error('wa_send: "to" is required');
  if (!body) throw new Error('wa_send: "body" is required');
  const digits = String(to).replace(/\D/g, '');
  if (digits.length < 7) throw new Error(`wa_send: invalid phone "${to}"`);

  const url = `https://web.whatsapp.com/send?phone=${digits}&text=${encodeURIComponent(body)}`;
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
  await page.waitForTimeout(5000);

  // Detect "phone not on WhatsApp" modal across locales.
  const notOnWa = page.getByText(/isn.?t on WhatsApp|no\s+es\s+un\s+usuario|invalid|inv[aá]lido|Could not contact|n[uú]mero/i);
  if (await notOnWa.first().isVisible().catch(() => false)) {
    const errShot = path.join(OUT, `${claimId || 'wa'}-wa-error.png`);
    await page.screenshot({ path: errShot, fullPage: true });
    // Be a bit forgiving — only throw if we ALSO can't find a message input.
    const inputProbe = await page.locator('div[contenteditable="true"]').count();
    if (inputProbe === 0) {
      throw new Error(`wa_send: phone ${digits} not on WhatsApp (or chat failed to open)`);
    }
  }

  // Locate the message-input contenteditable. WA Web ships several selector variants.
  const input = page.locator(
    'footer div[contenteditable="true"], div[contenteditable="true"][data-tab="10"], div[contenteditable="true"][title="Type a message" i]',
  ).last();
  await input.waitFor({ state: 'visible', timeout: 30000 });

  // The text is prefilled from the URL on most loads, but not all. If empty, type it.
  const currentText = await input.innerText().catch(() => '');
  if (!currentText || currentText.trim().length < 3) {
    await input.click();
    await page.keyboard.type(body, { delay: 6 });
    await page.waitForTimeout(400);
  }

  // Click send. Most stable selector across versions: span[data-icon="send"].
  const sendBtn = page.locator(
    'span[data-icon="send"], button[aria-label="Send" i], button[data-tab="11"], [data-testid="send"]',
  ).first();
  await sendBtn.waitFor({ state: 'visible', timeout: 15000 });
  await sendBtn.click();

  // Confirm: wait until input is empty again (a strong signal the message left).
  try {
    await page.waitForFunction(
      () => {
        const el = document.querySelector('footer div[contenteditable="true"]');
        return el && (el.innerText || '').trim().length === 0;
      },
      { timeout: 10000 },
    );
  } catch { /* fall through; screenshot will reveal */ }
  await page.waitForTimeout(1500);

  const shot = path.join(OUT, `${claimId || 'wa'}-wa-sent.png`);
  await page.screenshot({ path: shot, fullPage: true });

  return { portal: 'wa_send', to: digits, sent_at: new Date().toISOString(), screenshot: shot };
};

// ── App ─────────────────────────────────────────────────────────────────────
const app = express();
app.use(express.json({ limit: '2mb' }));

app.get('/health', (_, res) => res.json({ ok: true, auth: AUTH_DISABLED ? 'disabled' : 'enabled' }));

app.get('/status', auth(), async (req, res) => {
  const ready = await adsReachable();
  res.json({
    ok: true,
    role: req.role,
    adspower: { reachable: ready, url: ADS },
    worker: { port: PORT, node: process.version, uptime_s: Math.round(process.uptime()) },
  });
});

app.post('/submit', auth(), async (req, res) => {
  const { profileId, portal, useAdsPower = true, ...rest } = req.body ?? {};
  const fn = handlers[portal];
  if (!fn) return res.status(400).json({ error: `unknown portal: ${portal}`, known: Object.keys(handlers) });
  try {
    const out = await withSession(profileId, (p) => fn(p, rest), useAdsPower);
    res.json({ ok: true, ...out });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// List AdsPower profiles (for picking profile IDs in app UIs).
app.get('/adspower/profiles', auth(), async (_req, res) => {
  try {
    const data = await ads('/api/v1/user/list?page=1&page_size=100');
    res.json({ ok: true, profiles: data.list ?? [] });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Ensure the AdsPower desktop app is running. If it's closed, start it; then
// wait for the local API to come back online.
app.post('/adspower/ensure-running', auth(), async (_req, res) => {
  if (await adsReachable()) return res.json({ ok: true, already_running: true });
  const exe = process.env.ADSPOWER_EXE ?? 'C:\\Program Files (x86)\\AdsPower Global\\AdsPower Global.exe';
  try {
    spawn(exe, [], { detached: true, stdio: 'ignore', windowsHide: false }).unref();
  } catch (e) {
    return res.status(500).json({ error: `spawn failed: ${e.message}` });
  }
  // Poll up to 30s for API to respond.
  for (let i = 0; i < 15; i++) {
    await new Promise((r) => setTimeout(r, 2000));
    if (await adsReachable()) return res.json({ ok: true, started: true, waited_ms: (i + 1) * 2000 });
  }
  res.status(504).json({ error: 'AdsPower did not come online within 30s' });
});

// Serve a screenshot or HTML dump from SCREENSHOT_DIR. Path traversal is blocked
// by rejecting any name containing `/`, `\`, or `..`. Returns 404 if missing.
app.get('/screenshot/:name', auth(), (req, res) => {
  const name = req.params.name;
  if (!name || /[\\/]|\.\./.test(name)) return res.status(400).json({ error: 'invalid name' });
  const full = path.join(OUT, name);
  if (!fs.existsSync(full)) return res.status(404).json({ error: 'not found' });
  const ext = path.extname(name).toLowerCase();
  const mime = ext === '.png' ? 'image/png'
    : ext === '.jpg' || ext === '.jpeg' ? 'image/jpeg'
    : ext === '.html' ? 'text/html; charset=utf-8'
    : 'application/octet-stream';
  res.setHeader('Content-Type', mime);
  res.sendFile(full);
});

// List screenshot files (newest first). Useful for "what did my last probe capture?".
app.get('/screenshots', auth(), (_req, res) => {
  try {
    const files = fs.readdirSync(OUT)
      .map((n) => ({ name: n, mtime: fs.statSync(path.join(OUT, n)).mtimeMs, size: fs.statSync(path.join(OUT, n)).size }))
      .sort((a, b) => b.mtime - a.mtime)
      .slice(0, 100);
    res.json({ ok: true, files });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Remote command execution. ADMIN TOKEN ONLY. Runs in PowerShell on Windows.
// Use sparingly — primarily for "restart worker", "tail logs", "start AdsPower".
app.post('/exec', auth('admin'), async (req, res) => {
  const { cmd, cwd, timeout_ms = 30000, shell = 'powershell' } = req.body ?? {};
  if (!cmd || typeof cmd !== 'string') return res.status(400).json({ error: 'cmd required (string)' });
  const id = crypto.randomBytes(4).toString('hex');
  const started = Date.now();
  try {
    // PowerShell via -EncodedCommand (UTF-16 LE base64) bypasses all cmd.exe escaping.
    // cmd shell path stays primitive for rare cases that need it.
    let fullCmd;
    if (shell === 'cmd') {
      fullCmd = cmd;
    } else {
      const encoded = Buffer.from(cmd, 'utf16le').toString('base64');
      fullCmd = `powershell -NoProfile -EncodedCommand ${encoded}`;
    }
    const { stdout, stderr } = await exec(fullCmd, { cwd, timeout: timeout_ms, windowsHide: true, maxBuffer: 8 * 1024 * 1024 });
    res.json({ ok: true, id, duration_ms: Date.now() - started, stdout, stderr });
  } catch (e) {
    res.status(500).json({ ok: false, id, duration_ms: Date.now() - started, error: e.message, stdout: e.stdout, stderr: e.stderr, code: e.code });
  }
});

// ─── /redeploy: spawn detached PowerShell so worker can restart itself ──────
const REDEPLOY_BOOTSTRAP = process.env.REDEPLOY_BOOTSTRAP_PATH
  || 'C:\\spexx-cloud-ops\\contabo\\bootstrap.ps1';
const REDEPLOY_LOGS_DIR  = process.env.REDEPLOY_LOGS_DIR
  || 'C:\\spexx-state\\logs';

app.post('/redeploy', auth(), (req, res) => {
  const args = ['-ExecutionPolicy', 'Bypass', '-File', REDEPLOY_BOOTSTRAP];
  if (req.body?.service) { args.push('-Service', String(req.body.service)); }
  if (req.body?.phase)   { args.push('-Phase',   String(req.body.phase));   }

  const child = spawn('powershell.exe', args, {
    detached: true,
    stdio: 'ignore',
    windowsHide: true,
  });
  child.unref();

  res.status(202).json({
    accepted: true,
    pid: child.pid,
    bootstrap: REDEPLOY_BOOTSTRAP,
    logsDir: REDEPLOY_LOGS_DIR,
  });
});

app.get('/redeploy/status', auth(), (_req, res) => {
  try {
    const files = readdirSync(REDEPLOY_LOGS_DIR)
      .filter(f => /^bootstrap-\d{8}-\d{6}\.log$/.test(f))
      .map(f => ({ f, mtime: statSync(join(REDEPLOY_LOGS_DIR, f)).mtimeMs }))
      .sort((a, b) => b.mtime - a.mtime);
    if (!files.length) return res.json({ latest: null });
    const latest = files[0].f;
    const body = readFileSync(join(REDEPLOY_LOGS_DIR, latest), 'utf8');
    const tail = body.split(/\r?\n/).slice(-50).join('\n');
    res.json({ latest, mtime: new Date(files[0].mtime).toISOString(), tail });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`contabo-worker on :${PORT} — auth ${AUTH_DISABLED ? 'DISABLED (dev)' : 'enabled'}`);
});
