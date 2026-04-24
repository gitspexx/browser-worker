import express from 'express';
import { chromium } from 'playwright';
import fs from 'fs';
import path from 'path';
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
  // Chrome attached over CDP doesn't have Playwright's acceptDownloads flag set,
  // so download events fire but the stream is canceled before Playwright can
  // consume it. Set browser-wide download behavior explicitly to a known dir.
  try {
    const downloadDir = path.join(OUT, '..', 'downloads', profileId);
    await fs.promises.mkdir(downloadDir, { recursive: true }).catch(() => {});
    const cdp = await ctx.newCDPSession(page);
    await cdp.send('Browser.setDownloadBehavior', {
      behavior: 'allow',
      downloadPath: downloadDir,
    }).catch((err) => console.warn(`[withSession] setDownloadBehavior failed: ${err.message}`));
  } catch (err) {
    console.warn(`[withSession] download setup failed: ${err.message}`);
  }
  try {
    return await fn(page);
  } finally {
    await browser.close().catch(() => {});
    await fetch(`${ADS}/api/v1/browser/stop?user_id=${profileId}`).catch(() => {});
  }
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
  if (stillLogin) throw new Error('wyreg login failed — check credentials, 2FA, or CAPTCHA');
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
      const [download] = await Promise.all([
        page.waitForEvent('download', { timeout: 30000 }),
        downloadBtn.click({ timeout: 8000 }),
      ]);
      // Wait for Playwright to fully persist the file to disk, then read it
      // from the path. createReadStream races with page navigation and cancels
      // mid-stream — download.path() blocks until the download is complete.
      const buf = await drainDownload(download);
      return buf;
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
    for (let i = 0; i < plan.length; i++) {
      if (page.url() !== listUrl) {
        await page.goto(listUrl, { waitUntil: 'domcontentloaded', timeout: 15000 }).catch(() => {});
        await page.waitForLoadState('networkidle', { timeout: 8000 }).catch(() => {});
        await page.waitForTimeout(1500);
      }
      const fresh = await wyregEnumerateDocRows(page);
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

  // ── consumidor.gov.br (ported from Claims HQ govbr adapter) ──────────────
  async govbr(page, { claimId, action, body, companyName, bookingRef, desiredOutcome }) {
    const dashboard = 'https://www.consumidor.gov.br/pages/principal/';
    if (action === 'read_status') {
      await page.goto('https://www.consumidor.gov.br/pages/reclamacao/minhas/', { waitUntil: 'domcontentloaded', timeout: 60000 });
      const shot = path.join(OUT, `${claimId}-govbr-status.png`);
      await page.screenshot({ path: shot, fullPage: true });
      const rows = await page.locator('table tr').allInnerTexts().catch(() => []);
      return { portal: 'govbr', action, preview: shot, rows: rows.slice(0, 20) };
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

app.listen(PORT, '0.0.0.0', () => {
  console.log(`contabo-worker on :${PORT} — auth ${AUTH_DISABLED ? 'DISABLED (dev)' : 'enabled'}`);
});
