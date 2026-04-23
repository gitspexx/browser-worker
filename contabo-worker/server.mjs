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
  try {
    return await fn(page);
  } finally {
    await browser.close().catch(() => {});
    await fetch(`${ADS}/api/v1/browser/stop?user_id=${profileId}`).catch(() => {});
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
    const fullCmd = shell === 'cmd' ? cmd : `powershell -NoProfile -Command "${cmd.replace(/"/g, '\\"')}"`;
    const { stdout, stderr } = await exec(fullCmd, { cwd, timeout: timeout_ms, windowsHide: true, maxBuffer: 8 * 1024 * 1024 });
    res.json({ ok: true, id, duration_ms: Date.now() - started, stdout, stderr });
  } catch (e) {
    res.status(500).json({ ok: false, id, duration_ms: Date.now() - started, error: e.message, stdout: e.stdout, stderr: e.stderr, code: e.code });
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`contabo-worker on :${PORT} — auth ${AUTH_DISABLED ? 'DISABLED (dev)' : 'enabled'}`);
});
