// wyreg_form_llc — form a Wyoming LLC in the wyreg (Corporate Tools) cabinet.
//
// Driven in STAGES, not one long run. The AdsPower watchdog restarts the browser
// whenever its local API misses a probe (every 5 min), which kills the browser and
// every unsaved form field with it. Each stage below is atomic and finishes well
// inside that window, and each one is independently resumable from the id the
// previous stage returned.
//
//   create  -> walks Hire Us steps 1-5, returns biz_id. Charges nothing.
//   cart    -> adds Business Formation Service, returns the checkout total.
//   pay     -> selects the card and places the order. SPENDS MONEY.
//   details -> post-payment wizard: business details, people & roles, submit.
//   ra      -> the separate Registered Agent intake form.
//
// BCA defaults, applied unless the payload overrides them: member managed, the
// client as the named contact but reachable at BCA's email and phone, and
// "Use Our Address" everywhere on the state filing so no real address of ours
// or the client's lands on the public record.

const BASE = 'https://accounts.wyregisteredagent.net';

export const BCA_DEFAULTS = {
  management: 'member',
  contact_email: 'hello@bettercallaxel.com',
  contact_phone: '13079984560',
  use_our_address: true,
  speed: 'standard',
  // The BCAX card. Visa 9855 is a personal Revolut card -- never default to it.
  card_last4: '6754',
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ── interaction primitives ──────────────────────────────────────────────────
// PrimeVue ignores synthetic clicks: el.click(), dispatched MouseEvents and
// Playwright's own locator.click() on the hidden input all resolve happily while
// the radio stays unset and Continue stays disabled. Only a real mouse press at
// the element's centre registers, so every click here goes through the mouse.

let markerSeq = 0;

// Finds one element, scrolls it into view and tags it so we can measure it after
// the scroll without re-running the search. Returns the marker, or null.
const OVERLAY_SEL = '.p-select-overlay,.p-dropdown-panel,.p-select-list-container,.p-overlaypanel,.p-autocomplete-panel,[role="listbox"]';

async function mark(page, { text, selector, tags, nth = 0, visibleOnly = true, prefer, noScroll = false } = {}) {
  const marker = `wf${++markerSeq}`;
  const found = await page.evaluate(
    ({ m, pattern, selStr, tagStr, idx, vis, pref, noScr, ovSel }) => {
      const rx = pattern ? new RegExp(pattern, 'i') : null;
      let els = Array.from(document.querySelectorAll(selStr || tagStr));
      if (rx) {
        els = els.filter((el) => rx.test((el.innerText || el.textContent || '').trim()));
        // Keep only the innermost matches -- a text search otherwise returns the
        // whole page wrapper first, and clicking that hits nothing useful.
        els = els.filter((el) => !els.some((o) => o !== el && el.contains(o)));
      }
      if (vis) {
        els = els.filter((el) => {
          const r = el.getBoundingClientRect();
          const s = getComputedStyle(el);
          return r.width > 2 && r.height > 2 && s.visibility !== 'hidden' && s.display !== 'none'
            && (el.offsetParent !== null || s.position === 'fixed');
        });
      }
      // Overlay rows (a saved card, a dropdown option) nest the label inside the
      // clickable row, so the innermost match can be a span with no handler on it.
      // Shortest-text-wins picks the row itself, which is what actually responds.
      if (pref === 'shortest') {
        els.sort((a, b) => (a.innerText || '').length - (b.innerText || '').length);
      }
      const el = els[idx];
      if (!el) return null;
      el.setAttribute('data-wf', m);
      // Scrolling an element that lives inside an open PrimeVue overlay CLOSES
      // that overlay, so the option is gone before it can be measured or clicked.
      const inOverlay = !!el.closest(ovSel);
      if (!noScr && !inOverlay) el.scrollIntoView({ block: 'center', inline: 'center' });
      return {
        matches: els.length,
        inOverlay,
        text: (el.innerText || el.value || '').slice(0, 90),
        disabled: el.disabled === true || el.getAttribute('aria-disabled') === 'true',
      };
    },
    {
      m: marker,
      pattern: text ? (text instanceof RegExp ? text.source : text) : null,
      selStr: selector || null,
      tagStr: tags || 'button,[role="button"],a,label,li,div,span,td,h3,h4',
      idx: nth,
      vis: visibleOnly,
      pref: prefer ?? null,
      noScr: noScroll,
      ovSel: OVERLAY_SEL,
    },
  );
  return found ? { marker, ...found } : null;
}

async function boxOf(page, marker) {
  return page.evaluate((m) => {
    const el = document.querySelector(`[data-wf="${m}"]`);
    if (!el) return null;
    const r = el.getBoundingClientRect();
    return { x: r.x, y: r.y, w: r.width, h: r.height };
  }, marker);
}

async function clickReal(page, opts) {
  const hit = await mark(page, opts);
  if (!hit) throw new Error(`clickReal: nothing matched ${JSON.stringify(opts)}`);
  await sleep(350);

  // A mouse press lands on whatever is topmost at the point, so a button sitting
  // under the sticky app header gets the header instead -- the click "succeeds"
  // and nothing happens. Hit-test first, and nudge the page if the point is covered.
  const attempts = hit.inOverlay ? 1 : 3;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    const probe = await page.evaluate(({ m, scroll }) => {
      const el = document.querySelector(`[data-wf="${m}"]`);
      if (!el) return null;
      if (scroll) el.scrollIntoView({ block: 'center', inline: 'center' });
      const r = el.getBoundingClientRect();
      const x = r.x + r.width / 2;
      const y = r.y + r.height / 2;
      const top = document.elementFromPoint(x, y);
      return {
        x, y,
        obstructed: top ? !(top === el || el.contains(top) || top.contains(el)) : true,
        topmost: top ? `${top.tagName}.${((top.className || '') + '').slice(0, 40)}` : null,
      };
    }, { m: hit.marker, scroll: !hit.inOverlay });
    if (!probe) throw new Error(`clickReal: ${hit.marker} vanished before the click`);
    if (!probe.obstructed) {
      await page.mouse.move(probe.x, probe.y);
      await page.mouse.click(probe.x, probe.y);
      return { ...hit, via: 'mouse' };
    }
    if (hit.inOverlay) break;
    await page.evaluate(() => window.scrollBy(0, -160));
    await sleep(400);
  }

  // Still covered. A plain button's Vue @click listener does fire on a native
  // el.click() -- it is only the radio cards and dropdown options that ignore
  // synthetic events, and those are never the ones hiding under the header.
  const dispatched = await page.evaluate((m) => {
    const el = document.querySelector(`[data-wf="${m}"]`);
    if (!el) return false;
    (el.closest('button,a,[role="button"]') || el).click();
    return true;
  }, hit.marker);
  if (!dispatched) throw new Error(`clickReal: ${hit.marker} is covered and could not be clicked`);
  return { ...hit, via: 'native-fallback' };
}

// Best-effort click: returns false instead of throwing when the target is absent.
async function clickIf(page, opts) {
  try {
    await clickReal(page, opts);
    return true;
  } catch {
    return false;
  }
}

// Vue-bound inputs need the native value setter. Typing APPENDS -- ctrl+A,
// triple-click and Shift+Home all fail to clear the field, which silently
// produces concatenated values like "Bill Maximiliaan FrancoisAlessandro".
async function setValue(page, opts, value) {
  const hit = await mark(page, { tags: 'input,textarea', visibleOnly: true, ...opts });
  if (!hit) throw new Error(`setValue: no input matched ${JSON.stringify(opts)}`);
  const applied = await page.evaluate(
    ({ m, v }) => {
      const el = document.querySelector(`[data-wf="${m}"]`);
      if (!el) return 'no-element';
      const protos = {
        INPUT: HTMLInputElement.prototype,
        TEXTAREA: HTMLTextAreaElement.prototype,
        SELECT: HTMLSelectElement.prototype,
      };
      const proto = protos[el.tagName];
      // Calling an input's value setter with a non-input `this` throws
      // "Illegal invocation", which reads like a Playwright fault rather than
      // the selector fault it actually is.
      if (!proto) return `not-a-field:${el.tagName}`;
      Object.getOwnPropertyDescriptor(proto, 'value').set.call(el, v);
      el.dispatchEvent(new Event('input', { bubbles: true }));
      el.dispatchEvent(new Event('change', { bubbles: true }));
      return 'ok';
    },
    { m: hit.marker, v: value },
  );
  if (applied !== 'ok') throw new Error(`setValue: ${JSON.stringify(opts)} matched ${applied}`);
  await sleep(250);
  const got = await page.evaluate((m) => document.querySelector(`[data-wf="${m}"]`)?.value ?? null, hit.marker);
  if (got !== value) throw new Error(`setValue: ${JSON.stringify(opts)} holds ${JSON.stringify(got)}, wanted ${JSON.stringify(value)}`);
  return hit;
}

// A PrimeVue dropdown: click to open, click the option in the overlay panel.
async function pickDropdown(page, triggerOpts, optionText) {
  await clickReal(page, triggerOpts);
  await sleep(1400);
  await clickReal(page, {
    text: `^\\s*${optionText}\\s*$`,
    tags: '.p-dropdown-item,.p-select-option,[role="option"],li,span',
    prefer: 'shortest',
    noScroll: true,
  });
  await sleep(900);
}

// Sets a text field identified by its LABEL. The cabinet's PrimeVue inputs have
// no name and no placeholder -- only a generated id such as "pv_id_790" plus a
// <label for>, so the label is the only stable handle.
async function setFieldByLabel(page, labelText, value) {
  const applied = await page.evaluate(({ lbl, v, ovSel }) => {
    const rx = new RegExp(lbl, 'i');
    const labels = Array.from(document.querySelectorAll('label'))
      .filter((l) => rx.test((l.innerText || '').replace(/\*/g, '').trim()))
      .filter((l) => !l.closest(ovSel));
    for (const l of labels) {
      let el = null;
      const forId = l.getAttribute('for');
      if (forId) el = document.getElementById(forId);
      if (!el || !/^(INPUT|TEXTAREA)$/.test(el.tagName)) {
        let node = l;
        for (let up = 0; up < 4 && node && !el; up++) {
          el = node.querySelector('input:not([type="checkbox"]):not([type="radio"]),textarea');
          node = node.parentElement;
        }
      }
      if (!el || !/^(INPUT|TEXTAREA)$/.test(el.tagName) || el.offsetParent === null) continue;
      const proto = el.tagName === 'TEXTAREA' ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
      Object.getOwnPropertyDescriptor(proto, 'value').set.call(el, v);
      el.dispatchEvent(new Event('input', { bubbles: true }));
      el.dispatchEvent(new Event('change', { bubbles: true }));
      return el.value === v ? 'ok' : `wrote-but-holds:${el.value}`;
    }
    return 'no-field';
  }, { lbl: labelText, v: String(value), ovSel: OVERLAY_SEL });
  if (applied !== 'ok') throw new Error(`setFieldByLabel(${labelText}): ${applied}`);
  await sleep(250);
}

// Picks a dropdown identified by its LABEL. A PrimeVue select's innerText is its
// current VALUE ("Paraguay"), never its label, so the label has to be found first
// and the control located underneath it.
async function pickFieldDropdown(page, labelText, optionText) {
  const marked = await page.evaluate(({ lbl, ovSel }) => {
    const rx = new RegExp(lbl, 'i');
    const labels = Array.from(document.querySelectorAll('label,span,div,h6,p'))
      .filter((e) => {
        const t = (e.innerText || '').trim();
        return rx.test(t) && t.length < 40;
      })
      .filter((e) => !e.closest(ovSel));
    for (const l of labels) {
      let node = l;
      for (let up = 0; up < 4 && node; up++) {
        const ctl = node.querySelector('.p-select,.p-dropdown,[role="combobox"],select');
        if (ctl) { ctl.setAttribute('data-wf', 'fielddd'); return true; }
        node = node.parentElement;
      }
    }
    return false;
  }, { lbl: labelText, ovSel: OVERLAY_SEL });
  if (!marked) throw new Error(`pickFieldDropdown: no select under a label matching ${labelText}`);

  // Go through clickReal rather than clicking the measured box directly: it
  // scrolls the control into view first. A control below the fold has a y
  // outside the viewport, and a mouse press there lands on nothing at all.
  await clickReal(page, { selector: '[data-wf="fielddd"]' });
  await sleep(1600);

  // Long option lists (countries, US states) are virtualized: an option that is
  // not scrolled into view is not in the DOM at all, so clicking it can never
  // work. PrimeVue puts a filter input in the overlay for exactly this case.
  const filtered = await page.evaluate(({ q, ovSel }) => {
    const ov = document.querySelector(ovSel);
    if (!ov) return 'no-overlay';
    const inp = ov.querySelector('input.p-select-filter,input.p-dropdown-filter,input[role="searchbox"],input.p-inputtext');
    if (!inp) return 'no-filter';
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(inp, q);
    inp.dispatchEvent(new Event('input', { bubbles: true }));
    inp.dispatchEvent(new Event('change', { bubbles: true }));
    return 'ok';
  }, { q: String(optionText), ovSel: OVERLAY_SEL });
  if (filtered === 'ok') await sleep(1500);

  const esc = String(optionText).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  try {
    await clickReal(page, {
      text: `^\\s*${esc}\\s*$`,
      tags: '.p-select-option,.p-dropdown-item,[role="option"],li,span',
      prefer: 'shortest',
      noScroll: true,
    });
  } catch (e) {
    // Report what the overlay was actually offering -- "option not found" on its
    // own gives no way to tell a wrong name from an unrendered list.
    const visible = await page.evaluate((ovSel) => {
      const ov = document.querySelector(ovSel);
      if (!ov) return ['(overlay closed)'];
      return Array.from(ov.querySelectorAll('li,[role="option"]'))
        .map((e) => (e.innerText || '').trim()).filter(Boolean).slice(0, 15);
    }, OVERLAY_SEL);
    throw new Error(`pickFieldDropdown(${labelText}): "${optionText}" not selectable; filter=${filtered}; offered=${JSON.stringify(visible)}`);
  }
  await sleep(900);
}

async function bodyText(page) {
  return page.evaluate(() => document.body?.innerText || '');
}

// Advances the wizard. The guard is the whole point: this module walks screens
// that have a real Place Order button on them, and an exploratory Continue must
// never be able to find and press it.
const SPEND = /place order|pay now|submit order|complete purchase|confirm and pay/i;

async function advance(page, { label = 'Continue|Next|Save and Continue', settle = 3500 } = {}) {
  const hit = await mark(page, { text: `^\\s*(${label})\\s*$`, tags: 'button,[role="button"],a' });
  if (!hit) throw new Error(`advance: no "${label}" button on ${page.url()}`);
  if (SPEND.test(hit.text)) throw new Error(`advance: refusing to click a spend button ("${hit.text}")`);
  if (hit.disabled) throw new Error(`advance: "${hit.text}" is disabled -- the step above did not take`);
  const before = page.url();
  const done = await clickReal(page, { text: `^\\s*(${label})\\s*$`, tags: 'button,[role="button"],a' });
  await sleep(settle);
  return { clicked: hit.text, via: done.via, url_before: before, url_after: page.url() };
}

// ── navigation ──────────────────────────────────────────────────────────────
// A hash-only goto does not reload the SPA and its router falls back to
// #/dashpanel, so every hop here is a full document load, with one self-heal
// through Keycloak from the worker's own env.

// Re-authenticates against the cabinet's Keycloak. Deliberately layered,
// because this page has broken automation twice in two different ways: the
// "Sign in" button is DISABLED until the theme's model sees the fields change
// (so a plain click times out on "element is not enabled"), and the fields are
// not always click-targetable when the page is still settling (so page.click
// times out waiting for "visible, enabled and stable").
//
// Nothing here ever returns or logs a credential -- only lengths and flags.
async function cabinetLogin(page, user, pass) {
  if (!user || !pass) return { ok: false, reason: 'WYREG_USERNAME/WYREG_PASSWORD are not set on the worker' };

  // Wait for the form to exist, without requiring it to be clickable.
  let present = false;
  for (let i = 0; i < 20 && !present; i++) {
    present = await page.evaluate(() => !!document.querySelector('#username') && !!document.querySelector('#password'));
    if (!present) await sleep(1000);
  }
  if (!present) return { ok: false, reason: 'the login form never rendered' };

  // Set both fields through the native setter and fire the whole event family.
  // This is what the theme's reactive model listens to; it does not depend on
  // the element being visible or hit-testable.
  const filled = await page.evaluate(({ u, p }) => {
    const set = (id, v) => {
      const el = document.getElementById(id);
      if (!el) return false;
      el.focus();
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(el, v);
      for (const type of ['input', 'change', 'keyup', 'blur']) el.dispatchEvent(new Event(type, { bubbles: true }));
      return el.value.length === v.length;
    };
    const okU = set('username', u);
    const okP = set('password', p);
    const btn = document.querySelector('#kc-login');
    return { okU, okP, userLen: (document.getElementById('username')?.value || '').length, passLen: (document.getElementById('password')?.value || '').length, disabled: btn ? btn.disabled : null };
  }, { u: user, p: pass });
  if (!filled.okU || !filled.okP) return { ok: false, reason: `the fields did not take (username ${filled.userLen} chars, password ${filled.passLen} chars)` };

  await page.check('#rememberMe').catch(() => {});
  await sleep(500);

  // Submit the FORM rather than the button. requestSubmit() runs the theme's own
  // submit handler and validation, and it works whether or not the button is
  // still disabled -- Keycloak validates the credentials server-side regardless.
  const submitted = await page.evaluate(() => {
    const btn = document.querySelector('#kc-login');
    if (btn && !btn.disabled) { btn.click(); return 'button'; }
    const form = document.getElementById('kc-form') || document.querySelector('form[action*="authenticate"]');
    if (!form) return 'no-form';
    if (typeof form.requestSubmit === 'function') { form.requestSubmit(); return 'requestSubmit'; }
    form.submit();
    return 'submit';
  });
  if (submitted === 'no-form') return { ok: false, reason: 'no login form to submit' };

  await page.waitForURL(/accounts\.wyregisteredagent\.net/, { timeout: 60000 }).catch(() => {});
  await sleep(4000);
  const landed = page.url();
  const stillOnLogin = /openid-connect|login-actions|id\.wyregisteredagent\.net/.test(landed);
  if (stillOnLogin) {
    const err = await page.evaluate(() => Array.from(document.querySelectorAll('.alert,.kc-feedback-text,[role="alert"]'))
      .map((e) => (e.innerText || '').replace(/\s+/g, ' ').trim()).filter(Boolean).slice(0, 3));
    return { ok: false, reason: `still on the login page after ${submitted}${err.length ? `: ${err.join(' | ')}` : ''}`, via: submitted };
  }
  return { ok: true, via: submitted, url: landed };
}

async function gotoRoute(page, route, settle = 9000, depth = 0) {
  if (depth === 0) await ensureViewport(page);
  await page.goto(`${BASE}/#/${route}`, { waitUntil: 'domcontentloaded', timeout: 60000 });
  await sleep(2000);
  const head = route.split(/[/?]/)[0];
  if (!page.url().includes(head)) {
    await page.evaluate((u) => { window.location.href = u; }, `${BASE}/#/${route}`);
    await sleep(1500);
    await page.reload({ waitUntil: 'domcontentloaded', timeout: 60000 }).catch(() => {});
  }
  await sleep(settle);
  if (/openid-connect|login-actions|id\.wyregisteredagent\.net/.test(page.url())) {
    if (depth > 1) throw new Error('gotoRoute: stuck on the Keycloak login page after re-authenticating');
    const user = process.env.WYREG_USERNAME;
    const pass = process.env.WYREG_PASSWORD;
    if (!user || !pass) throw new Error('wyreg login required but WYREG_USERNAME/WYREG_PASSWORD are not set on the worker');
    const login = await cabinetLogin(page, user, pass);
    if (!login.ok) throw new Error(`wyreg login failed: ${login.reason}`);
    await sleep(6000);
    return gotoRoute(page, route, settle, depth + 1);
  }
  return page.url();
}

// The AdsPower window can come up small enough that the checkout's sticky header
// covers the buttons underneath it. A roomy viewport removes the whole class of
// failure -- at 1600x1000 the payment button sits mid-page instead of at y~50.
async function ensureViewport(page) {
  try {
    const vp = typeof page.viewportSize === 'function' ? page.viewportSize() : null;
    if (!vp || vp.width < 1400 || vp.height < 900) {
      await page.setViewportSize({ width: 1600, height: 1000 });
      await sleep(700);
    }
  } catch { /* a CDP-attached window may refuse to resize; the hit-test covers us */ }
}

function uuidFrom(url) {
  const m = url.match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i);
  return m ? m[0] : null;
}

// Turns "Use Our Address" on (or off) wherever it appears on the current screen.
// On the state filing this is what keeps every real address off the public record.
async function setUseOurAddress(page, on = true) {
  const toggled = [];
  for (let i = 0; i < 6; i++) {
    const state = await page.evaluate((idx) => {
      const labels = Array.from(document.querySelectorAll('label,span,div'))
        .filter((el) => /use our address/i.test((el.innerText || '').trim()) && (el.innerText || '').length < 60);
      const el = labels.filter((e) => !labels.some((o) => o !== e && e.contains(o)))[idx];
      if (!el) return null;
      const row = el.closest('label,.p-field,.field,.form-group,div');
      const box = row?.querySelector('input[type="checkbox"],[role="switch"],.p-checkbox,.p-inputswitch');
      if (!box) return { found: true, checked: null };
      const checked = box.getAttribute('aria-checked') === 'true'
        || box.checked === true
        || /p-highlight|p-inputswitch-checked|p-checkbox-checked/.test(box.className || '');
      box.setAttribute('data-wf', `uoa${idx}`);
      return { found: true, checked };
    }, i);
    if (!state) break;
    if (state.checked === on) { toggled.push({ index: i, changed: false, already: on }); continue; }
    const box = await boxOf(page, `uoa${i}`);
    if (!box) { toggled.push({ index: i, changed: false, error: 'no box' }); continue; }
    await page.mouse.click(box.x + box.w / 2, box.y + box.h / 2);
    await sleep(600);
    toggled.push({ index: i, changed: true });
  }
  return toggled;
}

// ── stages ──────────────────────────────────────────────────────────────────

// Steps 1-5 of Hire Us. "Add to Account" creates the business record and charges
// nothing. Note the name is NOT editable afterwards anywhere in the cabinet, so a
// wrong name here means building a fresh cart, not renaming.
async function stageCreate(page, p) {
  const name = String(p.company_name || '').trim();
  if (!name) throw new Error('create: company_name is required');
  const suffix = p.suffix ?? 'LLC';
  const state = p.state ?? 'Wyoming';
  const steps = [];

  await gotoRoute(page, 'hire-us/v2', 10000);
  steps.push({ step: 'hire-us', url: page.url() });

  await clickReal(page, { text: 'Add a new company', tags: 'div,button,[role="button"],li,span,h3,h4' });
  await sleep(1200);
  steps.push(await advance(page));

  try {
    await pickFieldDropdown(page, 'State|Jurisdiction', state);
  } catch {
    await pickDropdown(page, { text: /state|select/i, tags: '.p-dropdown,.p-select,[role="combobox"]' }, state);
  }
  steps.push({ step: 'state', value: state, ...(await advance(page)) });

  // Entity type is a grid of radio CARDS, not a dropdown.
  await clickReal(page, {
    text: p.entity_type_text ?? 'Limited Liability Company',
    tags: 'div,label,li,span,button,[role="radio"],h3,h4',
  });
  await sleep(900);
  steps.push({ step: 'entity', ...(await advance(page)) });

  await setValue(page, { selector: 'input[type="text"]:not([disabled])' }, name);
  const suffixPicked = await (async () => {
    try {
      await pickDropdown(page, { text: /LLC|L\.L\.C|suffix|designator/i, tags: '.p-dropdown,.p-select,[role="combobox"]' }, suffix);
      return suffix;
    } catch { return null; }
  })();
  steps.push({ step: 'name', value: name, suffix: suffixPicked, ...(await advance(page)) });

  // Confirmation screen -> Add to Account. This is the record-creating click, and
  // the name is not editable afterwards, so dry_run stops here: it proves the whole
  // wizard walked correctly without leaving an orphan record in the cabinet.
  if (p.dry_run) {
    const confirmText = await bodyText(page);
    return {
      stage: 'create',
      dry_run: true,
      biz_id: null,
      url: page.url(),
      name_on_screen: new RegExp(name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i').test(confirmText),
      add_to_account_present: /Add to Account/i.test(confirmText),
      confirmation_screen: confirmText.replace(/\n{2,}/g, '\n').slice(0, 800),
      steps,
      note: 'stopped before "Add to Account" -- nothing was created. Re-run without dry_run to file.',
    };
  }

  const added = await clickIf(page, { text: 'Add to Account', tags: 'button,[role="button"],a' });
  if (added) await sleep(4000);
  steps.push({ step: 'add_to_account', clicked: added, url: page.url() });
  await clickIf(page, { text: '^\\s*(Continue|Next)\\s*$', tags: 'button,[role="button"],a' });
  await sleep(4000);

  const text = await bodyText(page);
  const bizId = uuidFrom(page.url());
  return {
    stage: 'create',
    biz_id: bizId,
    url: page.url(),
    name_on_screen: new RegExp(name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i').test(text),
    steps,
    next: bizId
      ? `stage:"cart" with biz_id:"${bizId}"`
      : 'no biz_id in the URL -- open the cabinet and read it off #/businesses/<id> before continuing',
  };
}


// Steps 6-7. "Business Formation Service" BUNDLES the registered-agent line
// ($103 + $49 = $152). Adding the standalone "Registered Agent Service" product
// on top double-bills the client, so this stage adds exactly one product and
// reports every line it can see on the cart.
async function stageCart(page, p) {
  const biz = p.biz_id;
  if (!biz) throw new Error('cart: biz_id is required (from the create stage)');
  const steps = [];

  await gotoRoute(page, `hire-us/v2/select-products/${biz}`, 10000);
  steps.push({ step: 'select-products', url: page.url() });

  await clickReal(page, {
    text: p.product_text ?? 'Business Formation Service',
    tags: 'div,label,li,span,h3,h4,button',
  });
  await sleep(1000);

  const addedToCart = await clickIf(page, { text: 'Add to Cart', tags: 'button,[role="button"],a' });
  if (!addedToCart) throw new Error('cart: no "Add to Cart" button after selecting the product');
  await sleep(3500);
  steps.push({ step: 'add_to_cart', url: page.url() });

  await clickIf(page, { text: 'Review Cart|View Cart|Go to Cart', tags: 'button,[role="button"],a' });
  await sleep(3000);
  await clickIf(page, { text: '^\\s*(Checkout|Proceed to Checkout)\\s*$', tags: 'button,[role="button"],a' });
  await sleep(5000);
  steps.push({ step: 'checkout', url: page.url() });

  const text = await bodyText(page);
  const lines = text
    .split('\n')
    .map((l) => l.trim())
    .filter((l) => /\$\d/.test(l))
    .slice(0, 25);
  const totals = [...text.matchAll(/\$\s?([\d,]+\.\d{2})/g)].map((m) => m[1]);

  return {
    stage: 'cart',
    biz_id: biz,
    url: page.url(),
    on_checkout: /checkout/i.test(page.url()),
    lines,
    amounts_seen: totals,
    // The caller echoes this back as confirm_total on the pay stage, so a cart
    // that silently changed between the two calls cannot be charged.
    grand_total: totals.length ? totals[totals.length - 1] : null,
    has_formation_line: /Business Formation Service/i.test(text),
    has_registered_agent_line: /Registered Agent Service/i.test(text),
    steps,
    next: `stage:"pay" with biz_id:"${biz}", confirm_total:"${totals.length ? totals[totals.length - 1] : '?'}"`,
  };
}

// Selects a saved card and proves it took.
//
// Each saved card renders as a PrimeVue radio -- input[type=radio][name="cards"]
// inside a label, with the brand and last four in a sibling .card-details. The
// radio's checked state is read back after the click, because a PrimeVue radio
// ignores synthetic events and a click that misses leaves the previous card armed.
async function selectCard(page, last4) {
  const list = () => page.evaluate(() => Array.from(document.querySelectorAll('input[type="radio"][name="cards"]')).map((inp, i) => {
    const row = inp.closest('.number-detail') || inp.closest('label')?.parentElement || null;
    return { i, checked: !!inp.checked, text: (row?.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 70) };
  }));

  let cards = await list();
  // On a narrow window the list is collapsed behind a button; at a wide one it is
  // already expanded and no such button exists.
  if (!cards.length) {
    const opened = await clickIf(page, { text: 'Change Payment Method|Change Card', tags: 'button,[role="button"],a' });
    if (opened) await sleep(6000);
    cards = await list();
  }
  if (!cards.length) return { ok: false, reason: 'no saved-card radios on screen', cards: [] };

  const rx = new RegExp(String(last4));
  const idx = cards.findIndex((c) => rx.test(c.text));
  if (idx < 0) return { ok: false, reason: `no saved card ending ${last4}`, cards };
  if (cards[idx].checked) return { ok: true, already_selected: true, cards };

  await page.evaluate((i) => {
    const inp = document.querySelectorAll('input[type="radio"][name="cards"]')[i];
    const target = inp.closest('label') || inp.parentElement;
    target.setAttribute('data-wf', 'cardpick');
    target.scrollIntoView({ block: 'center' });
  }, idx);
  await sleep(500);
  const box = await boxOf(page, 'cardpick');
  if (!box) return { ok: false, reason: 'card row vanished before the click', cards };
  await page.mouse.move(box.x + box.w / 2, box.y + box.h / 2);
  await page.mouse.click(box.x + box.w / 2, box.y + box.h / 2);
  await sleep(3500);

  cards = await list();
  return { ok: !!cards[idx]?.checked, already_selected: false, cards };
}

// Step 8 -- THE ONE STAGE THAT SPENDS MONEY.
//
// The payment-method selection is component state and reverts to the account
// default on any navigation or reload, so the card change and Place Order must
// happen in a single page session with no hop in between. Every assertion below
// is re-checked against the screen immediately before the click, and the click
// only happens when the caller passed confirm_total matching the cart and
// confirm_charge: true. Without both, the stage stops and reports what it saw.
async function stagePay(page, p) {
  const biz = p.biz_id;
  if (!biz) throw new Error('pay: biz_id is required');
  const last4 = String(p.card_last4 ?? BCA_DEFAULTS.card_last4);
  const expectName = p.company_name ? String(p.company_name) : null;

  await gotoRoute(page, `hire-us/v2/checkout/${biz}?hide=true`, 11000);
  if (!/checkout/i.test(page.url())) throw new Error(`pay: not on the checkout route -- landed on ${page.url()}`);

  // Pick the card by its saved-card RADIO, not by page text. Every saved card is
  // always listed on this screen, so finding "6754" in the body only proves the
  // card exists -- never that it is the one that will be charged. The radio is
  // the only honest signal.
  const sel = await selectCard(page, last4);

  const text = await bodyText(page);
  const amounts = [...text.matchAll(/\$\s?([\d,]+\.\d{2})/g)].map((m) => m[1]);
  const grand = amounts.length ? amounts[amounts.length - 1] : null;
  const cardsOnScreen = [...text.matchAll(/(VISA|MASTERCARD|AMEX|DISCOVER)[^\n]{0,24}?(\d{4})/gi)]
    .map((m) => `${m[1].toUpperCase()} ${m[2]}`);

  const selectedCards = (sel.cards || []).filter((c) => c.checked);
  const checks = {
    on_checkout: /checkout/i.test(page.url()),
    card_selected: sel.ok === true,
    // Exactly one card is armed, and it is the one we asked for.
    exactly_one_card_selected: selectedCards.length === 1,
    selected_card_is_target: selectedCards.length === 1 && new RegExp(String(last4)).test(selectedCards[0].text),
    formation_line: /Business Formation Service/i.test(text),
    total_matches_confirm: p.confirm_total ? grand === String(p.confirm_total) : false,
    name_matches: expectName
      ? new RegExp(expectName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i').test(text)
      : null,
  };
  const failed = Object.entries(checks)
    .filter(([k, v]) => v === false && !(k === 'name_matches' && expectName === null))
    .map(([k]) => k);

  const authorized = p.confirm_charge === true && p.confirm_total;
  if (!authorized || failed.length) {
    return {
      stage: 'pay',
      placed: false,
      reason: !authorized
        ? 'not authorized: pass confirm_charge:true together with confirm_total from the cart stage'
        : `pre-flight assertions failed: ${failed.join(', ')}`,
      biz_id: biz,
      url: page.url(),
      grand_total_on_screen: grand,
      cards_on_screen: cardsOnScreen,
      checks,
      card_selection: sel,
    };
  }

  const hit = await mark(page, { text: '^\\s*Place Order\\s*$', tags: 'button,[role="button"],a' });
  if (!hit) throw new Error('pay: assertions passed but there is no "Place Order" button on screen');
  if (hit.disabled) throw new Error('pay: "Place Order" is disabled');
  const box = await boxOf(page, hit.marker);
  await page.mouse.click(box.x + box.w / 2, box.y + box.h / 2);
  await sleep(14000);

  const after = await bodyText(page);
  const invoice = after.match(/Invoice\s*#?\s*([A-Z0-9]{6,12})/i)?.[1] ?? null;
  const orderId = uuidFrom(page.url());

  return {
    stage: 'pay',
    placed: true,
    biz_id: biz,
    order_id: orderId,
    invoice,
    charged_total: grand,
    card: selectedCards.length === 1 ? selectedCards[0].text : `****${last4}`,
    url: page.url(),
    checks,
    next: orderId ? `stage:"details" with order_id:"${orderId}"` : 'read the order id off the post-payment URL',
  };
}

// Step 9 -- the post-payment wizard. Free: the filing is already paid for, this
// is the information the state filing is built from. Business Details, then
// People & Roles, then Review & Submit.
async function stageDetails(page, p) {
  const orderId = p.order_id;
  if (!orderId) throw new Error('details: order_id is required (from the pay stage)');
  const management = (p.management ?? BCA_DEFAULTS.management).toLowerCase();
  const members = Array.isArray(p.members) ? p.members : [];
  const useOurs = p.use_our_address ?? BCA_DEFAULTS.use_our_address;
  const steps = [];

  await gotoRoute(page, `verify-order?id=${orderId}`, 12000);
  steps.push({ step: 'verify-order', url: page.url() });

  // --- Business Details. The industry field is an autocomplete, so the value
  // has to be typed and then chosen from the suggestion list.
  if (p.industry_query) {
    await setValue(page, { selector: 'input[type="text"],input:not([type])' }, String(p.industry_query));
    await sleep(2200);
    const chose = await clickIf(page, { text: p.industry_pick ?? p.industry_query, tags: 'li,[role="option"],.p-autocomplete-item,div' });
    steps.push({ step: 'industry', query: p.industry_query, picked: chose });
    await sleep(800);
  }
  steps.push({ step: 'business_details_address', use_our_address: await setUseOurAddress(page, useOurs) });
  steps.push({ step: 'business_details', ...(await advance(page, { settle: 5000 })) });

  // --- People & Roles. Member managed unless the client asked otherwise.
  const mgmtLabel = management.startsWith('manager') ? 'Manager Managed' : 'Member Managed';
  const mgmtClicked = await clickIf(page, { text: mgmtLabel, tags: 'div,label,li,span,button,[role="radio"]' });
  await sleep(1200);
  steps.push({ step: 'management', value: mgmtLabel, clicked: mgmtClicked });

  for (let i = 0; i < members.length; i++) {
    const m = members[i];
    // Member rows commit server-side the moment they are added and survive a
    // reload, so only add a row when the screen does not already have one.
    if (i > 0) {
      await clickIf(page, { text: 'Add (a )?(new )?Member|Add Member', tags: 'button,[role="button"],a' });
      await sleep(1800);
    }
    const nameFields = [
      ['input[name*="first" i],input[placeholder*="First" i]', m.first],
      ['input[name*="middle" i],input[placeholder*="Middle" i]', m.middle],
      ['input[name*="last" i],input[placeholder*="Last" i]', m.last],
    ];
    for (const [sel, val] of nameFields) {
      if (!val) continue;
      try { await setValue(page, { selector: sel, nth: i }, String(val)); }
      catch (e) { steps.push({ step: `member${i}_field`, selector: sel, error: e.message.slice(0, 160) }); }
    }
    steps.push({ step: `member${i}`, name: [m.first, m.middle, m.last].filter(Boolean).join(' ') });
  }
  steps.push({ step: 'members_address', use_our_address: await setUseOurAddress(page, useOurs) });
  steps.push({ step: 'people_and_roles', ...(await advance(page, { settle: 5000 })) });

  // --- Review & Submit. Standard speed: decline the paid rush upgrade.
  const speed = (p.speed ?? BCA_DEFAULTS.speed).toLowerCase();
  if (speed === 'standard') {
    await clickIf(page, { text: '^\\s*Standard\\s*$', tags: 'div,label,li,span,button,[role="radio"]' });
    await sleep(900);
  } else {
    await clickIf(page, { text: '^\\s*Rush\\s*$', tags: 'div,label,li,span,button,[role="radio"]' });
    await sleep(900);
  }

  const text = await bodyText(page);
  const checks = {
    name: p.company_name
      ? new RegExp(String(p.company_name).replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i').test(text)
      : null,
    management: new RegExp(mgmtLabel, 'i').test(text),
    members_present: members.every((m) => !m.last || new RegExp(String(m.last), 'i').test(text)),
    registered_agent: /Registered Agent/i.test(text),
    // Standard speed means no rush charge should have crept onto the review.
    no_rush_charge: speed !== 'standard' || !/rush[^\n]{0,40}\$\s?\d/i.test(text),
  };
  const failed = Object.entries(checks).filter(([, v]) => v === false).map(([k]) => k);
  if (failed.length) {
    return { stage: 'details', submitted: false, reason: `review assertions failed: ${failed.join(', ')}`, url: page.url(), checks, steps };
  }
  if (p.submit === false) {
    return { stage: 'details', submitted: false, reason: 'submit:false -- filled and verified, left unsubmitted', url: page.url(), checks, steps };
  }

  const submitted = await clickIf(page, { text: 'Submit Your Order|^\\s*Submit\\s*$', tags: 'button,[role="button"],a' });
  await sleep(12000);
  const after = await bodyText(page);

  return {
    stage: 'details',
    submitted,
    url: page.url(),
    checks,
    steps,
    status_on_screen: (after.match(/(Order Received|Processing|Submitted to State|Formation Complete|approved)/gi) || []).slice(0, 6),
  };
}

// Removes duplicate rows from the Members (officials) table.
//
// Member rows commit server-side the moment they are added and survive a reload,
// so a re-run of the intake can leave identical rows behind -- and a duplicate
// member would be filed with the agent. The Remove button is DISABLED until a row
// checkbox is ticked, which is why it looks unusable at first glance.
async function dedupeMembers(page) {
  const rowsOf = () => page.evaluate(() => {
    const table = document.querySelector('table');
    const rows = table ? Array.from(table.querySelectorAll('tbody tr')) : [];
    return rows.map((tr, i) => ({
      i,
      key: (tr.innerText || '').replace(/\s+/g, ' ').replace(/\s*Edit\s*$/, '').trim(),
    }));
  });

  const before = await rowsOf();
  const seen = new Set();
  const duplicates = before.filter((r) => (seen.has(r.key) ? true : (seen.add(r.key), false)));
  if (!duplicates.length) return { removed: 0, rows_before: before.length, rows_after: before.length, rows: before.map((r) => r.key) };

  let removed = 0;
  for (let guard = 0; guard < 4; guard++) {
    const rows = await rowsOf();
    const seen2 = new Set();
    const dupIdx = rows.findIndex((r) => (seen2.has(r.key) ? true : (seen2.add(r.key), false)));
    if (dupIdx < 0) break;

    const ticked = await page.evaluate((idx) => {
      const tr = document.querySelectorAll('table tbody tr')[idx];
      if (!tr) return false;
      const box = tr.querySelector('.p-checkbox') || tr.querySelector('input[type="checkbox"]')?.parentElement;
      if (!box) return false;
      box.setAttribute('data-wf', 'dupcb');
      box.scrollIntoView({ block: 'center' });
      return true;
    }, dupIdx);
    if (!ticked) break;
    await sleep(500);
    const box = await boxOf(page, 'dupcb');
    if (!box) break;
    await page.mouse.click(box.x + box.w / 2, box.y + box.h / 2);
    await sleep(1500);

    // Remove only ever runs with exactly one row selected.
    const selected = await page.evaluate(() => Array.from(document.querySelectorAll('table tbody tr input[type="checkbox"]')).filter((c) => c.checked).length);
    if (selected !== 1) return { removed, error: `expected 1 selected row, found ${selected} -- not clicking Remove`, rows_before: before.length };

    const clicked = await clickIf(page, { text: '^\\s*Remove\\s*$', tags: 'button,[role="button"],a', prefer: 'shortest' });
    if (!clicked) return { removed, error: 'Remove button not clickable', rows_before: before.length };
    await sleep(2500);
    // A confirmation step may or may not appear.
    await clickIf(page, { text: '^\\s*(Remove|Yes|Confirm|Delete|OK)\\s*$', tags: 'button,[role="button"],a', prefer: 'shortest' });
    await sleep(3500);
    removed++;
  }

  const after = await rowsOf();
  return { removed, rows_before: before.length, rows_after: after.length, rows: after.map((r) => r.key) };
}

// Step 10 -- the Registered Agent service intake. This form is the OPPOSITE of
// the state filing on addresses: 30 N Gould is rejected here ("Registered Agent
// address not allowed") and the rejection WIPES the form, so the member and
// contact addresses must be real. Always Save before Submit.
//
// The trap worth knowing: Person is required, and selecting a saved person fills
// the name fields -- editing those names detaches the person again, after which
// Save refuses with no visible validation error. The only route that works is
// Person -> "+ Add a new Contact", which is what this stage does.
async function stageRa(page, p) {
  const id = p.request_id;
  if (!id) throw new Error('ra: request_id is required (the service-requiring-attention id)');
  const c = p.contact ?? {};
  const email = c.email ?? BCA_DEFAULTS.contact_email;
  const phone = c.phone ?? BCA_DEFAULTS.contact_phone;
  const steps = [];

  await gotoRoute(page, `service-requiring-attention/${id}`, 12000);
  steps.push({ step: 'intake', url: page.url() });

  const before = await bodyText(page);
  if (/30 N Gould/i.test(before)) {
    steps.push({ step: 'warn', note: 'the RA address is already on this form -- it will be rejected and wipe the form' });
  }

  // Duplicate member rows are removed before anything else, so the rest of the
  // form is filled against the row set that will actually be filed.
  if (p.dedupe_members) {
    const dedupe = await dedupeMembers(page);
    steps.push({ step: 'dedupe_members', ...dedupe });
    if (dedupe.error) {
      return { stage: 'ra', submitted: false, reason: `member dedupe stopped: ${dedupe.error}`, url: page.url(), steps };
    }
  }

  // Management Type is required on this form independently of the state filing.
  const mgmt = (p.management ?? BCA_DEFAULTS.management).toLowerCase().startsWith('manager')
    ? 'Manager Managed' : 'Member Managed';
  let mgmtSet = await clickIf(page, { text: `^\\s*${mgmt}\\s*$`, tags: 'div,label,li,span,button,[role="radio"],[role="option"]', prefer: 'shortest' });
  if (!mgmtSet) {
    try { await pickFieldDropdown(page, 'Management Type', mgmt); mgmtSet = true; }
    catch { /* reported below */ }
  }
  steps.push({ step: 'management_type', value: mgmt, set: mgmtSet });
  await sleep(800);

  // Person is a PrimeVue select, and "+ Add a new Contact" is its FIRST option --
  // the option does not exist in the DOM until the overlay is open, so the select
  // has to be opened before anything can be clicked inside it.
  const personOpened = await clickIf(page, {
    text: 'Select a Person',
    tags: '.p-select,.p-dropdown,[role="combobox"]',
    prefer: 'shortest',
  });
  if (personOpened) await sleep(2500);
  steps.push({ step: 'person_select_opened', clicked: personOpened });

  // An existing contact can simply be chosen. Do NOT then edit the name fields:
  // editing them detaches the person again and Save refuses with no visible error.
  // Email and phone are safe to overwrite, which is how the client stays the named
  // contact while correspondence routes to BCA.
  if (c.existing_person) {
    const picked = await clickIf(page, {
      text: `^\\s*${String(c.existing_person).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s*$`,
      tags: 'li,[role="option"],.p-select-option,span',
      prefer: 'shortest',
      noScroll: true,
    });
    await sleep(3000);
    steps.push({ step: 'person_picked', name: c.existing_person, clicked: picked });
    for (const [sel, val] of [
      ['input[type="email"],input[name*="email" i],input[placeholder*="mail" i]', email],
      ['input[type="tel"],input[name*="phone" i],input[placeholder*="hone" i]', phone],
    ]) {
      try { await setValue(page, { selector: sel }, String(val)); }
      catch (e) { steps.push({ step: 'contact_route_field', selector: sel, error: e.message.slice(0, 160) }); }
    }
  }

  const addedContact = c.existing_person ? false : await clickIf(page, {
    text: '\\+ Add a new Contact|Add a new Contact',
    tags: 'li,[role="option"],.p-select-option,span,button,a',
    prefer: 'shortest',
    noScroll: true,
  });
  if (addedContact) {
    await sleep(3500);
    // The sub-form opens pre-filled with the account holder's own name, email,
    // phone and mailing address. Only the NAME is wrong: the client has to be the
    // named contact, while email, phone and address stay BCA's so neither our
    // address nor the client's is exposed and correspondence reaches us.
    for (const [label, val] of [
      ['First Name', c.first],
      ['Last Name', c.last],
      ['Email Address', email],
      ['Phone Number', phone],
    ]) {
      if (!val) continue;
      try { await setFieldByLabel(page, label, val); steps.push({ step: `contact_${label.replace(/ /g, '_')}`, value: String(val) }); }
      catch (e) { steps.push({ step: `contact_${label.replace(/ /g, '_')}`, error: e.message.slice(0, 160) }); }
    }

    // Address: left at BCA's pre-filled values unless the caller passes one.
    // Country first -- setting it rewrites the Region and Zip inputs.
    const addr = c.address ?? {};
    if (addr.country) {
      try { await pickFieldDropdown(page, 'Country', addr.country); steps.push({ step: 'contact_country', value: addr.country }); }
      catch (e) { steps.push({ step: 'contact_country', error: e.message.slice(0, 160) }); }
    }
    for (const [label, val] of [['Line 1', addr.line1], ['Line 2', addr.line2], ['City', addr.city], ['Zip/Postal Code', addr.zip]]) {
      if (!val) continue;
      try { await setFieldByLabel(page, label, val); }
      catch (e) { steps.push({ step: 'contact_addr', label, error: e.message.slice(0, 160) }); }
    }
    // Region is required and ships BLANK even when the rest of the address is
    // pre-filled, which is the usual reason Save refuses with nothing visible.
    if (addr.region) {
      try { await pickFieldDropdown(page, 'State/Province/Region|Region|State', addr.region); steps.push({ step: 'contact_region', value: addr.region }); }
      catch (e) { steps.push({ step: 'contact_region', error: e.message.slice(0, 160) }); }
    }

    const savedContact = await clickIf(page, { text: '^\\s*(Save Contact|Save|Add Contact)\\s*$', tags: 'button,[role="button"],a', prefer: 'shortest' });
    await sleep(5000);
    const postContact = await bodyText(page);
    steps.push({
      step: 'contact_saved',
      clicked: savedContact,
      person_still_unset: /Select a Person/i.test(postContact),
    });
  }
  steps.push({ step: 'add_contact', clicked: addedContact });

  // Save, then verify from the DOM rather than from page text. Two tempting
  // signals are both wrong here: "Select a Person" is simply the placeholder
  // while a NEW contact is being created, and the asterisks next to required
  // labels are always present and say nothing about validation.
  const saved = await clickIf(page, { text: '^\\s*Save\\s*$', tags: 'button,[role="button"],a', prefer: 'shortest' });
  await sleep(6000);

  const verdict = await page.evaluate(() => {
    const invalid = Array.from(document.querySelectorAll('.p-invalid,[aria-invalid="true"]'))
      .filter((e) => e.offsetParent !== null);
    const messages = Array.from(document.querySelectorAll('.p-message,.p-toast-message,[role="alert"],.p-error,small.p-error'))
      .filter((e) => e.offsetParent !== null)
      .map((e) => (e.innerText || '').replace(/\s+/g, ' ').trim())
      .filter(Boolean)
      .slice(0, 8);
    return {
      invalid_count: invalid.length,
      invalid_labels: invalid.map((e) => {
        const id = e.id || e.querySelector('input,textarea')?.id;
        const l = id ? document.querySelector(`label[for="${id}"]`) : null;
        return (l?.innerText || e.getAttribute('aria-label') || '').replace(/\s+/g, ' ').trim().slice(0, 40);
      }).filter(Boolean).slice(0, 10),
      messages,
      url: location.href,
      still_requires_attention: /service-requiring-attention/i.test(location.href),
    };
  });

  const saveTook = saved && verdict.invalid_count === 0
    && !verdict.messages.some((m) => /required|invalid|error|not allowed/i.test(m));
  steps.push({ step: 'save', clicked: saved, invalid_count: verdict.invalid_count, messages: verdict.messages });

  if (!saveTook || p.submit === false) {
    return {
      stage: 'ra',
      submitted: false,
      reason: !saveTook
        ? `Save did not take -- ${verdict.invalid_count} invalid field(s)${verdict.messages.length ? `: ${verdict.messages.join(' | ')}` : ''}`
        : 'submit:false -- saved and verified, left unsubmitted',
      url: page.url(),
      validation: verdict,
      steps,
    };
  }

  const submitted = await clickIf(page, { text: '^\\s*Submit\\s*$', tags: 'button,[role="button"],a' });
  await sleep(8000);
  return { stage: 'ra', submitted, url: page.url(), steps };
}


// The Communications Contact select, located by its LABEL. Matching on the
// control's text cannot work: a PrimeVue select renders its VALUE, so once a
// person is bound the control reads "Alessandro Specchio" and the placeholder
// "Select a Person" is gone.
//
// The control sits near the bottom of a long form -- well below the fold -- so
// it has to be scrolled into view, re-measured in a SEPARATE evaluate (a rect
// read in the same one still carries the pre-scroll coordinates) and hit-tested
// before the press. Clicking the stale coordinate is why the overlay silently
// never opened.
async function openPersonSelect(page) {
  const locate = (doScroll) => page.evaluate((scroll) => {
    const labels = Array.from(document.querySelectorAll('label'))
      .filter((l) => /communications contact|person/i.test(l.innerText || ''));
    for (const l of labels) {
      let node = l;
      for (let up = 0; up < 5 && node; up++) {
        const sel = node.querySelector?.('.p-select,.p-dropdown,[role="combobox"]');
        if (sel && sel.offsetParent !== null) {
          sel.setAttribute('data-wf-person', '1');
          if (scroll) sel.scrollIntoView({ block: 'center', inline: 'nearest' });
          const r = sel.getBoundingClientRect();
          const x = r.x + r.width / 2;
          const y = r.y + r.height / 2;
          const top = document.elementFromPoint(x, y);
          return {
            x, y,
            inView: y > 60 && y < innerHeight - 20 && x > 0 && x < innerWidth,
            obstructed: top ? !(top === sel || sel.contains(top) || top.contains(sel)) : true,
            topmost: top ? `${top.tagName}.${((top.className || '') + '').slice(0, 36)}` : null,
            viewport: { w: innerWidth, h: innerHeight },
            label: (l.innerText || '').replace(/\s+/g, ' ').replace(/\*/g, '').trim().slice(0, 40),
            current: (sel.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 40),
          };
        }
        node = node.parentElement;
      }
    }
    return null;
  }, doScroll);

  let pt = await locate(true);
  if (!pt) return null;
  for (let attempt = 1; attempt <= 4; attempt++) {
    await sleep(700);
    pt = await locate(false);
    if (!pt) return null;
    if (pt.inView && !pt.obstructed) {
      await page.mouse.move(pt.x, pt.y);
      await page.mouse.click(pt.x, pt.y);
      await sleep(2800);
      const opened = await page.evaluate((ov) => !!Array.from(document.querySelectorAll(ov)).find((e) => e.offsetParent !== null), OVERLAY_SEL);
      if (opened) return { ...pt, attempts: attempt };
    }
    // Re-centre and try again: a sticky header can cover the point, and a
    // late-rendering section can push the control back under the fold.
    await page.evaluate(() => {
      const el = document.querySelector('[data-wf-person="1"]');
      if (el) el.scrollIntoView({ block: 'center', inline: 'nearest' });
      else window.scrollBy(0, 200);
    });
  }
  return { ...pt, opened: false, attempts: 4 };
}

// The contact inputs on this form carry NO `label[for]` and no placeholder, so
// they can only be addressed by what they currently hold. The sub-form opens
// pre-filled with the account holder's own details, which makes the current
// value a precise, verifiable handle: match it exactly, write the new one with
// the native setter, read it back.
async function setFieldByValue(page, current, next) {
  const res = await page.evaluate(({ cur, nxt }) => {
    const els = Array.from(document.querySelectorAll('input'))
      .filter((e) => e.offsetParent !== null && e.type === 'text' && e.value === cur);
    if (els.length !== 1) return { ok: false, matched: els.length };
    const el = els[0];
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(el, nxt);
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.dispatchEvent(new Event('blur', { bubbles: true }));
    return { ok: true, matched: 1, now: el.value };
  }, { cur: current, nxt: next });
  if (!res.ok) throw new Error(`setFieldByValue: ${JSON.stringify(current)} matched ${res.matched} inputs, need exactly 1`);
  await sleep(250);
  const got = await page.evaluate((nxt) => Array.from(document.querySelectorAll('input')).some((e) => e.value === nxt), next);
  if (!got) throw new Error(`setFieldByValue: ${JSON.stringify(next)} did not stick`);
  return res;
}

// Every row inside the open overlay, with its own rect. Rows are never scrolled
// -- scrolling inside a PrimeVue overlay closes it.
async function overlayRows(page) {
  return page.evaluate((ov) => {
    const o = Array.from(document.querySelectorAll(ov)).find((e) => e.offsetParent !== null);
    if (!o) return { found: false, rows: [] };
    const r = o.getBoundingClientRect();
    const rows = Array.from(o.querySelectorAll('li,[role="option"],a,button,.p-select-option'))
      .filter((e) => {
        const b = e.getBoundingClientRect();
        return b.width > 2 && b.height > 2;
      })
      .map((e, i) => {
        const b = e.getBoundingClientRect();
        return {
          i,
          tag: e.tagName,
          text: (e.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 60),
          x: Math.round(b.x + b.width / 2),
          y: Math.round(b.y + b.height / 2),
          h: Math.round(b.height),
          selected: e.getAttribute('aria-selected') === 'true',
        };
      });
    const filter = o.querySelector('input');
    return {
      found: true,
      rect: { x: Math.round(r.x), y: Math.round(r.y), w: Math.round(r.width), h: Math.round(r.height) },
      hasFilterInput: !!filter,
      rowCount: rows.length,
      rows,
    };
  }, OVERLAY_SEL);
}

// Labelled text inputs and selects currently on the form. The contact sub-form
// only renders its name/address inputs in some states, so what is present has to
// be read rather than assumed.
async function formSnapshot(page) {
  return page.evaluate(() => {
    const vis = (e) => e.offsetParent !== null;
    const labelOf = (e) => {
      if (e.id) {
        const l = document.querySelector(`label[for="${CSS.escape(e.id)}"]`);
        if (l) return (l.innerText || '').replace(/\s+/g, ' ').replace(/\*/g, '').trim().slice(0, 40);
      }
      return '';
    };
    return {
      inputs: Array.from(document.querySelectorAll('input,textarea')).filter(vis)
        .map((e) => ({ type: e.type || 'textarea', label: labelOf(e), value: (e.value || '').slice(0, 44) })),
      selects: Array.from(document.querySelectorAll('.p-select,.p-dropdown')).filter(vis)
        .map((e) => ({ value: (e.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 40) })),
      buttons: Array.from(document.querySelectorAll('button')).filter(vis)
        .map((e) => ({ text: (e.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 30), disabled: e.disabled === true }))
        .filter((b) => b.text),
    };
  });
}

// A structural map of every visible field, used to identify the "add a new
// contact" panel's inputs. They carry no label[for] and no placeholder, so each
// one is described by its id, its aria attributes, and the nearest text in its
// own ancestry -- enough to tell First Name from Last Name without guessing at
// an index.
async function fieldMap(page) {
  return page.evaluate(() => {
    const vis = (e) => e.offsetParent !== null;
    const clean = (s) => (s || '').replace(/\s+/g, ' ').replace(/\*/g, '').trim();
    const nearestText = (el) => {
      let node = el;
      for (let up = 0; up < 5 && node; up++) {
        // Any label, legend or small caption inside this ancestor that is not
        // itself a field wrapper for a different input.
        const cands = Array.from(node.querySelectorAll?.('label,legend,small,.p-float-label>span,span.field-label') || [])
          .map((l) => clean(l.innerText))
          .filter((t) => t && t.length <= 36);
        if (cands.length) return cands.slice(0, 3);
        node = node.parentElement;
      }
      return [];
    };
    const panelOf = (el) => {
      let node = el;
      for (let up = 0; up < 10 && node; up++) {
        if (node.classList && (node.classList.contains('p-dialog') || node.classList.contains('p-card')
          || node.classList.contains('new-contact') || node.tagName === 'FORM')) {
          return `${node.tagName}.${(node.className + '').slice(0, 40)}`;
        }
        node = node.parentElement;
      }
      return null;
    };
    return Array.from(document.querySelectorAll('input,textarea,select')).filter(vis).map((e, i) => {
      const r = e.getBoundingClientRect();
      return {
        i,
        tag: e.tagName,
        type: e.type || null,
        id: e.id || null,
        name: e.getAttribute('name'),
        aria: e.getAttribute('aria-label') || e.getAttribute('aria-labelledby'),
        autocomplete: e.getAttribute('autocomplete'),
        maxlength: e.getAttribute('maxlength'),
        value: (e.value || '').slice(0, 44),
        y: Math.round(r.y + scrollY),
        labelFor: e.id ? clean(document.querySelector(`label[for="${CSS.escape(e.id)}"]`)?.innerText) : '',
        near: nearestText(e),
        panel: panelOf(e),
      };
    });
  });
}

// Reads the Communications Contact block -- the form's OWN fields, the ones
// whose ids are PrimeVue-generated (`pv_id_*`). The separate "+ Add a new
// Contact" panel uses semantic ids (`address1`, `city`, `postalCode`) and
// lower-case labels ("First name"), so the two never get confused.
async function contactBlock(page) {
  return page.evaluate(() => {
    const clean = (s) => (s || '').replace(/\s+/g, ' ').replace(/\*/g, '').trim();
    const nearLabel = (el) => {
      let node = el;
      for (let up = 0; up < 5 && node; up++) {
        const l = Array.from(node.querySelectorAll?.('label,legend,small') || [])
          .map((e) => clean(e.innerText)).filter((t) => t && t.length <= 36);
        if (l.length) return l[0];
        node = node.parentElement;
      }
      return '';
    };
    const out = {};
    for (const el of Array.from(document.querySelectorAll('input[type="text"]'))) {
      if (el.offsetParent === null) continue;
      if (!/^pv_id_/.test(el.id || '')) continue;
      const key = nearLabel(el);
      if (!key) continue;
      if (!(key in out)) out[key] = { id: el.id, value: el.value };
    }
    const member = Array.from(document.querySelectorAll('tr,.official-row,[id^="official-row"]'))
      .map((e) => clean(e.innerText)).filter((t) => t.length > 20).slice(0, 3);
    return { fields: out, memberRows: member };
  });
}

// Writes one field of the Communications Contact block, addressed by its label
// and restricted to the `pv_id_*` ids so the new-contact panel can never be hit
// by accident. Exactly one match is required.
async function setContactField(page, label, value) {
  const res = await page.evaluate(({ lbl, val }) => {
    const clean = (s) => (s || '').replace(/\s+/g, ' ').replace(/\*/g, '').trim();
    const nearLabel = (el) => {
      let node = el;
      for (let up = 0; up < 5 && node; up++) {
        const l = Array.from(node.querySelectorAll?.('label,legend,small') || [])
          .map((e) => clean(e.innerText)).filter((t) => t && t.length <= 36);
        if (l.length) return l[0];
        node = node.parentElement;
      }
      return '';
    };
    const els = Array.from(document.querySelectorAll('input[type="text"]'))
      .filter((e) => e.offsetParent !== null && /^pv_id_/.test(e.id || '') && nearLabel(e) === lbl);
    if (els.length !== 1) return { ok: false, matched: els.length };
    const el = els[0];
    const before = el.value;
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(el, val);
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.dispatchEvent(new Event('blur', { bubbles: true }));
    return { ok: true, id: el.id, before, now: el.value };
  }, { lbl: label, val: value });
  if (!res.ok) throw new Error(`setContactField: label ${JSON.stringify(label)} matched ${res.matched} pv_id_ inputs, need exactly 1`);
  await sleep(250);
  if (res.now !== value) throw new Error(`setContactField: ${label} holds ${JSON.stringify(res.now)}, wanted ${JSON.stringify(value)}`);
  return res;
}

// Drives a PrimeVue select in the Communications Contact block. Restricted to
// `pv_id_*` controls so the "+ Add a new Contact" panel is never touched.
// The control sits below the fold, so it is scrolled, re-measured in a separate
// evaluate and hit-tested before the press -- a stale coordinate is why these
// overlays silently fail to open. Long lists (countries) are virtualized, so the
// option is reached through the overlay's filter input, never by scrolling the
// list (scrolling inside a PrimeVue overlay closes it).
async function setContactSelect(page, label, option) {
  const locate = (scroll) => page.evaluate(({ lbl, doScroll }) => {
    const clean = (s) => (s || '').replace(/\s+/g, ' ').replace(/\*/g, '').trim();
    const nearLabel = (el) => {
      let node = el;
      for (let up = 0; up < 5 && node; up++) {
        const l = Array.from(node.querySelectorAll?.('label,legend,small') || [])
          .map((e) => clean(e.innerText)).filter((t) => t && t.length <= 36);
        if (l.length) return l[0];
        node = node.parentElement;
      }
      return '';
    };
    const els = Array.from(document.querySelectorAll('.p-select,.p-dropdown'))
      .filter((e) => e.offsetParent !== null && /^pv_id_/.test(e.id || '') && nearLabel(e) === lbl);
    if (els.length !== 1) return { ok: false, matched: els.length };
    const el = els[0];
    if (doScroll) el.scrollIntoView({ block: 'center', inline: 'nearest' });
    const r = el.getBoundingClientRect();
    const x = r.x + r.width / 2;
    const y = r.y + r.height / 2;
    const top = document.elementFromPoint(x, y);
    return {
      ok: true, x, y, id: el.id, current: clean(el.innerText).slice(0, 36),
      inView: y > 60 && y < innerHeight - 20,
      obstructed: top ? !(top === el || el.contains(top) || top.contains(el)) : true,
      topmost: top ? `${top.tagName}.${((top.className || '') + '').slice(0, 36)}` : null,
    };
  }, { lbl: label, doScroll: scroll });

  let pt = await locate(true);
  if (!pt.ok) throw new Error(`setContactSelect: label ${JSON.stringify(label)} matched ${pt.matched} pv_id_ selects, need exactly 1`);
  let opened = false;
  for (let attempt = 1; attempt <= 5 && !opened; attempt++) {
    // Re-find by LABEL and re-scroll on every attempt. Writing any sibling
    // field re-renders this section, which replaces the node and invalidates
    // its generated id, so scrolling a remembered element does nothing at all
    // and the point stays off-screen (elementFromPoint then returns null).
    await locate(true);
    await sleep(800);
    pt = await locate(false);
    if (!pt.ok) throw new Error(`setContactSelect: ${label} vanished`);
    if (pt.inView && !pt.obstructed) {
      await page.mouse.move(pt.x, pt.y);
      await page.mouse.click(pt.x, pt.y);
      await sleep(2200);
      opened = await page.evaluate((ov) => !!Array.from(document.querySelectorAll(ov)).find((e) => e.offsetParent !== null), OVERLAY_SEL);
    }
  }
  if (!opened) throw new Error(`setContactSelect: the ${label} overlay would not open (inView=${pt.inView} obstructed=${pt.obstructed} topmost=${pt.topmost})`);

  // Reaching the option. Three strategies, cheapest first, because this select
  // has no filter input and its list is long enough that the target renders
  // outside the panel's visible box -- a press there lands on whatever sits
  // behind the overlay.
  const attempts = [];

  // 1. The filter input, when the select has one: narrows to a single row.
  const filter = await page.evaluate((ov) => {
    const o = Array.from(document.querySelectorAll(ov)).find((e) => e.offsetParent !== null);
    const inp = o?.querySelector('input');
    if (!inp) return null;
    const r = inp.getBoundingClientRect();
    return { x: r.x + r.width / 2, y: r.y + r.height / 2 };
  }, OVERLAY_SEL);
  if (filter) {
    await page.mouse.click(filter.x, filter.y);
    await sleep(300);
    await page.keyboard.type(String(option), { delay: 45 });
    await sleep(1800);
    attempts.push('filter');
  } else {
    // 2. PrimeVue type-ahead: typing with the overlay open moves the highlight
    //    to the matching option, and Enter commits it. No coordinates involved.
    await page.keyboard.type(String(option), { delay: 70 });
    await sleep(1200);
    const highlighted = await page.evaluate((ov) => {
      const o = Array.from(document.querySelectorAll(ov)).find((e) => e.offsetParent !== null);
      const h = o?.querySelector('[data-p-highlight="true"],.p-select-option-selected,.p-focus,[aria-selected="true"]');
      return h ? (h.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 40) : null;
    }, OVERLAY_SEL);
    attempts.push(`typeahead:${JSON.stringify(highlighted)}`);
    if (highlighted && highlighted.toLowerCase() === String(option).toLowerCase()) {
      await page.keyboard.press('Enter');
      await sleep(2500);
      const afterKb = await locate(false);
      if (afterKb.ok && afterKb.current.toLowerCase() === String(option).toLowerCase()) return { ...afterKb, via: attempts.join('|') };
    }
  }

  // 3. Scroll the overlay's OWN list container (never the page, and never via
  //    scrollIntoView -- both close the overlay) until the row sits inside the
  //    panel, then hit-test and click it.
  for (let pass = 1; pass <= 3; pass++) {
    const picked = await page.evaluate(({ ov, want, doScroll }) => {
      const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
      const o = Array.from(document.querySelectorAll(ov)).find((e) => e.offsetParent !== null);
      if (!o) return { ok: false, reason: 'overlay closed' };
      const rows = Array.from(o.querySelectorAll('li,[role="option"],.p-select-option'))
        .filter((e) => clean(e.innerText).toLowerCase() === String(want).toLowerCase());
      if (rows.length !== 1) {
        return {
          ok: false,
          reason: `${rows.length} rows match ${JSON.stringify(want)}`,
          sample: Array.from(o.querySelectorAll('li,[role="option"]')).slice(0, 10).map((e) => clean(e.innerText).slice(0, 30)),
        };
      }
      const row = rows[0];
      if (doScroll) {
        // The scroller is whichever ancestor inside the overlay actually overflows.
        let sc = row.parentElement;
        while (sc && sc !== o.parentElement && !(sc.scrollHeight > sc.clientHeight + 4)) sc = sc.parentElement;
        if (sc && sc.scrollHeight > sc.clientHeight + 4) {
          sc.scrollTop = row.offsetTop - (sc.clientHeight / 2) + (row.offsetHeight / 2);
          return { ok: false, reason: 'scrolled', scrolled: true, scrollTop: Math.round(sc.scrollTop) };
        }
        return { ok: false, reason: 'no scrollable container inside the overlay' };
      }
      const box = o.getBoundingClientRect();
      const r = row.getBoundingClientRect();
      const inPanel = r.y >= box.y - 1 && r.y + r.height <= box.y + box.height + 1;
      const x = r.x + r.width / 2;
      const y = r.y + r.height / 2;
      const top = document.elementFromPoint(x, y);
      const onTarget = top ? (top === row || row.contains(top) || top.contains(row)) : false;
      return {
        ok: inPanel && onTarget, x, y, inPanel, onTarget,
        reason: inPanel ? (onTarget ? null : `point covered by ${clean(top?.innerText).slice(0, 30)}`) : 'option outside the panel box',
      };
    }, { ov: OVERLAY_SEL, want: option, doScroll: pass > 1 });

    if (picked.ok) {
      await page.mouse.move(picked.x, picked.y);
      await page.mouse.click(picked.x, picked.y);
      await sleep(2500);
      attempts.push(`click:pass${pass}`);
      break;
    }
    attempts.push(`pass${pass}:${picked.reason}`);
    if (picked.reason === 'overlay closed' || (picked.sample && pass === 3)) {
      await page.keyboard.press('Escape').catch(() => {});
      throw new Error(`setContactSelect: ${label} -> ${option}: ${picked.reason}${picked.sample ? ` (saw: ${picked.sample.join(', ')})` : ''} [${attempts.join(' ')}]`);
    }
    await sleep(700);
  }

  const got = await locate(false);
  if (!got.ok || got.current.toLowerCase() !== String(option).toLowerCase()) {
    throw new Error(`setContactSelect: ${label} reads ${JSON.stringify(got.current)} after picking ${JSON.stringify(option)} [${attempts.join(' ')}]`);
  }
  return { ...got, via: attempts.join('|') };
}

// Region is a dropdown of states while Country is the United States, and often a
// plain text box for other countries. Try both rather than assume.
async function setContactRegion(page, value) {
  try { return { via: 'select', ...(await setContactSelect(page, 'State/Province/Region', value)) }; }
  catch (selErr) {
    try { return { via: 'text', ...(await setContactField(page, 'State/Province/Region', value)) }; }
    catch (txtErr) { throw new Error(`region: select failed (${selErr.message.slice(0, 120)}); text failed (${txtErr.message.slice(0, 120)})`); }
  }
}

// Rewrites the Communications Contact in place. The Person select is a
// convenience that COPIES an address-book entry into these fields; the fields
// themselves are what gets filed, so changing the name here leaves Alessandro's
// own address-book record untouched and creates no duplicate contact.
// Email, phone and address are deliberately left as BCA's.
async function stageContactDirect(page, p) {
  const id = p.request_id;
  if (!id) throw new Error('contact: request_id is required');
  const c = p.contact ?? {};
  const steps = [];

  await gotoRoute(page, `service-requiring-attention/${id}`, 12000);
  const before = await contactBlock(page);
  steps.push({ step: 'before', fields: before.fields, memberRows: before.memberRows });

  if (p.inspect) {
    // Every actionable button with its position and the heading it sits under.
    // This form carries TWO Save/Submit pairs -- one per section -- so a bare
    // text match on "Submit" clicks whichever comes first in the DOM and the
    // intake silently stays unsubmitted.
    const buttons = await page.evaluate(() => {
      const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
      return Array.from(document.querySelectorAll('button,[role="button"],input[type="submit"]'))
        .filter((e) => e.offsetParent !== null)
        .map((e, i) => {
          const r = e.getBoundingClientRect();
          // Walk back through previous siblings and ancestors for the nearest heading.
          let section = '';
          let node = e;
          outer: for (let up = 0; up < 8 && node; up++) {
            let sib = node.previousElementSibling;
            while (sib) {
              const h = sib.matches?.('h1,h2,h3,h4,legend') ? sib : sib.querySelector?.('h1,h2,h3,h4,legend');
              if (h && clean(h.innerText)) { section = clean(h.innerText).slice(0, 48); break outer; }
              sib = sib.previousElementSibling;
            }
            node = node.parentElement;
          }
          return {
            i,
            text: clean(e.innerText || e.value).slice(0, 36),
            disabled: e.disabled === true,
            y: Math.round(r.y + scrollY),
            x: Math.round(r.x),
            section,
            cls: ((e.className || '') + '').slice(0, 50),
          };
        });
    });
    // The Region and Country controls are PrimeVue selects, not inputs, so they
    // need their own map: nearest label, current value, and whether the option
    // list carries a filter input (a long country list is virtualized, so an
    // option that is not in view is not in the DOM and can never be clicked).
    const selects = await page.evaluate(() => {
      const clean = (s) => (s || '').replace(/\s+/g, ' ').replace(/\*/g, '').trim();
      const nearLabel = (el) => {
        let node = el;
        for (let up = 0; up < 5 && node; up++) {
          const l = Array.from(node.querySelectorAll?.('label,legend,small') || [])
            .map((e) => clean(e.innerText)).filter((t) => t && t.length <= 36);
          if (l.length) return l[0];
          node = node.parentElement;
        }
        return '';
      };
      return Array.from(document.querySelectorAll('.p-select,.p-dropdown')).filter((e) => e.offsetParent !== null)
        .map((e, i) => {
          const r = e.getBoundingClientRect();
          return { i, label: nearLabel(e), value: clean(e.innerText).slice(0, 36), y: Math.round(r.y + scrollY), id: e.id || null };
        });
    });
    return { stage: 'contact', mode: 'direct', inspect: true, before, selects, buttons, url: page.url() };
  }

  // Clicking Submit changed nothing on the first attempt, so this branch clicks
  // one named button by index and then OBSERVES -- dialogs, overlays, toasts,
  // whether the form is still there -- without reloading, which would discard
  // whatever appeared.
  if (p.submit_probe) {
    const target = await page.evaluate(({ idx, want }) => {
      const btns = Array.from(document.querySelectorAll('button,[role="button"],input[type="submit"]'))
        .filter((e) => e.offsetParent !== null);
      const el = btns[idx];
      if (!el) return { ok: false, reason: `no button at index ${idx}`, count: btns.length };
      const text = (el.innerText || el.value || '').replace(/\s+/g, ' ').trim();
      if (want && text !== want) return { ok: false, reason: `index ${idx} is ${JSON.stringify(text)}, expected ${JSON.stringify(want)}` };
      el.scrollIntoView({ block: 'center' });
      return { ok: true, text };
    }, { idx: p.click_index, want: p.click_text ?? null });
    if (!target.ok) return { stage: 'contact', mode: 'direct', probe: true, clicked: false, ...target, url: page.url() };
    await sleep(900);
    const pt = await page.evaluate((idx) => {
      const el = Array.from(document.querySelectorAll('button,[role="button"],input[type="submit"]')).filter((e) => e.offsetParent !== null)[idx];
      const r = el.getBoundingClientRect();
      const x = r.x + r.width / 2;
      const y = r.y + r.height / 2;
      const top = document.elementFromPoint(x, y);
      return { x, y, obstructed: top ? !(top === el || el.contains(top) || top.contains(el)) : true, topmost: top ? `${top.tagName}.${((top.className || '') + '').slice(0, 36)}` : null };
    }, p.click_index);
    if (pt.obstructed) return { stage: 'contact', mode: 'direct', probe: true, clicked: false, reason: `the point is covered by ${pt.topmost}`, pt, url: page.url() };
    await page.mouse.move(pt.x, pt.y);
    await page.mouse.click(pt.x, pt.y);
    await sleep(p.observe_ms ?? 6000);
    const observed = await page.evaluate(() => {
      const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
      const dialogs = Array.from(document.querySelectorAll('.p-dialog,[role="dialog"],.p-confirmdialog,.p-overlaypanel,.p-drawer'))
        .filter((e) => e.offsetParent !== null)
        .map((e) => ({
          cls: ((e.className || '') + '').slice(0, 60),
          text: clean(e.innerText).slice(0, 500),
          buttons: Array.from(e.querySelectorAll('button')).filter((b) => b.offsetParent !== null)
            .map((b) => ({ text: clean(b.innerText).slice(0, 30), disabled: b.disabled === true })),
        }));
      return {
        url: location.href,
        dialogs,
        toasts: Array.from(document.querySelectorAll('.p-message,.p-toast-message,[role="alert"],small.p-error'))
          .filter((e) => e.offsetParent !== null).map((e) => clean(e.innerText)).filter(Boolean).slice(0, 8),
        invalid: Array.from(document.querySelectorAll('.p-invalid,[aria-invalid="true"]')).filter((e) => e.offsetParent !== null).length,
        contactFormStillPresent: Array.from(document.querySelectorAll('input[type="text"]')).some((e) => e.offsetParent !== null && /^pv_id_/.test(e.id || '')),
        headings: Array.from(document.querySelectorAll('h1,h2,h3')).filter((e) => e.offsetParent !== null).map((e) => clean(e.innerText)).filter(Boolean).slice(0, 8),
      };
    });
    return { stage: 'contact', mode: 'direct', probe: true, clicked: true, button: target.text, at: pt, observed };
  }

  const writes = {};
  if (c.first) writes['First Name'] = String(c.first);
  if (c.last) writes['Last Name'] = String(c.last);
  if (c.email) writes['Email Address'] = String(c.email);
  if (c.phone) writes['Phone Number'] = String(c.phone);
  if (!Object.keys(writes).length && !c.address) throw new Error('contact: nothing to write -- pass contact.first / contact.last / contact.address');

  const addr0 = c.address ?? {};
  if (addr0.country) {
    try { const r = await setContactSelect(page, 'Country', addr0.country); steps.push({ step: 'set', label: 'Country', to: addr0.country, id: r.id, via: r.via }); }
    catch (e) { steps.push({ step: 'set', label: 'Country', to: addr0.country, error: e.message.slice(0, 260) }); }
    await sleep(2000);
  }

  for (const [label, val] of Object.entries(writes)) {
    try { const r = await setContactField(page, label, val); steps.push({ step: 'set', label, from: r.before, to: val, id: r.id }); }
    catch (e) { steps.push({ step: 'set', label, to: val, error: e.message.slice(0, 200) }); }
  }
  const failed = steps.filter((s) => s.step === 'set' && s.error);
  if (failed.length) {
    return { stage: 'contact', mode: 'direct', done: false, reason: `${failed.length} field write(s) failed -- not saving a half-edited contact`, fields: (await contactBlock(page)).fields, steps, url: page.url() };
  }

  // Address. This form REJECTS a virtual address outright -- Submit answers
  // "Unable to use a virtual address in Address." -- so BCA's own Sanford
  // mailbox cannot be used here even though it is the right answer everywhere
  // else. A real physical address is required, and it is only ever seen by the
  // registered agent, never by the public state filing.
  //
  // Country goes FIRST: setting it rewrites the Region and Zip controls and
  // shifts their ids, so any text written before it lands in the wrong boxes.
  const addr = addr0;
  if (p.probe_after_country) {
    return { stage: 'contact', mode: 'direct', probe_after_country: true, fields: (await contactBlock(page)).fields, map: await fieldMap(page), steps, url: page.url() };
  }
  for (const [label, val] of [['Line 1', addr.line1], ['Line 2', addr.line2], ['City', addr.city], ['Zip/Postal Code', addr.zip]]) {
    if (val === undefined) continue;
    try { const r = await setContactField(page, label, String(val)); steps.push({ step: 'set', label, from: r.before, to: String(val) }); }
    catch (e) { steps.push({ step: 'set', label, to: String(val), error: e.message.slice(0, 240) }); }
  }
  if (addr.region) {
    try { const r = await setContactRegion(page, addr.region); steps.push({ step: 'set', label: 'Region', to: addr.region, via: r.via }); }
    catch (e) { steps.push({ step: 'set', label: 'Region', to: addr.region, error: e.message.slice(0, 240) }); }
  }
  const addrFailed = steps.filter((s) => s.step === 'set' && s.error);
  if (addrFailed.length) {
    return {
      stage: 'contact', mode: 'direct', done: false,
      reason: `${addrFailed.length} address write(s) failed -- not saving a half-written address`,
      fields: (await contactBlock(page)).fields, steps, url: page.url(),
    };
  }
  if (p.stop_before_save) {
    return { stage: 'contact', mode: 'direct', done: false, saved: false, reason: 'stop_before_save -- filled and left unsaved for review', fields: (await contactBlock(page)).fields, steps, url: page.url() };
  }

  const saved = await clickIf(page, { text: '^\\s*Save\\s*$', tags: 'button,[role="button"]', prefer: 'shortest' });
  await sleep(6500);
  const feedback = await page.evaluate(() => ({
    toast: Array.from(document.querySelectorAll('.p-message,.p-toast-message,[role="alert"],small.p-error'))
      .filter((e) => e.offsetParent !== null).map((e) => (e.innerText || '').replace(/\s+/g, ' ').trim()).filter(Boolean).slice(0, 6),
    invalid: Array.from(document.querySelectorAll('.p-invalid,[aria-invalid="true"]')).filter((e) => e.offsetParent !== null).length,
  }));
  steps.push({ step: 'save', clicked: saved, ...feedback });

  // Proof is a reload and a read-back. A "Save Successful" toast is not proof.
  await gotoRoute(page, `service-requiring-attention/${id}`, 12000);
  const after = await contactBlock(page);
  steps.push({ step: 'after_reload', fields: after.fields });

  const got = (k) => after.fields[k]?.value ?? null;
  const want = (k, v) => v === undefined ? got(k) === (before.fields[k]?.value ?? null) : got(k) === String(v);
  const checks = {
    first_is_client: !c.first || got('First Name') === String(c.first),
    last_is_client: !c.last || got('Last Name') === String(c.last),
    email_still_bca: want('Email Address', c.email),
    phone_still_bca: want('Phone Number', c.phone),
    line1: want('Line 1', addr.line1),
    line2: want('Line 2', addr.line2),
    city: want('City', addr.city),
    zip: want('Zip/Postal Code', addr.zip),
  };
  const allOk = Object.values(checks).every(Boolean);
  if (!allOk || p.submit !== true) {
    return {
      stage: 'contact', mode: 'direct',
      done: allOk, submitted: false,
      reason: allOk ? 'contact rewritten and verified after reload; pass submit:true to submit the intake'
        : 'the read-back does not match what was written -- NOT submitting',
      checks, after: after.fields, steps, url: page.url(),
    };
  }

  const submitted = await clickIf(page, { text: '^\\s*Submit\\s*$', tags: 'button,[role="button"]' });
  await sleep(9000);
  const post = await page.evaluate(() => ({
    url: location.href,
    toast: Array.from(document.querySelectorAll('.p-message,.p-toast-message,[role="alert"]'))
      .filter((e) => e.offsetParent !== null).map((e) => (e.innerText || '').replace(/\s+/g, ' ').trim()).filter(Boolean).slice(0, 6),
  }));
  return { stage: 'contact', mode: 'direct', done: true, submitted, checks, after: after.fields, post, steps };
}

// Fixes the Communications Contact on an RA intake. `inspect:true` returns the
// evidence (form snapshot + every overlay row with its rect) and changes nothing.
async function stageContact(page, p) {
  if (p.mode === 'direct') return stageContactDirect(page, p);
  const id = p.request_id;
  if (!id) throw new Error('contact: request_id is required');
  const steps = [];

  await gotoRoute(page, `service-requiring-attention/${id}`, 12000);
  const opened = await openPersonSelect(page);
  if (!opened) {
    return { stage: 'contact', done: false, reason: 'no Person/Contact select found on this form', url: page.url(), form: await formSnapshot(page) };
  }
  const overlay = await overlayRows(page);

  if (p.inspect) {
    // Close the overlay again so the page is left as it was found.
    await page.keyboard.press('Escape').catch(() => {});
    return { stage: 'contact', inspect: true, control: opened, overlay, form: await formSnapshot(page), url: page.url() };
  }

  // Pick the row by exact text, clicking its own measured centre. Nothing is
  // scrolled and no row index is assumed -- a near-miss here binds the wrong
  // person onto a client's legal record.
  const want = p.pick;
  if (!want) throw new Error('contact: pass pick:"<exact option text>" (run with inspect:true first), or inspect:true');
  const matches = overlay.rows.filter((r) => r.text.toLowerCase() === String(want).toLowerCase());
  if (matches.length !== 1) {
    await page.keyboard.press('Escape').catch(() => {});
    return {
      stage: 'contact', done: false,
      reason: `pick "${want}" matched ${matches.length} rows, need exactly 1`,
      overlay, url: page.url(),
    };
  }
  const row = matches[0];
  // Confirm the point really belongs to that row before pressing.
  const probe = await page.evaluate(({ x, y, text }) => {
    const top = document.elementFromPoint(x, y);
    if (!top) return { ok: false, topmost: null };
    const rowEl = top.closest('li,[role="option"],a,button,.p-select-option') || top;
    return { ok: (rowEl.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 60).toLowerCase() === String(text).toLowerCase(), topmost: (rowEl.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 60) };
  }, { x: row.x, y: row.y, text: row.text });
  steps.push({ step: 'hit_test', want: row.text, at: { x: row.x, y: row.y }, ...probe });
  if (!probe.ok) {
    await page.keyboard.press('Escape').catch(() => {});
    return { stage: 'contact', done: false, reason: `the point for "${row.text}" is covered by "${probe.topmost}" -- not clicking`, overlay, steps, url: page.url() };
  }
  await page.mouse.move(row.x, row.y);
  await page.mouse.click(row.x, row.y);
  await sleep(4000);
  steps.push({ step: 'picked', text: row.text });

  const afterPick = await formSnapshot(page);
  steps.push({ step: 'after_pick', inputs: afterPick.inputs.length, selects: afterPick.selects.map((s) => s.value) });
  if (p.inspect_subform) {
    return { stage: 'contact', inspect_subform: true, picked: row.text, fields: await fieldMap(page), url: page.url(), steps };
  }

  // A brand-new contact opens a sub-form. Only the NAME is wrong in it: email,
  // phone and address stay BCA's so correspondence reaches us and no address is
  // exposed. Field names are matched by label, never by index.
  const c = p.contact ?? {};
  if (/add a new contact/i.test(row.text)) {
    // The sub-form opens pre-filled with the ACCOUNT HOLDER's own name, email,
    // phone and mailing address, and none of its inputs carry a label or a
    // placeholder. `rename` therefore addresses each field by the value it
    // currently holds: {"Alessandro":"Bill"} rewrites exactly the input that
    // reads "Alessandro" and fails loudly if that is not exactly one input.
    //
    // Only the NAME is changed. Email, phone and address stay BCA's, so
    // correspondence reaches us and neither our address nor the client's is
    // exposed on the record.
    for (const [from, to] of Object.entries(c.rename ?? {})) {
      try { await setFieldByValue(page, from, String(to)); steps.push({ step: 'rename', from, to }); }
      catch (e) { steps.push({ step: 'rename', from, to, error: e.message.slice(0, 200) }); }
    }
    steps.push({ step: 'subform_filled', snapshot: await formSnapshot(page) });
    if (p.stop_before_save) {
      return {
        stage: 'contact', done: false, saved: false,
        reason: 'stop_before_save -- sub-form filled and left unsaved for review',
        steps, url: page.url(),
      };
    }
    // Any rename that failed leaves the account holder's own name on a client's
    // record. Stop before Save rather than persist a half-renamed contact.
    const failed = steps.filter((s) => s.step === 'rename' && s.error);
    if (failed.length) {
      return {
        stage: 'contact', done: false,
        reason: `${failed.length} rename(s) failed -- not saving a half-renamed contact`,
        steps, url: page.url(),
      };
    }
  }

  const saved = await clickIf(page, { text: '^\\s*(Save Contact|Add Contact|Save)\\s*$', tags: 'button,[role="button"],a', prefer: 'shortest' });
  await sleep(6000);
  const toast = await page.evaluate(() => Array.from(document.querySelectorAll('.p-message,.p-toast-message,[role="alert"],small.p-error'))
    .filter((e) => e.offsetParent !== null)
    .map((e) => (e.innerText || '').replace(/\s+/g, ' ').trim()).filter(Boolean).slice(0, 6));
  const invalid = await page.evaluate(() => Array.from(document.querySelectorAll('.p-invalid,[aria-invalid="true"]')).filter((e) => e.offsetParent !== null).length);
  steps.push({ step: 'save', clicked: saved, toast, invalid_count: invalid });

  // Proof: reload and read the bound person back off the page. A toast is not proof.
  await gotoRoute(page, `service-requiring-attention/${id}`, 12000);
  const final = await formSnapshot(page);
  const boundTo = (await page.evaluate(() => {
    const labels = Array.from(document.querySelectorAll('label')).filter((l) => /person|communication|contact/i.test(l.innerText || ''));
    for (const l of labels) {
      let node = l;
      for (let up = 0; up < 4 && node; up++) {
        const sel = node.querySelector?.('.p-select,.p-dropdown');
        if (sel && sel.offsetParent !== null) return (sel.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 60);
        node = node.parentElement;
      }
    }
    return null;
  }));
  steps.push({ step: 'verified_after_reload', bound_to: boundTo });

  const want_name = [c.first, c.middle, c.last].filter(Boolean).join(' ');
  const matched = !!boundTo && !!want_name && boundTo.toLowerCase().includes(String(c.last || '').toLowerCase());

  if (!matched || p.submit !== true) {
    return {
      stage: 'contact',
      done: matched,
      submitted: false,
      bound_to: boundTo,
      expected: want_name || p.pick,
      reason: matched ? 'contact set and verified; pass submit:true to submit the intake' : 'the bound contact is still not the client -- NOT submitting',
      form: final,
      steps,
      url: page.url(),
    };
  }

  const submitted = await clickIf(page, { text: '^\\s*Submit\\s*$', tags: 'button,[role="button"],a' });
  await sleep(9000);
  return { stage: 'contact', done: true, submitted, bound_to: boundTo, url: page.url(), steps };
}

// Loads any cabinet route and reports what is on it. Used to answer questions
// the acting stages cannot answer about themselves -- notably whether a service
// still appears in the "requires attention" list after an intake was submitted.
async function stageStatus(page, p) {
  const route = p.route || 'services';
  await gotoRoute(page, route, p.settle ?? 12000);

  // Optional: click one control by its exact visible text, then report the page
  // again. Anything that could spend money is refused outright -- "Place Order"
  // on a stale cart is one click away from a real charge.
  const clicks = [];
  for (const want of (p.click ? [].concat(p.click) : [])) {
    if (/place order|pay now|purchase|complete order|buy/i.test(want)) {
      clicks.push({ want, clicked: false, refused: 'this control can spend money -- stageStatus will not click it' });
      continue;
    }
    try {
      const hit = await clickReal(page, { text: `^\\s*${String(want).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s*$`, tags: 'button,[role="button"],a,li,span', prefer: 'shortest' });
      clicks.push({ want, clicked: true, via: hit.via, text: hit.text });
      await sleep(p.click_settle ?? 4000);
    } catch (e) {
      clicks.push({ want, clicked: false, error: e.message.slice(0, 200) });
    }
  }

  const out = await page.evaluate(() => {
    const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
    return {
      url: location.href,
      headings: Array.from(document.querySelectorAll('h1,h2,h3,h4')).filter((e) => e.offsetParent !== null)
        .map((e) => clean(e.innerText)).filter(Boolean).slice(0, 15),
      rows: Array.from(document.querySelectorAll('tr,.p-datatable-row,.service-row,li'))
        .filter((e) => e.offsetParent !== null)
        .map((e) => clean(e.innerText)).filter((t) => t.length > 12 && t.length < 260).slice(0, 40),
      attentionMentions: Array.from(document.querySelectorAll('a,button,span,div'))
        .filter((e) => e.offsetParent !== null && /requir\w* attention|action required|needs attention/i.test(e.innerText || ''))
        .map((e) => clean(e.innerText).slice(0, 120)).slice(0, 10),
      bodyHas: {
        bmfr: /BMFR/i.test(document.body.innerText || ''),
        requiresAttention: /requir\w* attention/i.test(document.body.innerText || ''),
      },
      // Actionable controls, including the icon-only ones (a delete is usually a
      // bare trash icon with nothing but an aria-label or a title on it).
      controls: Array.from(document.querySelectorAll('button,[role="button"],a,[class*="trash" i],[class*="delete" i]'))
        .filter((e) => e.offsetParent !== null)
        .map((e) => ({
          tag: e.tagName,
          text: clean(e.innerText).slice(0, 36),
          aria: e.getAttribute('aria-label'),
          title: e.getAttribute('title'),
          href: (e.getAttribute('href') || '').slice(0, 60),
          icon: Array.from(e.querySelectorAll('i,svg')).map((n) => ((n.className || '') + '' || n.getAttribute('data-icon') || '').slice(0, 40)).filter(Boolean).slice(0, 2),
          cls: ((e.className || '') + '').slice(0, 50),
        }))
        .filter((c) => c.text || c.aria || c.title || c.icon.length),
    };
  });
  // Raw markup for a named region, for when a control has no text, no aria
  // label and no icon class to match on.
  let html = null;
  if (p.html_of) {
    html = await page.evaluate(({ sel, needle }) => {
      let els = Array.from(document.querySelectorAll(sel)).filter((e) => e.offsetParent !== null);
      if (needle) {
        const rx = new RegExp(needle, 'i');
        els = els.filter((e) => rx.test(e.innerText || ''));
        els = els.filter((e) => !els.some((o) => o !== e && e.contains(o)));
      }
      return els.slice(0, 3).map((e) => e.outerHTML.replace(/\s+/g, ' ').slice(0, 2600));
    }, { sel: p.html_of, needle: p.html_needle ?? null });
  }
  return { stage: 'status', route, clicks, html, ...out };
}

// Read-only look at whatever the cabinet serves right now -- the dashboard when
// the session is alive, the Keycloak login form when it has expired. Field
// VALUES are never returned, only their lengths, so a credential cannot leak
// into a transcript or a log.
async function stageLogin(page, p) {
  await page.goto('https://accounts.wyregisteredagent.net/', { waitUntil: 'domcontentloaded', timeout: 60000 });
  await sleep(p.settle ?? 9000);
  const out = await page.evaluate(() => {
    const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
    return {
      url: location.href,
      title: document.title,
      onLogin: /openid-connect|login-actions|auth\//.test(location.href),
      inputs: Array.from(document.querySelectorAll('input,select')).map((e) => ({
        tag: e.tagName,
        type: e.type || null,
        id: e.id || null,
        name: e.getAttribute('name'),
        autocomplete: e.getAttribute('autocomplete'),
        required: e.required === true,
        disabled: e.disabled === true,
        visible: e.offsetParent !== null,
        valueLength: (e.value || '').length,
      })),
      buttons: Array.from(document.querySelectorAll('button,input[type="submit"],a.btn')).map((e) => ({
        tag: e.tagName,
        type: e.type || null,
        id: e.id || null,
        name: e.getAttribute('name'),
        text: clean(e.innerText || e.value).slice(0, 36),
        disabled: e.disabled === true,
        visible: e.offsetParent !== null,
      })),
      errors: Array.from(document.querySelectorAll('.alert,.kc-feedback-text,[role="alert"],.pf-c-alert__title'))
        .map((e) => clean(e.innerText)).filter(Boolean).slice(0, 6),
      forms: Array.from(document.querySelectorAll('form')).map((f) => ({ id: f.id || null, action: (f.action || '').slice(0, 120), method: f.method })),
    };
  });
  return { stage: 'login', ...out };
}

// --- autopay + saved-card stages -------------------------------------------

// Anything the cabinet might pop open to ask a question. Kept wide on purpose:
// the first run of a new widget is a discovery run, and a selector that is too
// narrow reports "nothing opened" when something did.
const WIDGET_SEL = '.p-dialog,.p-drawer,.p-sidebar,.p-overlaypanel,.p-popover,.p-menu,'
  + '.p-tieredmenu,.p-confirmdialog,.p-confirmpopup,.p-card,[role="dialog"],[role="menu"]';

const squash = (s) => (s || '').replace(/\s+/g, ' ').trim();

const eq2 = (hay, needle) => (hay || '').replace(/\s+/g, ' ').toLowerCase()
  .includes((needle || '').replace(/\s+/g, ' ').trim().toLowerCase());

// The Services table renders one autopay button per row, each carrying a unique
// aria-label ("Autopay *6754, autopay for Registered Agent Service for BCAX
// LLC"). The visible text is only the last 4, so two rows paid by the same card
// are indistinguishable by text -- the label is the only safe address.
async function readAutopayRows(page) {
  return page.evaluate(() => Array.from(document.querySelectorAll('button[aria-label*="autopay for"]'))
    .map((e, i) => {
      const label = e.getAttribute('aria-label') || '';
      const m = label.match(/^Autopay \*(\d{4}),\s*autopay for (.+?) for (.+)$/i);
      const row = e.closest('tr,.p-datatable-row');
      const txt = (s) => (s || '').replace(/\s+/g, ' ').trim();
      return {
        i,
        last4: m ? m[1] : null,
        service: m ? txt(m[2]) : null,
        company: m ? txt(m[3]) : null,
        visible: e.offsetParent !== null,
        rowText: row ? txt(row.innerText).slice(0, 170) : null,
      };
    }));
}

// The autopay button disappears from a row once autopay is switched off, so a
// turn-off cannot be verified by reading the buttons. Read the table text too.
async function readServiceTable(page) {
  return page.evaluate(() => Array.from(document.querySelectorAll('tr'))
    .filter((e) => e.offsetParent !== null)
    .map((e) => (e.innerText || '').replace(/\s+/g, ' ').trim())
    .filter((t) => t.length > 12 && t.length < 260));
}

// Dumps every open widget with the parts that matter for driving it: the card
// radios, the buttons, and the raw markup for whatever has none of those.
async function dumpWidgets(page) {
  return page.evaluate((sel) => {
    const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
    let els = Array.from(document.querySelectorAll(sel))
      .filter((e) => e.offsetParent !== null || getComputedStyle(e).position === 'fixed');
    els = els.filter((e) => !els.some((o) => o !== e && e.contains(o)));
    return els.slice(0, 4).map((e) => ({
      cls: ((e.className || '') + '').slice(0, 90),
      role: e.getAttribute('role'),
      text: clean(e.innerText).slice(0, 700),
      radios: Array.from(e.querySelectorAll('input[type="radio"],input[type="checkbox"]')).map((r) => ({
        type: r.type,
        name: r.getAttribute('name'),
        value: r.value,
        checked: r.checked,
        near: clean((r.closest('label,li,tr,div') || {}).innerText || '').slice(0, 70),
      })),
      buttons: Array.from(e.querySelectorAll('button,[role="button"],a'))
        .filter((b) => b.offsetParent !== null)
        .map((b) => ({ text: clean(b.innerText).slice(0, 44), aria: b.getAttribute('aria-label'), disabled: b.disabled === true })),
      selects: Array.from(e.querySelectorAll('.p-select,select')).map((s) => clean(s.innerText || '').slice(0, 70)),
      html: e.outerHTML.replace(/\s+/g, ' ').slice(0, 2600),
    }));
  }, WIDGET_SEL);
}

// Marks one element chosen in the page itself, so it can then be handed to
// clickReal (which needs a selector, and cannot express "the 12th of these").
async function tagByIndex(page, selector, index, tag) {
  return page.evaluate(({ sel, i, t }) => {
    document.querySelectorAll(`[data-ap="${t}"]`).forEach((e) => e.removeAttribute('data-ap'));
    const el = document.querySelectorAll(sel)[i];
    if (!el) return false;
    el.setAttribute('data-ap', t);
    return true;
  }, { sel: selector, i: index, t: tag });
}

// Clicks a row inside an open PrimeVue menu. It must NOT scroll: scrolling an
// element that lives inside an open overlay closes that overlay, so the row is
// gone before the press lands and the failure reads as "row not found". The
// card rows are labelled with bullets ("MasterCard \u2022\u2022\u2022\u2022 6754"), so the needle
// is matched against the aria-label and the text, not against an exact string.
async function clickMenuItem(page, needle) {
  const pt = await page.evaluate((n) => {
    const rx = new RegExp(n, 'i');
    const all = Array.from(document.querySelectorAll('[role="menuitem"],.p-menu-item'))
      .filter((e) => e.offsetParent !== null);
    const hits = all.filter((e) => rx.test(`${e.getAttribute('aria-label') || ''} ${e.innerText || ''}`));
    const el = hits[0];
    if (!el) return { ok: false, seen: all.map((e) => e.getAttribute('aria-label') || (e.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 40)) };
    const target = el.querySelector('.p-menu-item-link,.p-menu-item-content') || el;
    target.setAttribute('data-ap', 'menuitem');
    const r = target.getBoundingClientRect();
    const x = r.x + r.width / 2;
    const y = r.y + r.height / 2;
    const top = document.elementFromPoint(x, y);
    return {
      ok: true,
      x,
      y,
      matches: hits.length,
      label: el.getAttribute('aria-label'),
      inView: r.top >= 0 && r.bottom <= window.innerHeight && r.left >= 0,
      obstructed: top ? !(top === target || target.contains(top) || top.contains(target)) : true,
      topmost: top ? `${top.tagName}.${((top.className || '') + '').slice(0, 40)}` : null,
    };
  }, needle);
  if (!pt.ok) return pt;
  if (pt.inView && !pt.obstructed) {
    await page.mouse.move(pt.x, pt.y);
    await page.mouse.click(pt.x, pt.y);
    return { ...pt, via: 'mouse' };
  }
  // A menu item's handler sits on the link element and fires on a native click.
  const did = await page.evaluate(() => {
    const el = document.querySelector('[data-ap="menuitem"]');
    if (!el) return false;
    el.click();
    return true;
  });
  return { ...pt, via: did ? 'native-fallback' : null, clicked: did };
}

// Changes which saved card pays for one service, or -- with inspect:true --
// only opens the widget and reports its shape, changing nothing.
async function stageAutopay(page, p) {
  await gotoRoute(page, 'services', p.settle ?? 13000);
  const before = await readAutopayRows(page);
  const eq = (a, b) => squash(a).toLowerCase() === squash(b).toLowerCase();
  const idx = before.findIndex((r) => eq(r.service, p.service) && eq(r.company, p.company));
  if (idx < 0) {
    return {
      stage: 'autopay',
      error: `no services row for ${JSON.stringify(p.service)} / ${JSON.stringify(p.company)}`,
      rows: before,
    };
  }
  const row = before[idx];
  // Refuse to act on a row that is not the one the caller described -- a stale
  // expectation means the page changed under the plan.
  if (p.from_last4 && row.last4 !== String(p.from_last4)) {
    return { stage: 'autopay', error: `row pays with *${row.last4}, caller expected *${p.from_last4}`, row };
  }

  const tagged = await tagByIndex(page, 'button[aria-label*="autopay for"]', idx, 'target');
  if (!tagged) return { stage: 'autopay', error: 'could not tag the autopay button', row };
  const click = await clickReal(page, { selector: '[data-ap="target"]' });
  await sleep(p.widget_settle ?? 6000);
  const widgets = await dumpWidgets(page);

  const out = { stage: 'autopay', row, clicked: click.via, widgets };
  if (p.inspect || (!p.want_last4 && !p.turn_off)) return { ...out, note: 'inspect only -- nothing was changed' };
  if (p.want_last4 && p.turn_off) return { ...out, error: 'pass either want_last4 or turn_off, not both' };

  // Switching autopay off leaves the service in place and stops it charging a
  // card. Verified against the table text, since the row's autopay button is
  // removed once there is no card to name.
  if (p.turn_off) {
    const off = await clickMenuItem(page, 'Turn\\s*Off\\s*Autopay');
    out.picked = off;
    if (!off.ok) return { ...out, error: 'the menu has no "Turn Off Autopay" row' };
    await sleep(3000);
    out.afterPick = await dumpWidgets(page);
    out.confirmed = await clickIf(page, { text: '^\\s*(Turn Off Autopay|Turn Off|Yes|Confirm|OK)\\s*$', tags: 'button,[role="button"]', prefer: 'shortest' });
    await sleep(p.save_settle ?? 7000);

    await gotoRoute(page, 'services', p.verify_settle ?? 12000);
    const rowsAfter = await readAutopayRows(page);
    const table = await readServiceTable(page);
    const line = table.find((t) => eq2(t, p.service) && eq2(t, p.company));
    out.after = { button: rowsAfter.find((r) => eq(r.service, p.service) && eq(r.company, p.company)) || null, line: line || null };
    out.ok = !!line && !/Autopay\s*\*\d{4}/i.test(line);
    if (!out.ok) out.error = line ? `row still reads: ${line}` : 'could not find the row after the change';
    return out;
  }

  // The widget is a menu whose rows are the saved cards; choosing one applies
  // the change straight away, so there is usually no Save to press afterwards.
  const want = String(p.want_last4);
  const picked = await clickMenuItem(page, `${want}(\\b|$)`);
  out.picked = picked;
  if (!picked.ok) return { ...out, error: `no menu row for a card ending ${want}` };
  if (picked.matches > 1) return { ...out, error: `${picked.matches} menu rows match ${want} -- refusing to guess` };
  await sleep(3000);
  out.afterPick = await dumpWidgets(page);

  // Only press a Save/Confirm if one actually appeared.
  const saveText = p.save_text || 'Save|Update|Confirm|Yes|Change Autopay|Set as Autopay';
  out.saved = await clickIf(page, { text: `^\\s*(${saveText})\\s*$`, tags: 'button,[role="button"]', prefer: 'shortest' });
  await sleep(p.save_settle ?? 7000);

  // Verify by reloading the inventory, not by trusting a toast.
  await gotoRoute(page, 'services', p.verify_settle ?? 12000);
  const after = await readAutopayRows(page);
  const nowRow = after.find((r) => eq(r.service, p.service) && eq(r.company, p.company));
  out.after = nowRow || null;
  out.ok = !!nowRow && nowRow.last4 === want;
  if (!out.ok) out.error = `autopay still reads *${nowRow ? nowRow.last4 : '??'}, wanted *${want}`;
  return out;
}

// Looks at the saved payment methods, and -- only when explicitly confirmed --
// deletes one. Deleting a card is irreversible, so it needs the last 4 twice:
// once to find the card, once as confirm_delete.
async function stageCards(page, p) {
  const route = p.route || 'account/payment-methods';
  await gotoRoute(page, route, p.settle ?? 12000);
  const read = () => page.evaluate(() => {
    const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
    return {
      url: location.href,
      headings: Array.from(document.querySelectorAll('h1,h2,h3,h4')).filter((e) => e.offsetParent !== null)
        .map((e) => clean(e.innerText)).filter(Boolean).slice(0, 12),
      cards: Array.from(document.querySelectorAll('tr,li,.p-card,[class*="card-row" i],[class*="payment" i]'))
        .filter((e) => e.offsetParent !== null && /\d{4}/.test(e.innerText || '') && /visa|mastercard|master card|amex|discover|ending|\*{2,}/i.test(e.innerText || ''))
        .map((e) => clean(e.innerText).slice(0, 180)),
      controls: Array.from(document.querySelectorAll('button,[role="button"],a,[class*="trash" i],[class*="delete" i]'))
        .filter((e) => e.offsetParent !== null)
        .map((e) => ({
          tag: e.tagName,
          text: clean(e.innerText).slice(0, 40),
          aria: e.getAttribute('aria-label'),
          title: e.getAttribute('title'),
          href: (e.getAttribute('href') || '').slice(0, 70),
          icon: Array.from(e.querySelectorAll('i,svg')).map((n) => ((n.className || '') + '').slice(0, 44)).filter(Boolean).slice(0, 2),
        }))
        .filter((c) => c.text || c.aria || c.title || c.icon.length),
      bodyText: clean(document.body.innerText).slice(0, 1500),
    };
  });
  const before = await read();
  const out = { stage: 'cards', route, before };

  // The card rows are plain text until "Edit credit cards" is pressed; the
  // per-card delete controls only exist inside that edit mode.
  if (p.edit_first) {
    out.edit = await clickIf(page, { selector: 'button[aria-label="Edit credit cards"]' });
    if (!out.edit) return { ...out, error: 'could not press "Edit credit cards"' };
    await sleep(p.edit_settle ?? 4000);
    out.inEdit = await read();
  }

  if (p.html_of) {
    out.html = await page.evaluate(({ sel, needle }) => {
      let els = Array.from(document.querySelectorAll(sel)).filter((e) => e.offsetParent !== null);
      if (needle) {
        const rx = new RegExp(needle, 'i');
        els = els.filter((e) => rx.test(e.innerText || ''));
        els = els.filter((e) => !els.some((o) => o !== e && e.contains(o)));
      }
      return els.slice(0, 3).map((e) => e.outerHTML.replace(/\s+/g, ' ').slice(0, 2800));
    }, { sel: p.html_of, needle: p.html_needle ?? null });
  }

  if (!p.delete_last4) return { ...out, note: 'read only -- pass delete_last4 + confirm_delete to remove a card' };
  if (String(p.confirm_delete) !== String(p.delete_last4)) {
    return { ...out, error: 'confirm_delete must repeat delete_last4 -- deleting a saved card cannot be undone' };
  }

  const want = String(p.delete_last4);
  // Each card has its own remove control, labelled with that card ("Remove
  // MasterCard ending 9465"). That label is the only address that cannot hit
  // the wrong card, so an ambiguous match is refused rather than guessed.
  const tagged = await page.evaluate((w) => {
    const rx = new RegExp(`remove.*ending\\s*${w}\\b`, 'i');
    const btns = Array.from(document.querySelectorAll('button,[role="button"]'))
      .filter((e) => e.offsetParent !== null)
      .filter((e) => rx.test(e.getAttribute('aria-label') || ''));
    const labels = Array.from(document.querySelectorAll('button,[role="button"]'))
      .map((e) => e.getAttribute('aria-label'))
      .filter((a) => a && /remove/i.test(a));
    if (btns.length !== 1) return { ok: false, reason: `${btns.length} remove controls match "ending ${w}"`, labels };
    document.querySelectorAll('[data-ap="del"]').forEach((e) => e.removeAttribute('data-ap'));
    btns[0].setAttribute('data-ap', 'del');
    return { ok: true, ctrl: { aria: btns[0].getAttribute('aria-label') }, labels };
  }, want);
  out.locate = tagged;
  if (!tagged.ok) return { ...out, error: `could not locate a delete control for *${want}: ${tagged.reason}` };

  const click = await clickReal(page, { selector: '[data-ap="del"]' });
  out.clicked = click.via;
  await sleep(p.widget_settle ?? 5000);
  out.widgets = await dumpWidgets(page);

  // Most cabinets put a second confirm behind the first click.
  const confirmText = p.confirm_text || 'Delete|Remove|Yes|Confirm|Delete Payment Method|Remove Card';
  out.confirmed = await clickIf(page, { text: `^\\s*(${confirmText})\\s*$`, tags: 'button,[role="button"]', prefer: 'shortest' });
  await sleep(p.confirm_settle ?? 7000);

  await gotoRoute(page, route, p.verify_settle ?? 11000);
  out.after = await read();
  out.ok = !out.after.cards.some((c) => c.includes(want));
  if (!out.ok) out.error = `*${want} is still listed after the delete`;
  return out;
}

const STAGES = { create: stageCreate, cart: stageCart, pay: stagePay, details: stageDetails, ra: stageRa, contact: stageContact, status: stageStatus, login: stageLogin, autopay: stageAutopay, cards: stageCards };

export async function wyregFormLlc(page, payload = {}) {
  const stage = payload.stage;
  const fn = STAGES[stage];
  if (!fn) {
    return {
      portal: 'wyreg_form_llc',
      error: `unknown stage: ${JSON.stringify(stage)}`,
      stages: Object.keys(STAGES),
      note: 'Run the stages in order. Each one returns the id the next one needs. '
        + 'Only "pay" spends money, and it needs confirm_charge:true plus confirm_total from "cart".',
      defaults: BCA_DEFAULTS,
    };
  }
  const out = await fn(page, payload);
  return { portal: 'wyreg_form_llc', ...out };
}

export default wyregFormLlc;
