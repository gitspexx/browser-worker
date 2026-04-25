export interface BrowserWorkerClientOptions {
  /** e.g. http://100.85.100.70:7070 over Tailscale */
  baseUrl: string;
  /** Bearer token issued by the worker's WORKER_TOKENS env. */
  token: string;
  /** Optional fetch override (tests, node-fetch shim). */
  fetch?: typeof fetch;
  /** Default per-call timeout. Overridable per request. */
  timeoutMs?: number;
}

export interface SubmitRequest {
  profileId?: string;
  portal: string;
  useAdsPower?: boolean;
  [payload: string]: unknown;
}

export interface SubmitResponse {
  ok: boolean;
  [key: string]: unknown;
}

export interface StatusResponse {
  ok: boolean;
  role: string;
  adspower: { reachable: boolean; url: string };
  worker: { port: number; node: string; uptime_s: number };
}

export interface ExecRequest {
  cmd: string;
  cwd?: string;
  timeout_ms?: number;
  shell?: 'powershell' | 'cmd';
}

export interface ExecResponse {
  ok: boolean;
  id: string;
  duration_ms: number;
  stdout?: string;
  stderr?: string;
  error?: string;
  code?: number;
}

export interface AdsPowerProfile {
  user_id: string;
  name?: string;
  group_id?: string;
  remark?: string;
  [key: string]: unknown;
}

/** One document pulled from wyreg's authenticated inbox. The buffer is the
 *  PDF bytes base64-encoded so the response stays JSON-serialisable over HTTP. */
export interface WyregPolledDoc {
  upstream_doc_id: string;
  filename: string;
  upstream_order_id: string | null;
  entity_name_hint: string | null;
  filing_date_hint: string | null;
  buffer_base64: string;
}

export interface WyregPollDocsResponse {
  ok: boolean;
  portal: 'wyreg_poll_docs';
  url: string;
  docs: WyregPolledDoc[];
}

export interface WyregDocRowDescriptor {
  docId: string;
  filename: string;
  entityNameHint: string | null;
  filingDateHint: string | null;
  upstreamOrderId: string | null;
  downloadHref: string | null;
  downloadClickSelector: string | null;
}

export interface WyregInspectDocsResponse {
  ok: boolean;
  portal: 'wyreg_inspect_docs';
  url: string;
  rows: WyregDocRowDescriptor[];
  nav_links: Array<{ tag: string; text: string; href: string | null; class: string }>;
  business_tiles: Array<{ text: string; href: string | null }>;
  /** Filename on the worker's SCREENSHOT_DIR; fetchable via GET /screenshot/:name. */
  screenshot: string;
  /** Same screenshot, inlined so callers don't need a second round-trip. */
  screenshot_base64: string;
  html_excerpt: string;
  body_text_excerpt: string;
  routes_attempted: Array<{ url: string; settled: boolean; reason: string }>;
}

/** Operating-hours check for the IRS EIN Online Assistant (Mon-Fri 07:00-22:00 ET). */
export interface IrsEinOperatingHours {
  open: boolean;
  reason: string;
  next_open_at: string | null;
}

export interface IrsEinFormFieldOption {
  value: string;
  text: string;
  selected: boolean;
}

export interface IrsEinFormField {
  tag: 'input' | 'select' | 'textarea';
  name: string | null;
  id: string | null;
  /** Present for `input` (HTML type attr); `null` for select/textarea. */
  type: string | null;
  /** Best-effort: <label for=id> first, else nearest ancestor label / text. */
  label: string | null;
  placeholder: string | null;
  value: string | null;
  /** Populated when tag === 'select'. */
  options?: IrsEinFormFieldOption[];
}

export interface IrsEinStepVisited {
  step_label: string;
  url: string;
  screenshot_path: string;
}

export interface IrsEinInspectResponse {
  ok: boolean;
  portal: 'irs_apply_ein_inspect';
  url: string;
  /** Label of the page the walk stopped on ("llc-members-question" on the happy path). */
  step_label: string;
  steps_visited: IrsEinStepVisited[];
  /** Visible inputs on the final page. Hidden/submit/button inputs are excluded. */
  form_fields: IrsEinFormField[];
  nav_links: Array<{ tag: string; text: string; href: string | null }>;
  /** First 8k of visible text on the final page. */
  body_text_excerpt: string;
  /** First 40k of `document.documentElement.outerHTML`. */
  html_excerpt: string;
  operating_hours: IrsEinOperatingHours;
}

// ── Phase 2 (placeholder types) ──────────────────────────────────────────
// Shapes are provisional — `applyEinDryRun` / `applyEinSubmit` will land in
// a follow-up PR once the inspect output confirms the page flow. Fields
// here are "reasonable guesses" to pin down the call-site surface; expect
// them to tighten in phase 2.

export interface IrsEinResponsibleParty {
  first_name: string;
  middle_initial?: string;
  last_name: string;
  suffix?: string;
  /** SSN or ITIN of the responsible party. One EIN per SSN per day per IRS. */
  ssn_or_itin: string;
  title?: string;
}

export interface IrsEinBusinessAddress {
  street: string;
  street2?: string;
  city: string;
  state: string;
  zip: string;
  country?: string;
}

export interface IrsEinBusinessDetails {
  legal_name: string;
  trade_name?: string;
  mailing_address: IrsEinBusinessAddress;
  /** Physical address if different from mailing; else omit. */
  physical_address?: IrsEinBusinessAddress;
  county: string;
  state_of_formation: string;
  llc_members_count: number;
  formation_date: string; // ISO yyyy-mm-dd
  /** e.g. 'started-new-business', 'hired-employees', 'banking-purposes'. */
  reason_for_applying: string;
  accounting_year_close_month: number; // 1-12
  expects_employees: boolean;
}

// phase 2
export interface IrsEinDryRunRequest {
  profileId: string;
  jobId?: string;
  /** Caller-generated token (UUID). Echo it back to applyEinSubmit only after
   *  the operator has reviewed the dryrun screenshots — the worker does not
   *  enforce the gate, the app side does. */
  confirmation_token: string;
  payload: {
    responsibleParty: IrsEinResponsibleParty;
    business: IrsEinBusinessDetails;
  };
}

// phase 2
export interface IrsEinDryRunResponse {
  ok?: boolean;
  portal: 'irs_apply_ein_dryrun';
  url: string;
  phase: 'review';
  confirmation_token: string;
  steps_visited: Array<{ step_label: string; url: string; screenshot_path: string }>;
  /** Server-side path to the Review page screenshot. Caller (state-filing-worker)
   *  fetches via /screenshot/:name or via direct file read on a shared volume. */
  review_screenshot_path?: string;
  /** Best-effort label/value pairs scraped from the Review page (dl/table). */
  review_snapshot: Record<string, Array<[string, string]>>;
  operating_hours: IrsEinOperatingHours;
}

// phase 2
export interface IrsEinSubmitRequest {
  profileId: string;
  jobId?: string;
  /** Must match the operator-approved dryrun token. */
  confirmation_token: string;
  /** Same payload that was used for the dryrun. IRS sessions don't survive
   *  between calls (each call is a fresh AdsPower attach), so submit must
   *  re-fill the form from scratch using the verified data. */
  payload: {
    responsibleParty: IrsEinResponsibleParty;
    business: IrsEinBusinessDetails;
  };
}

// phase 2
export interface IrsEinSubmitResponse {
  ok?: boolean;
  portal: 'irs_apply_ein_submit';
  url: string;
  phase: 'submitted';
  confirmation_token: string;
  steps_visited: Array<{ step_label: string; url: string; screenshot_path: string }>;
  /** Assigned EIN string in XX-XXXXXXX format; null only if extraction failed
   *  (in that case operator must read the EIN off the screenshot). */
  ein: string | null;
  /** CP 575 confirmation PDF base64. Null if IRS didn't expose the link. */
  cp575_pdf_base64: string | null;
  operating_hours: IrsEinOperatingHours;
}

export class BrowserWorkerError extends Error {
  constructor(message: string, readonly status: number, readonly body: unknown) {
    super(message);
    this.name = 'BrowserWorkerError';
  }
}

export class BrowserWorkerClient {
  private readonly baseUrl: string;
  private readonly token: string;
  private readonly fetchImpl: typeof fetch;
  private readonly defaultTimeoutMs: number;

  constructor(opts: BrowserWorkerClientOptions) {
    if (!opts.baseUrl) throw new Error('baseUrl required');
    if (!opts.token) throw new Error('token required');
    this.baseUrl = opts.baseUrl.replace(/\/$/, '');
    this.token = opts.token;
    this.fetchImpl = opts.fetch ?? fetch;
    this.defaultTimeoutMs = opts.timeoutMs ?? 120_000;
  }

  private async request<T>(method: 'GET' | 'POST', path: string, body?: unknown, timeoutMs?: number): Promise<T> {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), timeoutMs ?? this.defaultTimeoutMs);
    try {
      const res = await this.fetchImpl(`${this.baseUrl}${path}`, {
        method,
        headers: {
          Authorization: `Bearer ${this.token}`,
          ...(body ? { 'Content-Type': 'application/json' } : {}),
        },
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });
      const text = await res.text();
      const parsed = text ? JSON.parse(text) : {};
      if (!res.ok) throw new BrowserWorkerError(`${method} ${path} → ${res.status}`, res.status, parsed);
      return parsed as T;
    } finally {
      clearTimeout(t);
    }
  }

  /** Simple liveness — does not require a valid token. */
  async health(): Promise<{ ok: boolean; auth: 'enabled' | 'disabled' }> {
    const res = await this.fetchImpl(`${this.baseUrl}/health`);
    return res.json() as Promise<{ ok: boolean; auth: 'enabled' | 'disabled' }>;
  }

  status(): Promise<StatusResponse> {
    return this.request('GET', '/status');
  }

  /** Run a portal handler on the worker. Returns handler-specific payload. */
  submit(req: SubmitRequest, opts: { timeoutMs?: number } = {}): Promise<SubmitResponse> {
    return this.request('POST', '/submit', req, opts.timeoutMs ?? 300_000);
  }

  listProfiles(): Promise<{ ok: boolean; profiles: AdsPowerProfile[] }> {
    return this.request('GET', '/adspower/profiles');
  }

  /** Starts the AdsPower desktop app if it's not running; polls until the local API responds. */
  ensureAdsPowerRunning(): Promise<{ ok: boolean; already_running?: boolean; started?: boolean; waited_ms?: number }> {
    return this.request('POST', '/adspower/ensure-running', {}, 45_000);
  }

  /** Admin-token-only. Runs a PowerShell command on the Contabo host. Narrow escape hatch. */
  exec(req: ExecRequest): Promise<ExecResponse> {
    return this.request('POST', '/exec', req, (req.timeout_ms ?? 30_000) + 5_000);
  }

  /**
   * Poll the wyregisteredagent.net documents inbox for a given AdsPower profile.
   * Credentials live on the worker (WYREG_USERNAME/WYREG_PASSWORD); the persistent
   * profile keeps the Keycloak session warm so this call rarely has to log in.
   * PDF bytes come back base64-encoded inside each doc entry.
   */
  pollWyregDocs(opts: { profileId: string; jobId?: string; timeoutMs?: number }): Promise<WyregPollDocsResponse> {
    const { profileId, jobId, timeoutMs } = opts;
    return this.submit(
      { profileId, portal: 'wyreg_poll_docs', jobId },
      { timeoutMs: timeoutMs ?? 5 * 60_000 },
    ) as unknown as Promise<WyregPollDocsResponse>;
  }

  /**
   * Diagnostic flavour of pollWyregDocs: no downloads, returns nav/tile snapshot
   * and a screenshot (inline base64 + worker-side filename) so an operator can
   * tune selectors against the live portal.
   */
  inspectWyregDocs(opts: { profileId: string; jobId?: string; timeoutMs?: number }): Promise<WyregInspectDocsResponse> {
    const { profileId, jobId, timeoutMs } = opts;
    return this.submit(
      { profileId, portal: 'wyreg_inspect_docs', jobId },
      { timeoutMs: timeoutMs ?? 3 * 60_000 },
    ) as unknown as Promise<WyregInspectDocsResponse>;
  }

  /**
   * Discovery-only walk of the IRS EIN Online Assistant. Navigates to the
   * LLC number-of-members question, dumps form fields + nav + screenshots,
   * stops BEFORE any real data is required. Throws if the IRS portal is
   * outside Mon-Fri 07:00-22:00 ET.
   */
  async inspectEinForm(opts: { profileId: string; jobId?: string; timeoutMs?: number }): Promise<IrsEinInspectResponse> {
    const { profileId, jobId, timeoutMs } = opts;
    return this.submit(
      { profileId, portal: 'irs_apply_ein_inspect', jobId },
      { timeoutMs: timeoutMs ?? 3 * 60_000 },
    ) as unknown as Promise<IrsEinInspectResponse>;
  }

  /**
   * Phase 2: fill the IRS EIN Assistant from Step 1 to the Review page and
   * STOP there (no submit, no SSN-quota burn). Caller supplies a
   * confirmation_token (UUID is fine) which the operator must echo back to
   * applyEinSubmit to authorize the live submission.
   *
   * Selectors for Steps 2-4 are in active discovery — see worker handler
   * fillIrsEinForm TODO markers. Until tuning lands, calls may fail with
   * `fillIrsEinForm[stepN]: <selector> not found` errors that show which
   * specific input couldn't be located. Iterate selectors against the
   * inspect handler's form_fields output.
   */
  async applyEinDryRun(req: IrsEinDryRunRequest, opts: { timeoutMs?: number } = {}): Promise<IrsEinDryRunResponse> {
    const { profileId, jobId, confirmation_token, payload } = req as IrsEinDryRunRequest & {
      confirmation_token: string;
      payload: { responsibleParty: IrsEinResponsibleParty; business: IrsEinBusinessDetails };
    };
    if (!confirmation_token) throw new Error('applyEinDryRun: confirmation_token required');
    return this.submit(
      { profileId, portal: 'irs_apply_ein_dryrun', jobId, confirmation_token, payload },
      { timeoutMs: opts.timeoutMs ?? 5 * 60_000 },
    ) as unknown as Promise<IrsEinDryRunResponse>;
  }

  /**
   * Phase 2: re-fill the assistant end to end and click Submit. Returns the
   * issued EIN + CP 575 PDF base64. The caller MUST verify the
   * confirmation_token came from a fresh, operator-approved dryrun before
   * calling this — IRS allows ONE EIN per responsible-party SSN per day,
   * and a bad submit is locked in.
   */
  async applyEinSubmit(req: IrsEinSubmitRequest, opts: { timeoutMs?: number } = {}): Promise<IrsEinSubmitResponse> {
    const { profileId, jobId, confirmation_token, payload } = req as IrsEinSubmitRequest & {
      confirmation_token: string;
      payload: { responsibleParty: IrsEinResponsibleParty; business: IrsEinBusinessDetails };
    };
    if (!confirmation_token) throw new Error('applyEinSubmit: confirmation_token required');
    if (!payload) throw new Error('applyEinSubmit: payload required (must match the dryrun payload)');
    return this.submit(
      { profileId, portal: 'irs_apply_ein_submit', jobId, confirmation_token, payload },
      { timeoutMs: opts.timeoutMs ?? 5 * 60_000 },
    ) as unknown as Promise<IrsEinSubmitResponse>;
  }
}
