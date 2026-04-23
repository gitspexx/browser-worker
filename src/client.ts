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
}
