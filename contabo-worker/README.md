# Contabo Browser Worker

Runs on the Contabo Windows VPS next to AdsPower. Shared HTTP service that all Spexx apps (claims-hq, bca-app, flights, growthops) call over the Tailnet to drive anti-bot browser sessions without punching AdsPower's loopback CDP through Windows firewall.

**Why:** AdsPower's local API returns a CDP websocket bound to `127.0.0.1` on the Windows host. Remote containers on Hostinger cannot reach it directly. This worker is the bridge — it runs on the same host as AdsPower, so `connectOverCDP` works, and apps talk to it over HTTP on `:7070` through Tailscale.

## Install (PowerShell, one-time on Contabo)

```powershell
winget install OpenJS.NodeJS.LTS
# Copy this folder to C:\worker
cd C:\worker
npm install
npx playwright install chromium

# Create env file
@"
ADS_URL=http://127.0.0.1:50325
SCREENSHOT_DIR=C:\worker\screenshots
PORT=7070
# Comma-separated role:token pairs. Admin can call /exec; others cannot.
WORKER_TOKENS=admin:$(New-Guid),claims:$(New-Guid),bca:$(New-Guid),flights:$(New-Guid)
ADSPOWER_EXE=C:\Program Files (x86)\AdsPower Global\AdsPower Global.exe
"@ | Out-File -FilePath .env -Encoding ascii

# Run (ideally install as a Windows service later — see NSSM below)
node --env-file=.env server.mjs
```

Listens on `0.0.0.0:7070`. Publicly reachable only if the Tailnet interface is the only one routable — confirm with `netstat -an | findstr 7070`. Windows Firewall rule allows `:7070` only from `100.64.0.0/10` (Tailnet range).

### Run as a Windows service (recommended for production)

```powershell
# Install NSSM
choco install nssm
nssm install contabo-worker "C:\Program Files\nodejs\node.exe" "--env-file=C:\worker\.env C:\worker\server.mjs"
nssm set contabo-worker AppDirectory C:\worker
nssm set contabo-worker AppStdout C:\worker\logs\out.log
nssm set contabo-worker AppStderr C:\worker\logs\err.log
nssm start contabo-worker
```

## Authentication

All routes except `/health` require a bearer token:

```
Authorization: Bearer <token>
```

Token → role mapping comes from `WORKER_TOKENS` env var. Roles today:
- `admin` — full access including `/exec` (arbitrary PowerShell).
- `claims`, `bca`, `flights`, `growthops` — per-app tokens. Can call `/submit`, `/status`, `/adspower/*`. Cannot call `/exec`.

In dev (no tokens configured) auth is bypassed and everyone is admin — useful for local testing, catastrophic in prod. The `/health` response advertises which mode it's in.

## Endpoints

| Method | Path | Role | Purpose |
|---|---|---|---|
| GET | `/health` | none | Liveness + auth mode |
| GET | `/status` | any | Worker + AdsPower reachability + uptime |
| POST | `/submit` | any | Run a portal handler in an AdsPower session |
| GET | `/adspower/profiles` | any | List AdsPower profiles (for picking profile IDs in app UIs) |
| POST | `/adspower/ensure-running` | any | Start AdsPower desktop app if closed, poll API until ready |
| POST | `/exec` | admin | Run a PowerShell command on the Contabo host |

### `POST /submit`

```json
{
  "profileId": "k1bqusgh",
  "portal": "govbr",
  "useAdsPower": true,
  "claimId": "cb275f97-...",
  "action": "submit_new",
  "body": "Full complaint text...",
  "companyName": "GOL Linhas Aéreas",
  "bookingRef": "AWEZHN",
  "desiredOutcome": "Refund BRL 1,430"
}
```

Set `useAdsPower: false` to bypass AdsPower and launch plain headless Playwright — use for regulator portals (consumidor.gov.br, ENAC, PROFECO webdenuncia) that don't fingerprint-check, saves profile-opens on the free plan's ~10/day cap.

Portal handlers implemented:
- `peek`, `btb`, `profeco`, `webdenuncia`, `enac`, `inspect`, `xcover_status` — original claims-hq portals
- `govbr` — consumidor.gov.br (actions: `submit_new`, `read_status`)
- More to come as Claims HQ adapters are ported over from `admin/claims/server/services/portalAgent/adapters/`.

### `POST /exec` (admin only)

```json
{ "cmd": "Get-Process | Where-Object {$_.Name -like '*ads*'}" }
```

Use cases: "wake AdsPower", "tail worker log", "restart the worker service", "check disk space". Not a generic remote shell — think of it as a narrow admin escape hatch when RDP is not an option.

## Network

Nothing on this worker should be reachable from the public internet. Enforcement:
- Tailscale ACLs restrict `:7070` to the `claims-hq` + `bca-app` + `flights` Tailnet hosts + Alex's laptop.
- Windows Firewall rule: allow inbound TCP `:7070` only from `100.64.0.0/10` (Tailnet CGNAT).
- Never proxy this port via Cloudflare, nginx, or any public-facing router.
