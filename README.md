# @spexx/browser-worker

Shared anti-bot browser automation for the Spexx ecosystem. Two pieces:

- **`contabo-worker/`** — long-running HTTP service that wraps AdsPower + Playwright on a Windows VPS. Runs on Contabo at `100.85.100.70:7070` over Tailscale.
- **`src/`** — TypeScript HTTP client (`@spexx/browser-worker`) that Claims HQ, BCA app, flights, and growthops call to drive browser sessions without bundling Playwright/Chromium into their own images.

## Why this exists

AdsPower's local API returns a CDP websocket bound to `127.0.0.1`. Containers on Hostinger (where the Spexx apps live) cannot reach that loopback even over Tailscale. The worker bridges them: Playwright runs on the same host as AdsPower (loopback works), and apps on Hostinger delegate over HTTPS-on-Tailnet.

## Topology

```
Hostinger app container ──► Tailscale ──► Contabo :7070 ──► 127.0.0.1:50325 (AdsPower API)
                                                       ──► Playwright (headless Chromium)
```

## Consumers

- [`gitspexx/claims-hq`](https://github.com/gitspexx/claims-hq) — Claims HQ (first consumer, live 2026-04-23).
- BCA app, flights, growthops — will adopt the same client when anti-bot flows ship.

See `contabo-worker/README.md` for install/operations on the Windows host.
