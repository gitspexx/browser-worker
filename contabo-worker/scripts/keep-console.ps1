# keep-console.ps1 - keeps the 'admin' interactive session attached to the
# physical console so it stays "Active" AND actually rendering, so BotsolApp's
# UIA automation can both READ and CLICK the window.
#
# Two failure modes are handled:
#   1. Classic disconnect: query session reports admin as Disc -> tscon to console.
#   2. Dead-viewer "Active": query session LIES "Active" (RDP viewer dropped at the
#      network level) but the desktop isn't rendering, so UIA can't even enumerate
#      the window. Detected via the agent's own log: if BotsolApp is alive yet the
#      last several ticks all say "window not found", force it back to the console.
#      This is the bug that cost ~45 min on 2026-06-11.
#
# Runs as SYSTEM (only SYSTEM/SeTcbPrivilege can tscon to console). Never disrupts
# a genuinely-working RDP user: a real user means the agent CAN see the window, so
# the blind-heal branch never fires for them.
$ErrorActionPreference = 'SilentlyContinue'
$log = 'C:\worker\logs\keep-console.log'
function L($m) { try { Add-Content -Path $log -Value ("{0}  {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $m) -Encoding UTF8 } catch {} }

# Slack webhook (same source as the agent/watchdog)
$wh = $null
foreach ($ln in (Get-Content 'C:\worker\orchestrator.env' -EA SilentlyContinue)) {
    if ($ln -match '^\s*SLACK_WEBHOOK\s*=\s*(.+?)\s*$') { $wh = $Matches[1] }
}
function Slack($t) { if ($wh) { try { Invoke-RestMethod -Uri $wh -Method Post -Body (@{ text = $t } | ConvertTo-Json -Compress) -ContentType 'application/json' -TimeoutSec 15 | Out-Null } catch {} } }

# locate the admin session
$adminId = $null; $adminState = $null
foreach ($ln in ((query session) 2>$null)) {
    if ($ln -match 'admin\s+(\d+)\s+(Active|Disc|Conn|Listen)') {
        $adminId = $Matches[1]; $adminState = $Matches[2]; break
    }
}
if (-not $adminId) { L 'admin session not found'; exit 0 }

function Reattach($why) {
    L "$why -> tscon $adminId /dest:console"
    $out = & tscon $adminId /dest:console 2>&1
    L ("tscon result: {0}" -f (($out | Out-String).Trim()))
    Start-Sleep -Seconds 2
    $after = ((query session) 2>$null | Select-String '\badmin\b' | Select-Object -First 1)
    L ("after: {0}" -f ("$after").Trim())
}

# Case 1: classic disconnected session.
if ($adminState -ne 'Active') { Reattach "admin session state=$adminState"; exit 0 }

# Case 2: session claims Active but the agent is BLIND to the window.
# GATE: if BotsolApp is sitting on the 'Select Bot' chooser, ChooserNav owns it and
# console-reattach can't help (the chooser is human-gated). Skip entirely so we don't
# thrash tscon + spam Slack every cycle (cost: ~40 false self-heal alerts on 2026-06-12).
if (Test-Path 'C:\worker\chooser-alerted.marker') { L 'chooser up (ChooserNav owns it) - skipping blind self-heal'; exit 0 }
$bot = Get-Process BotsolApp -EA SilentlyContinue
if ($bot) {
    $tail = Get-Content 'C:\worker\logs\botsol-agent.log' -Tail 8 -EA SilentlyContinue
    $blind = @($tail | Select-String 'window not found' -SimpleMatch).Count
    if ($blind -ge 3) {
        $mk  = 'C:\worker\keepconsole-heal.marker'
        $now = [int]([datetime]::UtcNow - [datetime]'1970-01-01').TotalSeconds
        $last = 0; if (Test-Path $mk) { try { $last = [int]((Get-Content $mk -Raw).Trim()) } catch {} }
        if (($now - $last) -gt 300) {   # at most one heal + alert per 5 min
            Reattach "Active but agent BLIND ($blind/8 lines 'window not found'), BotsolApp pid=$($bot.Id) alive -> forcing console"
            Set-Content $mk "$now"
            Slack (":arrows_counterclockwise: *Botsol self-heal* on $env:COMPUTERNAME - the session reported 'Active' but the agent had gone blind to the BotsolApp window ($blind/8 ticks). Forced it back to the physical console; scraping should resume within ~2 min. No human action needed.")
        }
    }
}
exit 0
