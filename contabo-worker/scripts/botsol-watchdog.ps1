# Botsol watchdog — detects Crawler Complete popup + Chrome "Aw, Snap!" crash.
# Runs every 10 min via Scheduled Task as the interactive user (administrator).
# Posts to Slack #growth via webhook; auto-clicks Reload on crashed Chrome tab.

$ErrorActionPreference = 'Stop'

# Read SLACK_WEBHOOK from orchestrator.env (same source as botsol-agent.ps1).
# Fallback: SLACK_WEBHOOK env var.
$EnvFile = 'C:\worker\orchestrator.env'
$SlackWebhook = $env:SLACK_WEBHOOK
if (Test-Path $EnvFile) {
    Get-Content $EnvFile | ForEach-Object {
        if ($_ -match '^\s*SLACK_WEBHOOK\s*=\s*(.+?)\s*$') { $SlackWebhook = $Matches[1] }
    }
}
$StatePath    = 'C:\worker\botsol-state.json'
$LogDir       = 'C:\worker\logs'
$LogPath      = Join-Path $LogDir 'botsol-watchdog.log'
$ReloadCooldownSec = 300
$HeartbeatIntervalSec = 14400  # 4 hours — periodic "how's it going" pulse on long scrapes
$StallAlertMin = 30            # hung-stop deadlock needs a MANUAL restart; alert at 30 min
$StallAlertCooldownSec = 3600  # at most one stall backstop alert per hour
$QueueStatePath = 'C:\worker\botsol-queue-state.json'  # the agent's run state (not this watchdog's)

if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Force -Path $LogDir | Out-Null }

function Write-Log {
    param([string]$msg)
    $line = "{0}  {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg
    Add-Content -Path $LogPath -Value $line -Encoding UTF8
}

function Load-State {
    $defaults = @{
        last_crawler_complete_sig = $null
        last_chrome_reload_ts     = 0
        last_run_ts               = 0
        last_heartbeat_ts         = 0
        last_stall_alert_ts       = 0
        last_botsol_pid           = 0
        pid_change_streak         = 0
    }
    $state = $null
    if (Test-Path $StatePath) {
        try { $state = Get-Content $StatePath -Raw | ConvertFrom-Json } catch { }
    }
    if (-not $state) { $state = [pscustomobject]@{} }
    # Hydrate missing fields so dynamic assignment never silently fails on JSON-loaded objects
    foreach ($k in $defaults.Keys) {
        if (-not ($state.PSObject.Properties.Name -contains $k)) {
            Add-Member -InputObject $state -MemberType NoteProperty -Name $k -Value $defaults[$k]
        }
    }
    return $state
}

function Save-State {
    param($state)
    $state | ConvertTo-Json -Depth 10 | Set-Content -Path $StatePath -Encoding UTF8
}

function Post-Slack {
    param([string]$text)
    try {
        $body = @{ text = $text } | ConvertTo-Json -Compress
        $null = Invoke-RestMethod -Uri $SlackWebhook -Method Post -Body $body `
                 -ContentType 'application/json' -TimeoutSec 15
        Write-Log "SLACK OK: $text"
    } catch {
        Write-Log ("SLACK FAIL: {0}" -f $_.Exception.Message)
    }
}

function Now-Unix { [int][double]::Parse((Get-Date -UFormat %s)) }

try {
    Add-Type -AssemblyName UIAutomationClient
    Add-Type -AssemblyName UIAutomationTypes
} catch {
    Write-Log ("FATAL: could not load UIAutomation assemblies: {0}" -f $_.Exception.Message)
    exit 1
}

$state = Load-State
$now   = Now-Unix

Write-Log "watchdog start (user=$env:USERNAME, session=$env:SESSIONNAME)"

$root = [System.Windows.Automation.AutomationElement]::RootElement
if (-not $root) {
    Write-Log 'ERROR: RootElement null — interactive desktop not available. Exiting.'
    $state.last_run_ts = $now
    Save-State $state
    exit 0
}

# --- Check 0: ensure BotsolApp is running (relaunch if the whole app died) ---
# Without this, a full BotsolApp crash/close halts the 24/7 scrape loop until a
# human re-launches it from RDP. The watchdog runs InteractiveToken in the user
# session, so Start-Process lands the window on the interactive desktop where the
# botsol-agent (also session-2 interactive) can find and drive it.
$BotsolExe = 'C:\Program Files (x86)\Botsol\Botsol Crawler\BotsolApp.exe'
$botsolProc = Get-Process -Name 'BotsolApp' -ErrorAction SilentlyContinue
if (-not $botsolProc) {
    if (Test-Path $BotsolExe) {
        try {
            Start-Process -FilePath $BotsolExe -WorkingDirectory (Split-Path $BotsolExe)
            Write-Log "BotsolApp not running — relaunched from $BotsolExe"
            Post-Slack ":rotating_light: *BotsolApp was down — watchdog relaunched it* on $env:COMPUTERNAME. Agent resumes the scrape loop within ~2 min."
            Start-Sleep -Seconds 12   # let the window initialize before the UIA checks below
        } catch {
            Write-Log ("ERROR: failed to relaunch BotsolApp: {0}" -f $_.Exception.Message)
            Post-Slack ":x: *BOTSOLAPP DOWN AND WATCHDOG RELAUNCH FAILED* on $env:COMPUTERNAME — needs manual RDP launch."
        }
    } else {
        Write-Log "ERROR: BotsolApp not running and exe missing at $BotsolExe"
    }
} else {
    Write-Log ("BotsolApp running (PID {0})" -f $botsolProc.Id)
    # --- Check 0b: PID crash-loop detector ---
    # A different PID on every check means BotsolApp keeps dying + relaunching (144 PID
    # changes on 2026-07-07). Alert once per episode: exactly when the consecutive-change
    # counter hits 4; a stable check resets it.
    $curBotsolPid = [int](@($botsolProc)[0].Id)   # @() guards the multi-process case mid-crash-loop
    $lastBotsolPid = 0
    try { $lastBotsolPid = [int]$state.last_botsol_pid } catch {}
    $pidStreak = 0
    try { $pidStreak = [int]$state.pid_change_streak } catch {}
    if ($lastBotsolPid -ne 0 -and $curBotsolPid -ne $lastBotsolPid) {
        $pidStreak = $pidStreak + 1
        Write-Log ("BotsolApp PID changed ({0} -> {1}); consecutive-change streak={2}" -f $lastBotsolPid, $curBotsolPid, $pidStreak)
        if ($pidStreak -eq 4) {
            Post-Slack (":rotating_light: *BOTSOL CRASH-LOOPING* on $env:COMPUTERNAME: PID changed on $pidStreak consecutive checks — BotsolApp keeps dying and restarting, no scraping is happening. Manual attention needed.")
        }
    } else {
        if ($pidStreak -gt 0) { Write-Log ("BotsolApp PID stable ({0}); crash-loop streak reset (was {1})" -f $curBotsolPid, $pidStreak) }
        $pidStreak = 0
    }
    $state.last_botsol_pid = $curBotsolPid
    $state.pid_change_streak = $pidStreak
}

# Enumerate top-level windows
$allWindows = @()
try {
    $allWindows = $root.FindAll(
        [System.Windows.Automation.TreeScope]::Children,
        [System.Windows.Automation.Condition]::TrueCondition
    )
} catch {
    Write-Log ("WARN: FindAll top windows failed: {0}" -f $_.Exception.Message)
}

Write-Log ("scanned {0} top-level windows" -f $allWindows.Count)

# --- Check 1: Botsol Crawler Complete popup ---
$crawlerHit = $null
foreach ($w in $allWindows) {
    try {
        $name = $w.Current.Name
        if ($name -and ($name -match 'Crawler\s*Complete' -or $name -match 'Scraping\s*Complete' -or $name -match 'Crawl\s*Complete')) {
            $crawlerHit = $w
            break
        }
    } catch { }
}

if ($crawlerHit) {
    $sig = '{0}|{1}' -f $crawlerHit.Current.Name, $crawlerHit.Current.NativeWindowHandle
    if ($state.last_crawler_complete_sig -ne $sig) {
        Write-Log (":white_check_mark: Botsol crawler-complete popup seen (agent auto-extracts) Window: {0}" -f $crawlerHit.Current.Name)
        $state.last_crawler_complete_sig = $sig
    } else {
        Write-Log "Crawler Complete already alerted (sig=$sig)"
    }
} else {
    if ($state.last_crawler_complete_sig) {
        Write-Log 'Clearing stale crawler_complete_sig (popup gone)'
        $state.last_crawler_complete_sig = $null
    }
}

# --- Check 2: Chrome "Aw, Snap!" crash → click Reload ---
$chromeWindows = @()
try {
    $chromeWindows = $root.FindAll(
        [System.Windows.Automation.TreeScope]::Children,
        (New-Object System.Windows.Automation.PropertyCondition(
            [System.Windows.Automation.AutomationElement]::ClassNameProperty,
            'Chrome_WidgetWin_1'))
    )
} catch {
    Write-Log ("WARN: Chrome window scan failed: {0}" -f $_.Exception.Message)
}

Write-Log ("Chrome windows found: {0}" -f $chromeWindows.Count)

$lastReload = [int]$state.last_chrome_reload_ts
$reloadedThisRun = $false
$chromeScanErrLogged = $false   # UIA FindFirst on Chrome throws every run (broken for weeks) — log once per run, debug level

foreach ($cw in $chromeWindows) {
    if ($reloadedThisRun) { break }
    try {
        $awSnap = $cw.FindFirst(
            [System.Windows.Automation.TreeScope]::Descendants,
            (New-Object System.Windows.Automation.PropertyCondition(
                [System.Windows.Automation.AutomationElement]::NameProperty,
                'Aw, Snap!'))
        )
        if (-not $awSnap) {
            # Fallback: look for 'Something went wrong while displaying this webpage.'
            $altText = $cw.FindFirst(
                [System.Windows.Automation.TreeScope]::Descendants,
                (New-Object System.Windows.Automation.PropertyCondition(
                    [System.Windows.Automation.AutomationElement]::NameProperty,
                    'Something went wrong while displaying this webpage.'))
            )
            if (-not $altText) { continue }
        }

        if (($now - $lastReload) -lt $ReloadCooldownSec) {
            Write-Log ("Aw, Snap! detected but cooldown active ({0}s < {1}s)" -f ($now - $lastReload), $ReloadCooldownSec)
            continue
        }

        # Find Reload button (crash page button is Name='Reload', control type Button).
        $reloadBtns = $cw.FindAll(
            [System.Windows.Automation.TreeScope]::Descendants,
            (New-Object System.Windows.Automation.AndCondition(
                (New-Object System.Windows.Automation.PropertyCondition(
                    [System.Windows.Automation.AutomationElement]::ControlTypeProperty,
                    [System.Windows.Automation.ControlType]::Button)),
                (New-Object System.Windows.Automation.PropertyCondition(
                    [System.Windows.Automation.AutomationElement]::NameProperty,
                    'Reload'))
            ))
        )

        $clicked = $false
        foreach ($btn in $reloadBtns) {
            try {
                $pat = $btn.GetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern)
                $pat.Invoke()
                $clicked = $true
                break
            } catch { }
        }

        if ($clicked) {
            Write-Log ':arrows_counterclockwise: Botsol Chrome auto-reloaded after Aw-Snap crash (routine; not alerting).'
            $state.last_chrome_reload_ts = $now
            $reloadedThisRun = $true
        } else {
            Write-Log 'Aw, Snap! detected but no Reload button invoked (button not found or Invoke failed)'
        }
    } catch {
        # Aw-Snap scan is best-effort/decorative when UIA is broken: degrade to 'chrome=?'
        # instead of erroring once per window per run.
        if (-not $chromeScanErrLogged) {
            Write-Log ("DEBUG: Chrome scan degraded (chrome=?), Aw-Snap check skipped this run: {0}" -f $_.Exception.Message)
            $chromeScanErrLogged = $true
        }
    }
}


# --- Check 4: frozen-progress backstop alert ---
# The agent self-heals a frozen run (see Get-StallDecision in botsol-agent.ps1). This
# is a coarser safety net: if the agent reports no scrape progress for a long time
# (e.g. its own task died, or self-heal failed), alert a human. Only fires when the
# agent has an active run state; normal IDLE gaps clear queue-state, so no false alarms.
try {
    if (Test-Path $QueueStatePath) {
        $qs = $null
        try { $qs = Get-Content $QueueStatePath -Raw | ConvertFrom-Json } catch {}
        if ($qs -and $qs.stall_last_progress_at) {
            $lp = $null
            try { $lp = [datetime]::Parse([string]$qs.stall_last_progress_at, $null, [System.Globalization.DateTimeStyles]::RoundtripKind) } catch {}
            if ($lp) {
                $ageMin = [int]((Get-Date) - $lp).TotalMinutes
                $lastAlert = 0; try { $lastAlert = [int]$state.last_stall_alert_ts } catch {}
                if ($ageMin -ge $StallAlertMin -and ($now - $lastAlert) -ge $StallAlertCooldownSec) {
                    Post-Slack (":rotating_light: *BOTSOL STUCK — MANUAL RESTART NEEDED* on $env:COMPUTERNAME. No scrape progress for ~$ageMin min on ``$($qs.current_country)/$($qs.current_source_file)`` (record #$($qs.stall_last_record)). This is the hung-``Stopping…`` deadlock — the agent CANNOT clear it without a restart (by design, it never kills BotsolApp). :point_right: *ACTION:* RDP into 66.70.134.73 → close + relaunch BotsolApp → click the bot + *Select*. Re-reminding hourly until scraping resumes.")
                    $state.last_stall_alert_ts = $now
                    Write-Log "stall backstop alert posted (age=$ageMin min)"
                }
            }
        }
    }
} catch { Write-Log ("Check4 stall backstop error: {0}" -f $_.Exception.Message) }

# --- Check 3: Periodic heartbeat (every 4h) — light "how's it going" pulse ---
$lastHb = 0
try { $lastHb = [int]$state.last_heartbeat_ts } catch {}
if (($now - $lastHb) -ge $HeartbeatIntervalSec) {
    $agentTail = ''
    try {
        $agentTail = (Get-Content 'C:\worker\logs\botsol-agent.log' -Tail 3 -ErrorAction SilentlyContinue) -join "`n"
    } catch {}
    $pendingCount = 0
    try {
        $pendingCount = (Get-ChildItem 'C:\Botsol\pipeline\keywords_v2' -Recurse -Filter '*.starter.txt' -ErrorAction SilentlyContinue | Where-Object { $_.Directory.Name -notin @('done','skipped','_skip') }).Count
    } catch {}
    $qs = $null
    try { $qs = Get-Content 'C:\worker\botsol-queue-state.json' -Raw -ErrorAction SilentlyContinue | ConvertFrom-Json } catch {}
    $cur = if ($qs -and $qs.current_country) { "$($qs.current_country)/$($qs.current_source_file) @ record #$($qs.stall_last_record)" } else { 'between categories' }
    $hbMsg = @"
:heart: *Botsol still running* on $env:COMPUTERNAME ($((Get-Date).ToString('HH:mm')) local)
- now scraping: $cur
- categories left in queue: $pendingCount
"@
    # Staleness gate: a live process means nothing if the output pipeline is dead (45h of
    # hearts through zero output on 2026-07-05/07). Newest archived CSV older than 3h
    # (a single keyword file can legitimately take ~2h) => STALLED alert instead of the heart.
    $newestArc = $null
    try {
        $newestArc = Get-ChildItem 'C:\Botsol\archive' -File -ErrorAction SilentlyContinue |
            Sort-Object LastWriteTime -Descending | Select-Object -First 1
    } catch {}
    $arcAgeH = -1
    if ($newestArc) { $arcAgeH = [math]::Round(((Get-Date) - $newestArc.LastWriteTime).TotalHours, 1) }
    if ($newestArc -and $arcAgeH -ge 3) {
        Post-Slack (":rotating_light: *BOTSOL STALLED* on $env:COMPUTERNAME: no archived CSV since $($newestArc.LastWriteTime.ToString('yyyy-MM-dd HH:mm')) (${arcAgeH}h ago) — output pipeline dead. Currently reported: $cur, $pendingCount categories left in queue.")
        Write-Log "heartbeat suppressed: archive stale ${arcAgeH}h (newest: $($newestArc.Name)) — posted STALLED alert instead"
    } else {
        Post-Slack $hbMsg
    }
    $state.last_heartbeat_ts = $now
}

$state.last_run_ts = $now
Save-State $state
Write-Log 'watchdog run complete'
