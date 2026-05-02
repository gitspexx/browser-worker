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

if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Force -Path $LogDir | Out-Null }

function Write-Log {
    param([string]$msg)
    $line = "{0}  {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg
    Add-Content -Path $LogPath -Value $line -Encoding UTF8
}

function Load-State {
    if (Test-Path $StatePath) {
        try { return (Get-Content $StatePath -Raw | ConvertFrom-Json) } catch { }
    }
    return [pscustomobject]@{
        last_crawler_complete_sig = $null
        last_chrome_reload_ts     = 0
        last_run_ts               = 0
    }
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
        Post-Slack (":white_check_mark: *Botsol crawler complete* on Contabo — save the CSV from the output folder and kick off the next batch. Window: ``{0}``" -f $crawlerHit.Current.Name)
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
            Post-Slack ':arrows_counterclockwise: *Botsol Chrome auto-reloaded* on Contabo after `Aw, Snap!` crash.'
            $state.last_chrome_reload_ts = $now
            $reloadedThisRun = $true
        } else {
            Write-Log 'Aw, Snap! detected but no Reload button invoked (button not found or Invoke failed)'
        }
    } catch {
        Write-Log ("Chrome scan error: {0}" -f $_.Exception.Message)
    }
}

$state.last_run_ts = $now
Save-State $state
Write-Log 'watchdog run complete'
