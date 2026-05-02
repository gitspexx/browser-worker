# bootstrap-botsol.ps1
# Cold-start handler for Botsol Crawler:
#   - if main "Crawler App" window already up → no-op (idempotent)
#   - else launch BotsolApp.exe, wait for "Select Bot" dialog, pick first ListItem,
#     click "Select" button, wait for main window to appear.
# Runs via Scheduled Task BotsolBootstrap (Administrator / Interactive — needs UIA desktop).
# Triggered by /exec → schtasks /run /tn BotsolBootstrap.

$ErrorActionPreference = 'Stop'
$LogDir = 'C:\worker\logs'
$Log = Join-Path $LogDir 'bootstrap-botsol.log'
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Force -Path $LogDir | Out-Null }
function W([string]$m) {
    $line = "{0}  {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $m
    Add-Content -Path $Log -Value $line -Encoding UTF8
    Write-Host $line
}

Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes

W "bootstrap start (user=$env:USERNAME, session=$env:SESSIONNAME)"

function Get-Root { [System.Windows.Automation.AutomationElement]::RootElement }
function PCondName($name) {
    New-Object System.Windows.Automation.PropertyCondition(
        [System.Windows.Automation.AutomationElement]::NameProperty, $name)
}
function PCondType($type) {
    New-Object System.Windows.Automation.PropertyCondition(
        [System.Windows.Automation.AutomationElement]::ControlTypeProperty, $type)
}

function Find-MainWindow {
    $root = Get-Root
    $cond = PCondType ([System.Windows.Automation.ControlType]::Window)
    $windows = $root.FindAll([System.Windows.Automation.TreeScope]::Children, $cond)
    foreach ($w in $windows) {
        try {
            $n = $w.Current.Name
            $cls = $w.Current.ClassName
            if ($cls -like 'WindowsForms10*' -and $n -match 'Crawler App|Business Profiles Scraper') {
                return $w
            }
        } catch {}
    }
    return $null
}

function Find-SelectBotDialog {
    $root = Get-Root
    $cond = PCondType ([System.Windows.Automation.ControlType]::Window)
    $windows = $root.FindAll([System.Windows.Automation.TreeScope]::Children, $cond)
    foreach ($w in $windows) {
        try {
            $n = $w.Current.Name
            # Bootstrap dialog title is exactly "Botsol Crawler"; main app contains "Crawler App"
            if ($n -eq 'Botsol Crawler') { return $w }
            if ($n -match 'Select\s*Bot') { return $w }
        } catch {}
    }
    return $null
}

# 1) Already up? Done.
$existing = Find-MainWindow
if ($existing) {
    W "main window already up: '$($existing.Current.Name)' — nothing to do"
    exit 0
}

# 2) Launch
$exe = 'C:\Program Files (x86)\Botsol\Botsol Crawler\BotsolApp.exe'
if (-not (Test-Path -LiteralPath $exe)) { W "ERROR: exe not found: $exe"; exit 1 }
W "launching $exe"
Start-Process -FilePath $exe

# 3) Wait for Select Bot dialog
$dialog = $null
$deadline = (Get-Date).AddSeconds(45)
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Milliseconds 500
    $dialog = Find-SelectBotDialog
    if ($dialog) { break }
    # Maybe it skipped the dialog and went straight to main window (single bot, remembered selection)
    $main = Find-MainWindow
    if ($main) { W "main window appeared without Select dialog: '$($main.Current.Name)'"; exit 0 }
}
if (-not $dialog) { W 'ERROR: Select Bot dialog never appeared within 45s'; exit 2 }
W "dialog found: '$($dialog.Current.Name)'"

# 4) Find ComboBox inside dialog
$combo = $dialog.FindFirst(
    [System.Windows.Automation.TreeScope]::Descendants,
    (PCondType ([System.Windows.Automation.ControlType]::ComboBox)))
if (-not $combo) { W 'ERROR: ComboBox not found in dialog'; exit 3 }
W 'combo found'

# 5) Expand combo
try {
    $exp = $combo.GetCurrentPattern([System.Windows.Automation.ExpandCollapsePattern]::Pattern)
    $exp.Expand()
    Start-Sleep -Milliseconds 600
    W 'combo expanded'
} catch { W "Expand failed (non-fatal): $($_.Exception.Message)" }

# 6) Find list items — first inside combo, fallback to entire desktop (popup may detach)
$itemCond = PCondType ([System.Windows.Automation.ControlType]::ListItem)
$items = $combo.FindAll([System.Windows.Automation.TreeScope]::Descendants, $itemCond)
if (-not $items -or $items.Count -eq 0) {
    W 'no items inside combo; scanning whole desktop'
    $items = (Get-Root).FindAll([System.Windows.Automation.TreeScope]::Descendants, $itemCond)
}

W ("listitems found: {0}" -f $items.Count)
$names = @()
foreach ($it in $items) {
    try { $names += $it.Current.Name } catch { $names += '<no-name>' }
}
W ("items: " + ($names -join ' | '))
if ($items.Count -lt 1) { W 'ERROR: no list items to select'; exit 4 }

# 7) Select first
$first = $items[0]
W "selecting first item: '$($first.Current.Name)'"
$selected = $false
try {
    $sip = $first.GetCurrentPattern([System.Windows.Automation.SelectionItemPattern]::Pattern)
    $sip.Select()
    $selected = $true
} catch { W "SelectionItemPattern.Select failed: $($_.Exception.Message)" }
if (-not $selected) {
    try {
        $inv = $first.GetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern)
        $inv.Invoke()
        $selected = $true
        W 'used InvokePattern fallback'
    } catch { W "InvokePattern fallback failed: $($_.Exception.Message)" }
}
if (-not $selected) { W 'ERROR: could not select first item'; exit 5 }
Start-Sleep -Milliseconds 400

# 8) Click Select button
$selectBtn = $dialog.FindFirst(
    [System.Windows.Automation.TreeScope]::Descendants,
    (New-Object System.Windows.Automation.AndCondition(
        (PCondType ([System.Windows.Automation.ControlType]::Button)),
        (PCondName 'Select'))))
if (-not $selectBtn) { W 'ERROR: Select button not found'; exit 6 }
try {
    $selectBtn.GetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern).Invoke()
    W 'clicked Select'
} catch { W "Invoke Select button failed: $($_.Exception.Message)"; exit 6 }

# 9) Wait for main window
$deadline = (Get-Date).AddSeconds(60)
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Milliseconds 750
    $main = Find-MainWindow
    if ($main) { W "MAIN window up: '$($main.Current.Name)'"; exit 0 }
}
W 'ERROR: main window never appeared after Select click'
exit 7
