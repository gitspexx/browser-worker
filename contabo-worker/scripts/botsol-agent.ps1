#Requires -Version 5.1
# ==============================================================================
# botsol-agent.ps1
# One-tick orchestrator for Botsol (WinForms) on Contabo Windows VPS.
# Scheduled-task driven; runs every 2 minutes; never loops.
# ==============================================================================

$ErrorActionPreference = 'Continue'

# ---------- Paths ----------
$ENV_FILE       = 'C:\worker\orchestrator.env'
$STATE_FILE     = 'C:\worker\botsol-queue-state.json'
$LOG_DIR        = 'C:\worker\logs'
$LOG_FILE       = Join-Path $LOG_DIR 'botsol-agent.log'
$KEYWORDS_ROOT  = 'C:\Botsol\pipeline\keywords_v2'
$OUTPUT_DIR     = 'C:\Botsol\output'

# ---------- Logging ----------
function Ensure-Dir([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) {
        try { New-Item -ItemType Directory -Path $Path -Force | Out-Null } catch {}
    }
}

Ensure-Dir $LOG_DIR
Ensure-Dir $OUTPUT_DIR

function Write-Log {
    param(
        [string]$Message,
        [ValidateSet('info','warn','error','debug')]
        [string]$Level = 'info'
    )
    $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $line = "{0}  [{1}] {2}" -f $ts, $Level, $Message
    try {
        Add-Content -LiteralPath $LOG_FILE -Value $line -Encoding UTF8
    } catch {}
    try { Write-Host $line } catch {}
}

# ---------- Config loader ----------
function Read-EnvFile {
    param([string]$Path)
    $cfg = @{}
    if (-not (Test-Path -LiteralPath $Path)) {
        Write-Log "env file not found at $Path" 'warn'
        return $cfg
    }
    foreach ($raw in (Get-Content -LiteralPath $Path -Encoding UTF8)) {
        $line = $raw.Trim()
        if (-not $line) { continue }
        if ($line.StartsWith('#')) { continue }
        $eq = $line.IndexOf('=')
        if ($eq -lt 1) { continue }
        $k = $line.Substring(0,$eq).Trim()
        $v = $line.Substring($eq+1).Trim()
        $cfg[$k] = $v
    }
    return $cfg
}

function Parse-Bool {
    param($Value, [bool]$Default = $true)
    if ($null -eq $Value) { return $Default }
    $s = ([string]$Value).Trim().ToLowerInvariant()
    if ($s -eq 'true' -or $s -eq '1' -or $s -eq 'yes' -or $s -eq 'on') { return $true }
    if ($s -eq 'false' -or $s -eq '0' -or $s -eq 'no' -or $s -eq 'off') { return $false }
    return $Default
}

$cfg = Read-EnvFile $ENV_FILE
$KOLLABLY_URL     = $cfg['KOLLABLY_URL']
$KOLLABLY_KEY     = $cfg['KOLLABLY_KEY']
$SLACK_WEBHOOK    = $cfg['SLACK_WEBHOOK']
$DRY_RUN          = Parse-Bool $cfg['DRY_RUN'] $true
$AUTO_START_NEXT       = Parse-Bool $cfg['AUTO_START_NEXT'] $false
$AUTO_DELETE_CURRENT   = Parse-Bool $cfg['AUTO_DELETE_CURRENT'] $false
$USE_STARTER           = Parse-Bool $cfg['USE_STARTER'] $true
$USE_PRUNED            = Parse-Bool $cfg['USE_PRUNED'] $true

# ONLY_COUNTRIES = comma-separated allow-list (e.g. "colombia,mexico"). Empty = all.
# Useful for staged rollout: validate one country end-to-end before unleashing the
# full alphabetical sweep across ~80 countries.
$ONLY_COUNTRIES = @()
if ($cfg['ONLY_COUNTRIES']) {
    $ONLY_COUNTRIES = ($cfg['ONLY_COUNTRIES'] -split ',') |
        ForEach-Object { $_.Trim().ToLowerInvariant() } |
        Where-Object { $_ }
}
$RESULT_CAP            = if ($cfg['RESULT_CAP'])    { [string]$cfg['RESULT_CAP'] }    else { '100' }
$KEYWORD_LIMIT         = if ($cfg['KEYWORD_LIMIT']) { [string]$cfg['KEYWORD_LIMIT'] } else { '5' }

Write-Log "tick start  dry_run=$DRY_RUN  auto_start_next=$AUTO_START_NEXT  auto_delete_current=$AUTO_DELETE_CURRENT  use_starter=$USE_STARTER  use_pruned=$USE_PRUNED  result_cap=$RESULT_CAP  keyword_limit=$KEYWORD_LIMIT"

# ---------- Slack helper ----------
function Post-Slack {
    param([string]$Text)
    if (-not $SLACK_WEBHOOK) { return }
    try {
        $body = @{ text = $Text } | ConvertTo-Json -Compress -Depth 4
        Invoke-RestMethod -Uri $SLACK_WEBHOOK -Method Post -Body $body `
            -ContentType 'application/json' -TimeoutSec 10 | Out-Null
    } catch {
        Write-Log "slack post failed: $($_.Exception.Message)" 'warn'
    }
}

# ---------- Supabase helpers ----------
function Get-SupaHeaders {
    return @{
        'apikey'        = $KOLLABLY_KEY
        'Authorization' = "Bearer $KOLLABLY_KEY"
        'Content-Type'  = 'application/json'
        'Prefer'        = 'return=representation'
    }
}

function Insert-ScrapeJob {
    param([hashtable]$Row)
    if (-not $KOLLABLY_URL -or -not $KOLLABLY_KEY) {
        Write-Log "supabase not configured; skipping insert" 'warn'
        return $null
    }
    try {
        $uri = "$KOLLABLY_URL/rest/v1/scrape_jobs"
        $body = ConvertTo-Json @($Row) -Depth 6 -Compress
        $resp = Invoke-RestMethod -Uri $uri -Method Post -Headers (Get-SupaHeaders) `
            -Body $body -TimeoutSec 15
        if ($resp -and $resp.Count -gt 0) { return $resp[0] }
        return $resp
    } catch {
        Write-Log "Insert-ScrapeJob failed: $($_.Exception.Message)" 'error'
        return $null
    }
}

function Update-ScrapeJob {
    param(
        [Parameter(Mandatory=$true)] $Id,
        [Parameter(Mandatory=$true)] [hashtable]$Patch
    )
    if (-not $KOLLABLY_URL -or -not $KOLLABLY_KEY) {
        Write-Log "supabase not configured; skipping update" 'warn'
        return $null
    }
    if (-not $Id) {
        Write-Log "Update-ScrapeJob called with empty id" 'warn'
        return $null
    }
    try {
        $uri = "$KOLLABLY_URL/rest/v1/scrape_jobs?id=eq.$Id"
        $body = ConvertTo-Json $Patch -Depth 6 -Compress
        $resp = Invoke-RestMethod -Uri $uri -Method Patch -Headers (Get-SupaHeaders) `
            -Body $body -TimeoutSec 15
        return $resp
    } catch {
        Write-Log "Update-ScrapeJob($Id) failed: $($_.Exception.Message)" 'error'
        return $null
    }
}

# ---------- Queue state ----------
function Read-QueueState {
    if (-not (Test-Path -LiteralPath $STATE_FILE)) { return $null }
    try {
        $raw = Get-Content -LiteralPath $STATE_FILE -Raw -Encoding UTF8
        if (-not $raw) { return $null }
        return ($raw | ConvertFrom-Json)
    } catch {
        Write-Log "queue-state.json parse failed: $($_.Exception.Message)" 'warn'
        return $null
    }
}

function Write-QueueState {
    param([hashtable]$State)
    try {
        $json = $State | ConvertTo-Json -Depth 6
        # Write UTF8 with BOM
        $bytes = [System.Text.Encoding]::UTF8.GetPreamble() + [System.Text.Encoding]::UTF8.GetBytes($json)
        [System.IO.File]::WriteAllBytes($STATE_FILE, $bytes)
    } catch {
        Write-Log "queue-state.json write failed: $($_.Exception.Message)" 'warn'
    }
}

function Clear-QueueState {
    if (Test-Path -LiteralPath $STATE_FILE) {
        try { Remove-Item -LiteralPath $STATE_FILE -ErrorAction SilentlyContinue } catch {}
    }
}

# ---------- Queue picker ----------
# Logic: walk countries alphabetically; per country, group files by category-stem
# (cafe.starter.txt + cafe.pruned.txt + cafe.txt all collapse to stem 'cafe').
# Skip stems that have <stem>.txt in <country>/done/ (already processed).
# For remaining stems alphabetically, pick best variant:
#   USE_STARTER + has starter -> starter
#   else USE_PRUNED + has pruned -> pruned
#   else original
function Get-FileVariant([string]$name) {
    if ($name -like '*.starter.txt') { return 'starter' }
    if ($name -like '*.pruned.txt')  { return 'pruned' }
    return 'original'
}
function Get-FileStem([string]$name) {
    return ($name -replace '\.starter\.txt$|\.pruned\.txt$|\.txt$', '')
}

function Get-NextQueueItem {
    if (-not (Test-Path -LiteralPath $KEYWORDS_ROOT)) {
        Write-Log "keywords root missing: $KEYWORDS_ROOT" 'warn'
        return $null
    }
    $countries = Get-ChildItem -LiteralPath $KEYWORDS_ROOT -Directory -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -notmatch '^[._]' } |
        Sort-Object Name

    if ($ONLY_COUNTRIES.Count -gt 0) {
        $countries = $countries | Where-Object { $ONLY_COUNTRIES -contains $_.Name.ToLowerInvariant() }
        if (-not $countries) {
            Write-Log "ONLY_COUNTRIES=$($ONLY_COUNTRIES -join ',') matched zero country dirs in $KEYWORDS_ROOT" 'warn'
            return $null
        }
    }

    foreach ($country in $countries) {
        $doneDir = Join-Path $country.FullName 'done'

        $files = Get-ChildItem -LiteralPath $country.FullName -File -Filter '*.txt' -ErrorAction SilentlyContinue
        if (-not $files) { continue }

        # Group by stem
        $byStem = @{}
        foreach ($f in $files) {
            $stem = Get-FileStem $f.Name
            $variant = Get-FileVariant $f.Name
            if (-not $byStem.ContainsKey($stem)) { $byStem[$stem] = @{} }
            $byStem[$stem][$variant] = $f
        }

        # Compute "done stems": any *.txt in done/ marks its stem as completed
        $doneStems = @{}
        if (Test-Path -LiteralPath $doneDir) {
            $doneFiles = Get-ChildItem -LiteralPath $doneDir -File -Filter '*.txt' -ErrorAction SilentlyContinue
            foreach ($df in $doneFiles) {
                $ds = Get-FileStem $df.Name
                $doneStems[$ds] = $true
            }
        }

        foreach ($stem in ($byStem.Keys | Sort-Object)) {
            if ($doneStems.ContainsKey($stem)) { continue }

            $variants = $byStem[$stem]
            $pick = $null
            $variantUsed = $null
            if ($USE_STARTER -and $variants.ContainsKey('starter')) {
                $pick = $variants['starter']; $variantUsed = 'starter'
            } elseif ($USE_PRUNED -and $variants.ContainsKey('pruned')) {
                $pick = $variants['pruned']; $variantUsed = 'pruned'
            } elseif ($variants.ContainsKey('original')) {
                $pick = $variants['original']; $variantUsed = 'original'
            } elseif ($variants.ContainsKey('pruned')) {
                $pick = $variants['pruned']; $variantUsed = 'pruned-fallback'
            } elseif ($variants.ContainsKey('starter')) {
                $pick = $variants['starter']; $variantUsed = 'starter-fallback'
            }

            if ($pick) {
                return [pscustomobject]@{
                    Country  = $country.Name
                    Stem     = $stem
                    FileName = $pick.Name
                    FullPath = $pick.FullName
                    Variant  = $variantUsed
                }
            }
        }
    }
    return $null
}

function Get-CountryProgress {
    param([string]$Country)
    $countryDir = Join-Path $KEYWORDS_ROOT $Country
    $doneDir = Join-Path $countryDir 'done'
    $remaining = 0
    $done = 0
    if (Test-Path -LiteralPath $countryDir) {
        $remaining = (Get-ChildItem -LiteralPath $countryDir -File -Filter '*.txt' -ErrorAction SilentlyContinue).Count
    }
    if (Test-Path -LiteralPath $doneDir) {
        $done = (Get-ChildItem -LiteralPath $doneDir -File -Filter '*.txt' -ErrorAction SilentlyContinue).Count
    }
    $total = $remaining + $done
    return [pscustomobject]@{ Done = $done; Total = $total; Remaining = $remaining }
}

function Count-Lines {
    param([string]$Path)
    try {
        $n = 0
        Get-Content -LiteralPath $Path -ErrorAction Stop | ForEach-Object {
            if ($_.Trim().Length -gt 0) { $n++ }
        }
        return $n
    } catch { return 0 }
}

# ---------- UIA ----------
function Load-Uia {
    try {
        Add-Type -AssemblyName UIAutomationClient -ErrorAction Stop
        Add-Type -AssemblyName UIAutomationTypes -ErrorAction Stop
        Add-Type -AssemblyName WindowsBase -ErrorAction Stop
        Add-Type -AssemblyName System.Windows.Forms -ErrorAction Stop  # SendKeys fallback
        return $true
    } catch {
        Write-Log "UIA assemblies failed to load: $($_.Exception.Message)" 'error'
        return $false
    }
}

function Find-BotsolWindow {
    $root = [System.Windows.Automation.AutomationElement]::RootElement
    $cond = New-Object System.Windows.Automation.PropertyCondition(
        [System.Windows.Automation.AutomationElement]::ControlTypeProperty,
        [System.Windows.Automation.ControlType]::Window)
    $windows = $root.FindAll([System.Windows.Automation.TreeScope]::Children, $cond)
    foreach ($w in $windows) {
        $cls = $null; $name = $null
        try { $cls = $w.Current.ClassName } catch {}
        try { $name = $w.Current.Name } catch {}
        if (-not $cls) { continue }
        if ($cls -like 'WindowsForms10*') {
            if ($name -match 'Crawler App|Business Profiles Scraper') {
                return $w
            }
        }
    }
    return $null
}

$script:BotsolChildrenCache = $null

function Get-BotsolChildren {
    param([System.Windows.Automation.AutomationElement]$Root)
    if ($null -ne $script:BotsolChildrenCache) { return $script:BotsolChildrenCache }
    if (-not $Root) { return @() }
    # Children scope only: Botsol's buttons + live-log EDIT live at depth 1 under the Window.
    # Descendants scope times out on the embedded Internet Explorer_Server panes
    # (hosted browser controls containing the 15K+ record rendered live log).
    try {
        $kids = $Root.FindAll(
            [System.Windows.Automation.TreeScope]::Children,
            [System.Windows.Automation.Condition]::TrueCondition)
        $script:BotsolChildrenCache = @($kids)
        $aidSummary = @()
        foreach ($k in $kids) {
            try {
                $aid = $k.Current.AutomationId
                $en  = $k.Current.IsEnabled
                $aidSummary += ("{0}={1}" -f $aid, $en)
            } catch {}
        }
        Write-Log ("botsol children count=$($kids.Count) aids: " + ($aidSummary -join ' '))
    } catch {
        Write-Log "FindAll Children failed: $($_.Exception.Message)" 'error'
        $script:BotsolChildrenCache = @()
    }
    return $script:BotsolChildrenCache
}

function Find-ByAutomationId {
    param(
        [System.Windows.Automation.AutomationElement]$Root,
        [string]$AutomationId
    )
    if (-not $Root) { return $null }
    # PropertyCondition on AutomationIdProperty is flaky for these WinForms controls —
    # falls back to enumerate-once (via cache) + PS-side filter.
    foreach ($k in (Get-BotsolChildren -Root $Root)) {
        try {
            if ($k.Current.AutomationId -eq $AutomationId) { return $k }
        } catch {}
    }
    return $null
}

function Get-ButtonEnabled {
    param([System.Windows.Automation.AutomationElement]$Btn)
    if (-not $Btn) { return $false }
    try { return [bool]$Btn.Current.IsEnabled } catch { return $false }
}

function Get-EditValue {
    param([System.Windows.Automation.AutomationElement]$Edit)
    if (-not $Edit) { return '' }
    try {
        $vp = $Edit.GetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern)
        if ($vp) { return [string]$vp.Current.Value }
    } catch {}
    try {
        $tp = $Edit.GetCurrentPattern([System.Windows.Automation.TextPattern]::Pattern)
        if ($tp) { return [string]$tp.DocumentRange.GetText(-1) }
    } catch {}
    return ''
}

function Set-EditValue {
    param(
        [System.Windows.Automation.AutomationElement]$Edit,
        [string]$Value
    )
    if (-not $Edit) { return $false }
    try {
        $vp = $Edit.GetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern)
        if ($vp) {
            $vp.SetValue($Value)
            return $true
        }
    } catch {
        Write-Log "Set-EditValue ValuePattern failed: $($_.Exception.Message)" 'warn'
    }
    return $false
}

function Invoke-Element {
    param([System.Windows.Automation.AutomationElement]$El)
    if (-not $El) { return $false }
    try {
        $ip = $El.GetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern)
        if ($ip) { $ip.Invoke(); return $true }
    } catch {
        Write-Log "Invoke-Element failed: $($_.Exception.Message)" 'warn'
    }
    return $false
}

function Wait-ForDialog {
    param(
        [int]$TimeoutSec = 10,
        [string]$NameMatch = 'Save|Open'
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        $root = [System.Windows.Automation.AutomationElement]::RootElement
        $cond = New-Object System.Windows.Automation.PropertyCondition(
            [System.Windows.Automation.AutomationElement]::ControlTypeProperty,
            [System.Windows.Automation.ControlType]::Window)
        $windows = $root.FindAll([System.Windows.Automation.TreeScope]::Children, $cond)
        foreach ($w in $windows) {
            $cls = ''; $name = ''
            try { $cls = $w.Current.ClassName } catch {}
            try { $name = $w.Current.Name } catch {}
            if ($cls -eq '#32770' -or ($name -and $name -match $NameMatch)) {
                return $w
            }
        }
        Start-Sleep -Milliseconds 400
    }
    return $null
}

function Find-DialogFilenameEdit {
    param([System.Windows.Automation.AutomationElement]$Dialog)
    if (-not $Dialog) { return $null }
    $editCond = New-Object System.Windows.Automation.PropertyCondition(
        [System.Windows.Automation.AutomationElement]::ControlTypeProperty,
        [System.Windows.Automation.ControlType]::Edit)
    # Prefer one named 'File name:'
    $edits = $Dialog.FindAll([System.Windows.Automation.TreeScope]::Descendants, $editCond)
    foreach ($e in $edits) {
        $n = ''
        try { $n = $e.Current.Name } catch {}
        if ($n -match 'File name') { return $e }
    }
    if ($edits.Count -gt 0) { return $edits[0] }
    return $null
}

function Find-DialogButton {
    param(
        [System.Windows.Automation.AutomationElement]$Dialog,
        [string]$NameMatch
    )
    if (-not $Dialog) { return $null }
    $btnCond = New-Object System.Windows.Automation.PropertyCondition(
        [System.Windows.Automation.AutomationElement]::ControlTypeProperty,
        [System.Windows.Automation.ControlType]::Button)
    $btns = $Dialog.FindAll([System.Windows.Automation.TreeScope]::Descendants, $btnCond)
    foreach ($b in $btns) {
        $n = ''
        try { $n = $b.Current.Name } catch {}
        if ($n -match $NameMatch) { return $b }
    }
    return $null
}

# Dismisses a frmBoolInput / "Botsol: User Input Required" Yes/No popup (a CHILD of Botsol's main
# window, not a top-level dialog). Used for the email-and-social-media question that appears on
# every Start Bot click. Returns $true if dismissed, $false if not present (which is fine to ignore).
function Dismiss-BoolInput {
    param([string]$ButtonName = 'Yes', [int]$TimeoutSec = 8)
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        $bot = Find-BotsolWindow
        if ($bot) {
            try {
                $kids = $bot.FindAll(
                    [System.Windows.Automation.TreeScope]::Children,
                    [System.Windows.Automation.Condition]::TrueCondition)
                foreach ($k in $kids) {
                    try {
                        $aid = $k.Current.AutomationId
                        $cls = $k.Current.ClassName
                        $en  = $k.Current.IsEnabled
                        # frmBoolInput / frmInput (Botsol's own dialogs) OR #32770 (standard
                        # Windows MessageBox like "Confirm Delete!!"). Both are children of BotForm.
                        if ($en -and ($aid -eq 'frmBoolInput' -or $aid -eq 'frmInput' -or $aid -like 'frmBool*' -or $cls -eq '#32770')) {
                            $btn = Find-DialogButton -Dialog $k -NameMatch ('^&?' + [regex]::Escape($ButtonName) + '$')
                            if ($btn -and (Invoke-Element $btn)) {
                                Write-Log "BoolInput child dialog (aid=$aid class=$cls) dismissed via '$ButtonName'"
                                return $true
                            }
                        }
                    } catch {}
                }
            } catch {}
        }
        # Fallback: top-level dialog with title containing "User Input Required"
        try {
            $root = [System.Windows.Automation.AutomationElement]::RootElement
            $wins = $root.FindAll(
                [System.Windows.Automation.TreeScope]::Children,
                [System.Windows.Automation.Condition]::TrueCondition)
            foreach ($w in $wins) {
                try {
                    $nm = [string]$w.Current.Name
                    if ($nm -match 'User Input Required|scrape email|social media') {
                        $btn = Find-DialogButton -Dialog $w -NameMatch ('^' + [regex]::Escape($ButtonName) + '$')
                        if ($btn -and (Invoke-Element $btn)) {
                            Write-Log "BoolInput top-level dialog '$nm' dismissed via '$ButtonName'"
                            return $true
                        }
                    }
                } catch {}
            }
        } catch {}
        Start-Sleep -Milliseconds 400
    }
    return $false
}

# Handles a numeric InputBox-style prompt: waits for a new top-level dialog window,
# fills its Edit control with $Value, and dismisses with OK / Yes / Enter fallback.
# Excludes Botsol main, Chrome, File Explorer, Save/Open dialogs from match.
# Used for the two prompts ('400' result cap + '5') Botsol shows after Start Bot.
function Handle-NumericPrompt {
    param(
        [string]$Value,
        [int]$TimeoutSec = 12,
        [string]$Tag = 'prompt'
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    $found = $null
    while ((Get-Date) -lt $deadline -and -not $found) {
        # Primary: search Botsol's children for frm*Input dialog (the pattern Botsol uses).
        # The "Botsol: User Input Required" prompts (numeric + bool) are CHILD windows of
        # Botsol's main, not top-level. AutomationIds like frmInput, frmIntInput, frmBoolInput.
        $bot = Find-BotsolWindow
        if ($bot) {
            try {
                $kids = $bot.FindAll(
                    [System.Windows.Automation.TreeScope]::Children,
                    [System.Windows.Automation.Condition]::TrueCondition)
                foreach ($k in $kids) {
                    try {
                        $aid = [string]$k.Current.AutomationId
                        $en  = [bool]$k.Current.IsEnabled
                        # Match any frm* child (frmInput, frmIntInput, frmSelectFile, frmFileInput, ...)
                        # The Edit-presence check below filters out frmBoolInput (no Edit, just Yes/No).
                        if ($en -and $aid -like 'frm*' -and $aid -ne 'frmBoolInput') {
                            $editCond = New-Object System.Windows.Automation.PropertyCondition(
                                [System.Windows.Automation.AutomationElement]::ControlTypeProperty,
                                [System.Windows.Automation.ControlType]::Edit)
                            $editsTest = $k.FindAll([System.Windows.Automation.TreeScope]::Descendants, $editCond)
                            if ($editsTest.Count -gt 0) { $found = $k; break }
                        }
                    } catch {}
                }
            } catch {}
        }
        # Fallback: top-level dialog with title containing "User Input Required" / "Records"
        if (-not $found) {
            try {
                $root = [System.Windows.Automation.AutomationElement]::RootElement
                $wins = $root.FindAll(
                    [System.Windows.Automation.TreeScope]::Children,
                    [System.Windows.Automation.Condition]::TrueCondition)
                foreach ($w in $wins) {
                    try {
                        $cls = $w.Current.ClassName
                        $nm  = [string]$w.Current.Name
                        if ($cls -like 'Chrome_WidgetWin_1*') { continue }
                        if ($cls -like 'CabinetWClass*') { continue }
                        if ($nm -match 'Crawler App|Business Profiles') { continue }
                        if ($nm -match '^Save As$|^Open$') { continue }
                        if ($nm -match 'User Input Required|How many records|search') {
                            $found = $w; break
                        }
                    } catch {}
                }
            } catch {}
        }
        if (-not $found) { Start-Sleep -Milliseconds 400 }
    }

    if (-not $found) {
        Write-Log "$Tag prompt not found within ${TimeoutSec}s" 'warn'
        return $false
    }

    Write-Log "$Tag dialog found: name='$($found.Current.Name)' class='$($found.Current.ClassName)'"

    # First-time debug: dump the prompt's UIA tree so we can refine on first wet run
    try {
        $debugDir = 'C:\worker\stopper\prompts'
        New-Item -ItemType Directory -Force $debugDir | Out-Null
        $dumpPath = Join-Path $debugDir ("{0}_{1}.json" -f $Tag, (Get-Date -Format 'HHmmss'))
        if (-not (Test-Path $dumpPath)) {
            $info = [ordered]@{
                tag = $Tag; value = $Value
                name = $found.Current.Name; class = $found.Current.ClassName
                children = @()
            }
            $kids = $found.FindAll(
                [System.Windows.Automation.TreeScope]::Descendants,
                [System.Windows.Automation.Condition]::TrueCondition)
            foreach ($k in $kids) {
                try {
                    $info.children += [pscustomobject]@{
                        type = $k.Current.ControlType.ProgrammaticName
                        name = $k.Current.Name
                        aid  = $k.Current.AutomationId
                        class = $k.Current.ClassName
                        enabled = $k.Current.IsEnabled
                    }
                } catch {}
            }
            $info | ConvertTo-Json -Depth 10 | Set-Content $dumpPath -Encoding UTF8
        }
    } catch {}

    # Fill the first enabled Edit
    $editCond = New-Object System.Windows.Automation.PropertyCondition(
        [System.Windows.Automation.AutomationElement]::ControlTypeProperty,
        [System.Windows.Automation.ControlType]::Edit)
    $edits = $found.FindAll([System.Windows.Automation.TreeScope]::Descendants, $editCond)
    $edit = $null
    foreach ($e in $edits) {
        try { if ($e.Current.IsEnabled) { $edit = $e; break } } catch {}
    }
    if (-not $edit) {
        Write-Log "$Tag dialog has no enabled Edit control" 'warn'
        return $false
    }
    if (-not (Set-EditValue $edit $Value)) {
        # Fallback: SendKeys (focus + select-all + type)
        try {
            $edit.SetFocus()
            Start-Sleep -Milliseconds 200
            [System.Windows.Forms.SendKeys]::SendWait('^a')
            Start-Sleep -Milliseconds 100
            [System.Windows.Forms.SendKeys]::SendWait($Value)
            Write-Log "$Tag filled via SendKeys fallback"
        } catch {
            Write-Log "$Tag couldn't set value '$Value': $($_.Exception.Message)" 'warn'
            return $false
        }
    }

    # Dismiss: try OK / Yes button, fall back to Enter
    $okBtn = Find-DialogButton -Dialog $found -NameMatch '^OK$|^Ok$|^Yes$|^Accept$|^Continue$'
    if ($okBtn -and (Invoke-Element $okBtn)) {
        Write-Log "$Tag set='$Value', dismissed via OK button"
        return $true
    }
    try {
        [System.Windows.Forms.SendKeys]::SendWait('{ENTER}')
        Write-Log "$Tag set='$Value', sent ENTER (no OK button matched)"
        return $true
    } catch {
        Write-Log "$Tag dismissal failed: $($_.Exception.Message)" 'warn'
        return $false
    }
}

# ---------- Live-log parsing ----------
function Parse-LiveLog {
    param([string]$Text)
    $out = [pscustomobject]@{
        LatestRecord  = 0
        LatestKeyword = ''
    }
    if (-not $Text) { return $out }
    $maxN = 0
    $lastKw = ''
    foreach ($m in [regex]::Matches($Text, 'Record\s*#\s*(\d+)')) {
        $n = 0
        if ([int]::TryParse($m.Groups[1].Value, [ref]$n)) {
            if ($n -gt $maxN) { $maxN = $n }
        }
    }
    $kwMatches = [regex]::Matches($Text, 'Keyword\s*:\s*([^\r\n]+)')
    if ($kwMatches.Count -gt 0) {
        $lastKw = $kwMatches[$kwMatches.Count - 1].Groups[1].Value.Trim()
    }
    $out.LatestRecord = $maxN
    $out.LatestKeyword = $lastKw
    return $out
}

# ============================================================================
# MAIN
# ============================================================================
try {

    if (-not (Load-Uia)) {
        Write-Log "cannot proceed without UIA" 'error'
        exit 0
    }

    $win = Find-BotsolWindow
    if (-not $win) {
        Write-Log "Botsol window not found; AMBIGUOUS phase, exiting" 'warn'
        exit 0
    }

    # Botsol exposes WinForms designer names as AutomationIds (stable across restarts).
    # Earlier recon using FindAll without UIA-client-level WindowsBase saw hash-based
    # numeric IDs; loading WindowsBase (done in Load-Uia) switches to the provider
    # that surfaces the designer names. These names are authoritative.
    $btnStart  = Find-ByAutomationId -Root $win -AutomationId 'btnStart'
    $btnStop   = Find-ByAutomationId -Root $win -AutomationId 'btnStop'
    $btnExport = Find-ByAutomationId -Root $win -AutomationId 'btnExport'
    $btnDelete = Find-ByAutomationId -Root $win -AutomationId 'btndelete'
    $editLog   = Find-ByAutomationId -Root $win -AutomationId 'txtProgress'

    $startEn  = Get-ButtonEnabled $btnStart
    $stopEn   = Get-ButtonEnabled $btnStop
    $exportEn = Get-ButtonEnabled $btnExport
    $deleteEn = Get-ButtonEnabled $btnDelete

    Write-Log "buttons: start=$startEn stop=$stopEn export=$exportEn delete=$deleteEn"

    $state = Read-QueueState

    # Phase classification
    $phase = 'AMBIGUOUS'
    if ($stopEn -and -not $exportEn) {
        $phase = 'RUNNING'
    } elseif ($exportEn -and $startEn -and -not $stopEn) {
        $phase = 'DONE'
    } elseif ($startEn -and -not $stopEn -and -not $exportEn) {
        $phase = 'IDLE'
    }
    Write-Log "phase=$phase"

    # DONE phase + no active queue state happens in two situations:
    #   A. Just-finished scrape that was started manually (no queue-state.json populated)
    #      -> data is still loaded -> need to click Delete first
    #   B. Post-Delete sticky-button quirk -> Botsol's UI keeps Delete/Export enabled
    #      after a successful Delete -> safe to reclassify as IDLE
    # We can't distinguish A from B cleanly, so the safe behavior is: try Delete first,
    # confirm via "Confirm Delete!!" Yes/No popup, exit; next tick will see real IDLE.
    if ($phase -eq 'DONE' -and (-not $state -or -not $state.current_run_id)) {
        if ($AUTO_DELETE_CURRENT -and $btnDelete) {
            Write-Log "DONE phase + no active run state + AUTO_DELETE_CURRENT=true -> clicking Delete first to clear stale data"
            if (Invoke-Element $btnDelete) {
                # Botsol shows a confirm popup ("Confirm Delete!!" or similar #32770) with Yes/No.
                # If the popup exists, click Yes; if not, the click was a no-op (already deleted).
                Dismiss-BoolInput -ButtonName 'Yes' -TimeoutSec 6 | Out-Null
                Write-Log "Delete clicked + Yes confirmed; exiting tick (next tick will pick clean IDLE)"
                exit 0
            } else {
                Write-Log "Delete invoke returned false; falling through" 'warn'
            }
        }
        if ($AUTO_START_NEXT) {
            Write-Log "DONE phase + no active run state + AUTO_START_NEXT=true -> reclassifying as IDLE"
            $phase = 'IDLE'
        }
    }

    switch ($phase) {

        'RUNNING' {
            $logText = Get-EditValue $editLog
            $parsed = Parse-LiveLog $logText
            Write-Log "running: latest_record=$($parsed.LatestRecord)  keyword='$($parsed.LatestKeyword)'"

            # Detect Chrome restart (planned by Botsol every N lines, OR unplanned crash auto-reloaded by watchdog).
            # Track Chrome window PIDs across ticks. PID-set change == at least one Chrome instance restarted.
            $chromePids = @()
            try {
                $rootEl = [System.Windows.Automation.AutomationElement]::RootElement
                $chCond = New-Object System.Windows.Automation.PropertyCondition(
                    [System.Windows.Automation.AutomationElement]::ClassNameProperty, 'Chrome_WidgetWin_1')
                $chWins = $rootEl.FindAll([System.Windows.Automation.TreeScope]::Children, $chCond)
                foreach ($cw in $chWins) {
                    try { $chromePids += [int]$cw.Current.ProcessId } catch {}
                }
            } catch {}
            $chromePidsStr = ($chromePids | Sort-Object) -join ','
            $lastChromePidsStr = ''
            if ($state -and $state.chrome_pids) { $lastChromePidsStr = [string]$state.chrome_pids }
            if ($lastChromePidsStr -and $lastChromePidsStr -ne $chromePidsStr) {
                $evt = [ordered]@{
                    ts            = (Get-Date).ToString('o')
                    event         = 'chrome_restart'
                    country       = $state.current_country
                    source_file   = $state.current_source_file
                    run_id        = $state.current_run_id
                    record        = $parsed.LatestRecord
                    keyword       = $parsed.LatestKeyword
                    old_pids      = $lastChromePidsStr
                    new_pids      = $chromePidsStr
                }
                $line = $evt | ConvertTo-Json -Compress
                try { Add-Content -LiteralPath 'C:\worker\logs\chrome-restarts.jsonl' -Value $line -Encoding UTF8 } catch {}
                Write-Log "chrome restart detected: old_pids=[$lastChromePidsStr] new_pids=[$chromePidsStr] @ record #$($parsed.LatestRecord)"
                Post-Slack (":arrows_counterclockwise: Chrome restarted during $($state.current_country)/$($state.current_source_file) at record #$($parsed.LatestRecord) (keyword: $($parsed.LatestKeyword))")
            }

            if ($state -and $state.current_run_id) {
                $patch = @{
                    last_keyword  = $parsed.LatestKeyword
                    results_found = $parsed.LatestRecord
                }
                Update-ScrapeJob -Id $state.current_run_id -Patch $patch | Out-Null

                # Persist updated chrome_pids back to queue-state.json
                try {
                    $newState = @{}
                    foreach ($prop in $state.PSObject.Properties) { $newState[$prop.Name] = $prop.Value }
                    $newState.chrome_pids = $chromePidsStr
                    Write-QueueState $newState
                } catch {}
            } else {
                Write-Log "RUNNING phase but no current_run_id in state file" 'warn'
            }
            exit 0
        }

        'DONE' {
            if (-not $state -or -not $state.current_country -or -not $state.current_source_file) {
                Write-Log "DONE phase but state missing; cannot compute output path safely" 'warn'
                exit 0
            }
            $country = [string]$state.current_country
            $srcFile = [string]$state.current_source_file
            $stem = [System.IO.Path]::GetFileNameWithoutExtension($srcFile)
            $stamp = Get-Date -Format 'yyyyMMdd-HHmm'
            $outName = "{0}_{1}_{2}.csv" -f $country, $stem, $stamp
            $outPath = Join-Path $OUTPUT_DIR $outName

            $logText = Get-EditValue $editLog
            $parsed = Parse-LiveLog $logText
            $latestN = $parsed.LatestRecord

            $startedAt = $null
            if ($state.started_at) { $startedAt = [string]$state.started_at }

            if ($DRY_RUN) {
                Post-Slack "[DRY_RUN] would export -> $outPath"
                if ($state.current_run_id) {
                    $patch = @{
                        status            = 'completed'
                        completed_at      = (Get-Date).ToUniversalTime().ToString("o")
                        output_csv_path   = $outPath
                        results_found     = $latestN
                    }
                    Update-ScrapeJob -Id $state.current_run_id -Patch $patch | Out-Null
                }
                Write-Log "[DRY_RUN] DONE handled without clicks; would write $outPath  records=$latestN"
                exit 0
            }

            # Wet run: click Export Data
            if (-not (Invoke-Element $btnExport)) {
                Write-Log "failed to invoke Export Data" 'error'
                if ($state.current_run_id) {
                    Update-ScrapeJob -Id $state.current_run_id -Patch @{
                        status = 'failed'
                        error  = 'invoke export failed'
                    } | Out-Null
                }
                Post-Slack ":x: Botsol export click failed for $country/$srcFile"
                exit 0
            }

            $dlg = Wait-ForDialog -TimeoutSec 10 -NameMatch 'Save'
            if (-not $dlg) {
                Write-Log "save dialog did not appear within 10s" 'error'
                if ($state.current_run_id) {
                    Update-ScrapeJob -Id $state.current_run_id -Patch @{
                        status = 'failed'
                        error  = 'save dialog timeout'
                    } | Out-Null
                }
                Post-Slack ":x: Botsol save dialog timeout for $country/$srcFile"
                exit 0
            }

            $fnEdit = Find-DialogFilenameEdit $dlg
            if (-not (Set-EditValue $fnEdit $outPath)) {
                Write-Log "could not set save dialog filename" 'error'
                if ($state.current_run_id) {
                    Update-ScrapeJob -Id $state.current_run_id -Patch @{
                        status = 'failed'
                        error  = 'set filename failed'
                    } | Out-Null
                }
                Post-Slack ":x: Botsol set filename failed for $country/$srcFile"
                exit 0
            }

            $saveBtn = Find-DialogButton -Dialog $dlg -NameMatch '^Save$|Save'
            if (-not (Invoke-Element $saveBtn)) {
                Write-Log "could not click Save" 'error'
                if ($state.current_run_id) {
                    Update-ScrapeJob -Id $state.current_run_id -Patch @{
                        status = 'failed'
                        error  = 'click save failed'
                    } | Out-Null
                }
                Post-Slack ":x: Botsol click Save failed for $country/$srcFile"
                exit 0
            }

            # Wait for "Botsol: Export Data to CSV" confirmation popup with "Data saved to..." text + OK button.
            # Without dismissing this modal, Botsol blocks the entire app and the next agent tick
            # cannot proceed. Discovered from user screenshots 2026-04-27.
            $confirmDlg = Wait-ForDialog -TimeoutSec 30 -NameMatch 'Botsol.*Export|Export Data to CSV'
            if ($confirmDlg) {
                $okBtn = Find-DialogButton -Dialog $confirmDlg -NameMatch '^OK$|^Ok$'
                if ($okBtn -and (Invoke-Element $okBtn)) {
                    Write-Log "OK confirmation popup dismissed"
                } else {
                    Write-Log "OK popup detected but couldn't click OK (continuing)" 'warn'
                }
            } else {
                Write-Log "OK confirmation popup not detected within 30s (file may already be saved)" 'warn'
            }

            # Wait for output file
            $deadline = (Get-Date).AddSeconds(30)
            $ok = $false
            while ((Get-Date) -lt $deadline) {
                if (Test-Path -LiteralPath $outPath) {
                    try {
                        $sz = (Get-Item -LiteralPath $outPath).Length
                        if ($sz -gt 1024) { $ok = $true; break }
                    } catch {}
                }
                Start-Sleep -Milliseconds 500
            }

            if (-not $ok) {
                Write-Log "output file did not appear or too small: $outPath" 'error'
                if ($state.current_run_id) {
                    Update-ScrapeJob -Id $state.current_run_id -Patch @{
                        status = 'failed'
                        error  = 'output file missing or too small'
                    } | Out-Null
                }
                Post-Slack ":x: Botsol export produced no usable file: $country/$srcFile"
                exit 0
            }

            # Success
            $duration = ''
            if ($startedAt) {
                try {
                    $st = [datetime]::Parse($startedAt)
                    $sec = [int]((Get-Date).ToUniversalTime() - $st.ToUniversalTime()).TotalSeconds
                    $duration = "$sec s"
                } catch {}
            }
            if ($state.current_run_id) {
                Update-ScrapeJob -Id $state.current_run_id -Patch @{
                    status          = 'completed'
                    completed_at    = (Get-Date).ToUniversalTime().ToString("o")
                    output_csv_path = $outPath
                    results_found   = $latestN
                } | Out-Null
            }
            Post-Slack (":white_check_mark: Botsol DONE  $country / $stem  rows=$latestN  duration=$duration")

            # Move source .txt to done/
            $countryDir = Join-Path $KEYWORDS_ROOT $country
            $doneDir = Join-Path $countryDir 'done'
            Ensure-Dir $doneDir
            $srcPath = Join-Path $countryDir $srcFile
            if (Test-Path -LiteralPath $srcPath) {
                try {
                    Move-Item -LiteralPath $srcPath -Destination (Join-Path $doneDir $srcFile) -Force
                    Write-Log "moved $srcFile -> done/"
                } catch {
                    Write-Log "failed to move source .txt: $($_.Exception.Message)" 'warn'
                }
            }

            # Click Delete Current Data to clear for next run — gated by env var.
            # AUTO_DELETE_CURRENT=false keeps the in-memory data in Botsol as a safety net
            # (user can re-export manually if CSV looks wrong). After verifying the first
            # successful export, flip AUTO_DELETE_CURRENT=true in orchestrator.env.
            if ($AUTO_DELETE_CURRENT) {
                if (-not (Invoke-Element $btnDelete)) {
                    Write-Log "Delete Current Data click failed (non-fatal)" 'warn'
                }
            } else {
                Write-Log "skipping Delete Current Data (AUTO_DELETE_CURRENT=false). Verify CSV then click Delete manually, then flip the env var."
                Post-Slack ":mag: Colombia / do exported + txt moved. Data still in Botsol memory as safety net; verify CSV then click Delete manually + set AUTO_DELETE_CURRENT=true."
            }

            Clear-QueueState
            exit 0
        }

        'IDLE' {
            if (-not $AUTO_START_NEXT) {
                Write-Log "IDLE but AUTO_START_NEXT=false; pausing (operator handoff required for Start Bot + numeric prompts flow)"
                # Post Slack once per pause — state-less dedupe: only notify if we just
                # transitioned out of a run (queue-state cleared but log shows recent RUNNING/DONE).
                exit 0
            }
            $next = Get-NextQueueItem
            if (-not $next) {
                Post-Slack ":checkered_flag: Botsol: all keyword queues empty"
                Write-Log "no more queue items"
                Clear-QueueState
                exit 0
            }
            $progress = Get-CountryProgress -Country $next.Country
            $kIndex = $progress.Done + 1
            $totalForCountry = $progress.Total
            $lineCount = Count-Lines -Path $next.FullPath

            if ($DRY_RUN) {
                Post-Slack "[DRY_RUN] would start -> $($next.Country)/$($next.FileName)"
                $row = @{
                    status          = 'queued'
                    source          = 'botsol'
                    country         = $next.Country
                    category        = $next.Stem
                    source_file     = $next.FileName
                    total_keywords  = $lineCount
                }
                $inserted = Insert-ScrapeJob -Row $row
                if ($inserted -and $inserted.id) {
                    Write-Log "[DRY_RUN] inserted scrape_jobs id=$($inserted.id)"
                }
                Write-Log "[DRY_RUN] IDLE handled without clicks; would start $($next.Country)/$($next.FileName)"
                exit 0
            }

            # Wet run
            $row = @{
                status          = 'queued'
                source          = 'botsol'
                country         = $next.Country
                category        = $next.Stem
                source_file     = $next.FileName
                total_keywords  = $lineCount
            }
            $inserted = Insert-ScrapeJob -Row $row
            $jobId = $null
            if ($inserted -and $inserted.id) { $jobId = $inserted.id }

            if (-not (Invoke-Element $btnStart)) {
                Write-Log "failed to invoke Start Bot" 'error'
                if ($jobId) {
                    Update-ScrapeJob -Id $jobId -Patch @{
                        status = 'failed'
                        error  = 'invoke start failed'
                    } | Out-Null
                }
                Post-Slack ":x: Botsol Start click failed"
                exit 0
            }

            # AFTER Start click, Botsol shows (in order):
            #   1) email-Y/N child dialog (frmBoolInput): "Do you want to scrape email and other social media..."
            #      -> answer YES (we want emails). Discovered 2026-04-27.
            #   2) numeric prompt (frmIntInput/frmInput child): "How many records you want to scrape for each search."
            #      pre-filled 400, button "Continue". -> type RESULT_CAP, click Continue.
            #   3) second numeric prompt (also child) -> type KEYWORD_LIMIT.
            #   4) top-level Open file dialog -> type path, click Open.
            #
            # The bool + numeric prompts are CHILDREN of Botsol's main window, not top-level.
            Dismiss-BoolInput -ButtonName 'Yes' -TimeoutSec 8 | Out-Null

            if (-not (Handle-NumericPrompt -Value $RESULT_CAP -TimeoutSec 12 -Tag 'prompt1_result_cap')) {
                Write-Log "first numeric prompt failed (continuing -- file-open may still appear)" 'warn'
                if ($jobId) {
                    Update-ScrapeJob -Id $jobId -Patch @{ status='failed'; error='prompt1 failed' } | Out-Null
                }
                Post-Slack ":x: Botsol prompt1 (result cap) failed for $($next.Country)/$($next.Stem)"
                exit 0
            }
            if (-not (Handle-NumericPrompt -Value $KEYWORD_LIMIT -TimeoutSec 12 -Tag 'prompt2_keyword_limit')) {
                Write-Log "second numeric prompt failed (continuing -- file-open may still appear)" 'warn'
                if ($jobId) {
                    Update-ScrapeJob -Id $jobId -Patch @{ status='failed'; error='prompt2 failed' } | Out-Null
                }
                Post-Slack ":x: Botsol prompt2 (delay) failed for $($next.Country)/$($next.Stem)"
                exit 0
            }

            # 4) Second bool prompt: "Do you want to extract only unique businesses?"
            #    Discovered 2026-04-27. Yes = smaller CSV (one row per unique CID).
            Dismiss-BoolInput -ButtonName 'Yes' -TimeoutSec 8 | Out-Null

            # 5) File-select dialog: "Botsol: Select a file" -- ANOTHER Botsol-custom child
            #    dialog (not a standard #32770 Open dialog). Pre-filled path Edit + OK button.
            #    Discovered 2026-04-27 from user screenshot. Same Edit+OK pattern as numeric
            #    prompts, so Handle-NumericPrompt handles it (matches frm* children with Edit
            #    and dismisses via OK/Continue/Yes/Accept matcher).
            if (-not (Handle-NumericPrompt -Value $next.FullPath -TimeoutSec 15 -Tag 'file_select')) {
                Write-Log "file-select dialog failed (path: $($next.FullPath))" 'error'
                if ($jobId) {
                    Update-ScrapeJob -Id $jobId -Patch @{
                        status = 'failed'
                        error  = 'file-select dialog failed'
                    } | Out-Null
                }
                Post-Slack ":x: Botsol file-select failed for $($next.Country)/$($next.Stem)"
                exit 0
            }

            $startedIso = (Get-Date).ToUniversalTime().ToString("o")
            if ($jobId) {
                Update-ScrapeJob -Id $jobId -Patch @{
                    status     = 'running'
                    started_at = $startedIso
                } | Out-Null
            }

            Post-Slack (":arrow_forward: Botsol: $($next.Country)/$($next.Stem) ($($next.Variant)) -- crawl started, $lineCount keywords, cap=$RESULT_CAP")

            $newState = @{
                current_run_id      = $jobId
                current_country     = $next.Country
                current_source_file = $next.FileName
                started_at          = $startedIso
            }
            Write-QueueState $newState
            Write-Log "started $($next.Country)/$($next.FileName)  job_id=$jobId"
            exit 0
        }

        default {
            Write-Log "AMBIGUOUS button combo (start=$startEn stop=$stopEn export=$exportEn delete=$deleteEn); no action" 'warn'
            exit 0
        }
    }

} catch {
    $msg = $_.Exception.Message
    Write-Log "unhandled error: $msg" 'error'
    try { Post-Slack ":x: botsol-agent unhandled error: $msg" } catch {}
    exit 0
}
