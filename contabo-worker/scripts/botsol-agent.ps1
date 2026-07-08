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
# When true, skip the Botsol Export+Save-As UI flow and dump the CSV directly
# from db.sqlite via the Python extractor. The Save-As dialog has been a
# source of repeated stalls (descendant #32770 with no enumerable buttons,
# 10s timeouts, etc.). SQLite snapshot is faster, deterministic, and bypasses
# UI freezes that happen when Botsol becomes "Not Responding" post-Crawler-Complete.
$EXPORT_VIA_SQLITE     = Parse-Bool $cfg['EXPORT_VIA_SQLITE'] $true
$EXTRACT_SCRIPT        = if ($cfg['EXTRACT_SCRIPT']) { $cfg['EXTRACT_SCRIPT'] } else { 'C:\worker\scripts\extract_botsol_csv.py' }

# ONLY_COUNTRIES = comma-separated allow-list (e.g. "colombia,mexico"). Empty = all.
# Useful for staged rollout: validate one country end-to-end before unleashing the
# full alphabetical sweep across ~80 countries.
$ONLY_COUNTRIES = @()
if ($cfg['ONLY_COUNTRIES']) {
    $ONLY_COUNTRIES = ($cfg['ONLY_COUNTRIES'] -split ',') |
        ForEach-Object { $_.Trim().ToLowerInvariant() } |
        Where-Object { $_ }
}
# COUNTRY_ROTATION = ordered comma-separated list (e.g. "ecuador,colombia,peru,chile,
# mexico,brazil"). When set AND ONLY_COUNTRIES empty, the agent picks files only from
# the FIRST country in this list that still has unfinished stems. Once a country
# fully exhausts (every stem appears in done/), the agent advances to the next.
# This auto-rotates without env edits - finish ecuador, drop into colombia, etc.
$COUNTRY_ROTATION = @()
if ($cfg['COUNTRY_ROTATION']) {
    $COUNTRY_ROTATION = ($cfg['COUNTRY_ROTATION'] -split ',') |
        ForEach-Object { $_.Trim().ToLowerInvariant() } |
        Where-Object { $_ }
}
$RESULT_CAP            = if ($cfg['RESULT_CAP'])    { [string]$cfg['RESULT_CAP'] }    else { '100' }
$KEYWORD_LIMIT         = if ($cfg['KEYWORD_LIMIT']) { [string]$cfg['KEYWORD_LIMIT'] } else { '5' }

# Frozen-run self-heal thresholds (ticks; agent ticks every ~2 min).
# A RUNNING-but-frozen scrape (record counter not advancing) is invisible to the
# phase machine, so it used to sit dead for days (2026-06-05..08: 3.5 days at
# record #229). STALL_SOFT_TICKS no-progress ticks -> click Stop (salvage partial
# rows via the normal DONE->extract path). STALL_HARD_TICKS more with still no
# progress -> kill BotsolApp + cold-start chain. Defaults: soft ~30min, hard ~16min later.
$STALL_SOFT_TICKS      = if ($cfg['STALL_SOFT_TICKS']) { [int]$cfg['STALL_SOFT_TICKS'] } else { 15 }
$STALL_HARD_TICKS      = if ($cfg['STALL_HARD_TICKS']) { [int]$cfg['STALL_HARD_TICKS'] } else { 8 }

Write-Log "tick start  dry_run=$DRY_RUN  auto_start_next=$AUTO_START_NEXT  auto_delete_current=$AUTO_DELETE_CURRENT  use_starter=$USE_STARTER  use_pruned=$USE_PRUNED  result_cap=$RESULT_CAP  keyword_limit=$KEYWORD_LIMIT  export_via_sqlite=$EXPORT_VIA_SQLITE"

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
        # Surface the PostgREST error body too - message alone hides constraint
        # violations (a 23514 went undiagnosed for 9 days on message-only logging).
        $errMsg = $_.Exception.Message
        $respBody = ''
        try { $sr = New-Object IO.StreamReader($_.Exception.Response.GetResponseStream()); $respBody = $sr.ReadToEnd() } catch {}
        Write-Log "Insert-ScrapeJob failed: $errMsg body=$respBody" 'error'
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
        # Surface the PostgREST error body too - message alone hides constraint
        # violations (a 23514 went undiagnosed for 9 days on message-only logging).
        $errMsg = $_.Exception.Message
        $respBody = ''
        try { $sr = New-Object IO.StreamReader($_.Exception.Response.GetResponseStream()); $respBody = $sr.ReadToEnd() } catch {}
        Write-Log "Update-ScrapeJob($Id) failed: $errMsg body=$respBody" 'error'
        return $null
    }
}

function Process-RetryRequests {
    # Honor "Retry" clicks from the dashboard. Dashboard PATCHes scrape_jobs to
    # status='queued' for the failed job. Each tick we scan for botsol-source
    # rows in queued state, restore the source .txt from done/<file>.txt back to
    # <country>/<file>.txt (if present), then mark the row as 'retry_pending'
    # so the agent's normal IDLE flow can pick the file. Idempotent: re-running
    # against an already-restored file is a no-op.
    if (-not $KOLLABLY_URL -or -not $KOLLABLY_KEY) { return }
    try {
        $uri = "$KOLLABLY_URL/rest/v1/scrape_jobs?source=eq.botsol&status=eq.queued&select=id,country,source_file"
        $rows = Invoke-RestMethod -Uri $uri -Headers (Get-SupaHeaders) -TimeoutSec 15
        if (-not $rows -or $rows.Count -eq 0) { return }
        Write-Log "retry: $($rows.Count) queued botsol jobs to process"
        # Guard rails (2026-07-08 audit):
        #  - never restore the file of the CURRENTLY ACTIVE run (restore-during-run race)
        #  - collapse duplicate country+source_file rows to one per tick (first row wins)
        $qsRetry = Read-QueueState
        $activeCountry = ''
        $activeFile    = ''
        if ($qsRetry) {
            if ($qsRetry.current_country)     { $activeCountry = [string]$qsRetry.current_country }
            if ($qsRetry.current_source_file) { $activeFile    = [string]$qsRetry.current_source_file }
        }
        $seenRetry = @{}
        foreach ($r in $rows) {
            $jid = $r.id
            $country = $r.country
            $srcFile = $r.source_file
            if (-not $country -or -not $srcFile) {
                Write-Log "retry: row $jid missing country/source_file, marking failed" 'warn'
                Update-ScrapeJob -Id $jid -Patch @{ status='failed'; error='retry rejected: missing country/source_file' } | Out-Null
                continue
            }
            $comboKey = "$country|$srcFile"
            if ($seenRetry.ContainsKey($comboKey)) {
                Write-Log "retry: duplicate row $jid for $country/$srcFile - collapsing (first row wins)" 'warn'
                Update-ScrapeJob -Id $jid -Patch @{ status='failed'; error='duplicate retry collapsed' } | Out-Null
                continue
            }
            $seenRetry[$comboKey] = $true
            if ($activeCountry -and $activeFile -and $country -eq $activeCountry -and $srcFile -eq $activeFile) {
                Write-Log "retry: $country/$srcFile is the ACTIVE run - skipping restore this tick (row stays queued)" 'warn'
                continue
            }
            $countryDir = Join-Path $KEYWORDS_ROOT $country
            $doneDir = Join-Path $countryDir 'done'
            $livePath = Join-Path $countryDir $srcFile
            $donePath = Join-Path $doneDir $srcFile
            if (Test-Path -LiteralPath $livePath) {
                Write-Log "retry: $country/$srcFile already in country dir, leaving in place"
            } elseif (Test-Path -LiteralPath $donePath) {
                try {
                    Move-Item -LiteralPath $donePath -Destination $livePath -Force
                    Write-Log "retry: moved $country/done/$srcFile -> $country/$srcFile"
                } catch {
                    Write-Log "retry: failed to restore $country/$srcFile : $($_.Exception.Message)" 'error'
                    Update-ScrapeJob -Id $jid -Patch @{ status='failed'; error="retry restore failed: $($_.Exception.Message)" } | Out-Null
                    continue
                }
            } else {
                Write-Log "retry: $country/$srcFile not found in dir or done/ - cannot retry" 'warn'
                Update-ScrapeJob -Id $jid -Patch @{ status='failed'; error='retry rejected: source file missing' } | Out-Null
                continue
            }
            # Mark as retry_pending - purely informational. Agent IDLE flow will
            # create a NEW scrape_jobs row when it picks up the file. The old row
            # stays as 'retry_pending' for audit. (We don't delete it.)
            # Capture the PATCH result: Update-ScrapeJob returns $null on failure
            # (e.g. status check constraint rejects 'retry_pending'). Don't log
            # success unconditionally; fall back to 'failed' so the row isn't
            # re-picked as queued every tick forever.
            $upd = Update-ScrapeJob -Id $jid -Patch @{ status='retry_pending' }
            if ($null -eq $upd) {
                Write-Log "retry: retry_pending PATCH rejected for $jid ($country/$srcFile) - marking failed instead" 'warn'
                Update-ScrapeJob -Id $jid -Patch @{ status='failed'; error='retry consumed (retry_pending rejected)' } | Out-Null
                continue
            }
            Write-Log ":arrows_counterclockwise: Botsol retry queued for $country / $srcFile (file restored from done/)"
        }
    } catch {
        Write-Log "Process-RetryRequests error: $($_.Exception.Message)" 'warn'
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


function Abandon-CurrentRun {
    param(
        [string]$country,
        [string]$srcFile,
        [string]$reason = ''
    )
    try {
        if ($country -and $srcFile) {
            $countryDir = Join-Path $KEYWORDS_ROOT $country
            $skipDir    = Join-Path $countryDir 'skipped'
            Ensure-Dir $skipDir
            $srcPath = Join-Path $countryDir $srcFile
            if (Test-Path -LiteralPath $srcPath) {
                Move-Item -LiteralPath $srcPath -Destination (Join-Path $skipDir $srcFile) -Force
                Write-Log "Abandon: moved $country/$srcFile -> skipped/ (reason: $reason)" 'warn'
            } else {
                Write-Log "Abandon: source $country/$srcFile not found; clearing state anyway (reason: $reason)" 'warn'
            }
        }
    } catch {
        Write-Log "Abandon: failed to move skip file: $($_.Exception.Message)" 'warn'
    }
    Clear-QueueState
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
    } elseif ($COUNTRY_ROTATION.Count -gt 0) {
        # Re-order country list to follow rotation; drop countries not listed.
        # The Get-NextQueueItem loop already short-circuits on first country with
        # unfinished stems, so just changing the order achieves auto-advance:
        # ecuador exhausts -> first hit becomes colombia -> etc.
        $byName = @{}
        foreach ($c in $countries) { $byName[$c.Name.ToLowerInvariant()] = $c }
        $ordered = @()
        foreach ($n in $COUNTRY_ROTATION) {
            if ($byName.ContainsKey($n)) { $ordered += $byName[$n] }
        }
        if (-not $ordered) {
            Write-Log "COUNTRY_ROTATION=$($COUNTRY_ROTATION -join ',') matched zero country dirs in $KEYWORDS_ROOT" 'warn'
            return $null
        }
        $countries = $ordered
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

        # Compute exact filenames already in done/ (per-variant, not per-stem)
        # so that pruned round-2 still runs after its sibling starter is done.
        $doneNames = @{}
        if (Test-Path -LiteralPath $doneDir) {
            foreach ($df in (Get-ChildItem -LiteralPath $doneDir -File -Filter '*.txt' -ErrorAction SilentlyContinue)) {
                $doneNames[$df.Name] = $true
            }
        }

        foreach ($stem in ($byStem.Keys | Sort-Object)) {
            $variants = $byStem[$stem]
            # Drop variants that are already in done/ by exact filename match
            $eligible = @{}
            foreach ($vname in $variants.Keys) {
                $vFile = $variants[$vname]
                if (-not $doneNames.ContainsKey($vFile.Name)) {
                    $eligible[$vname] = $vFile
                }
            }
            if ($eligible.Count -eq 0) { continue }

            $pick = $null
            $variantUsed = $null
            if ($USE_STARTER -and $eligible.ContainsKey('starter')) {
                $pick = $eligible['starter']; $variantUsed = 'starter'
            } elseif ($USE_PRUNED -and $eligible.ContainsKey('pruned')) {
                $pick = $eligible['pruned']; $variantUsed = 'pruned'
            } elseif ($eligible.ContainsKey('original')) {
                $pick = $eligible['original']; $variantUsed = 'original'
            } elseif ($eligible.ContainsKey('pruned')) {
                $pick = $eligible['pruned']; $variantUsed = 'pruned-fallback'
            } elseif ($eligible.ContainsKey('starter')) {
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

function Dismiss-BotsolModalPopup {
    # Crawler Complete + similar standard MessageBox popups can mount in TWO places:
    #   A. Top-level #32770 window owned by BotsolApp PID (separate desktop window)
    #   B. CHILD of BotForm with class #32770 (in-process modal hosted as child)
    # Discovered B by scanning live state 2026-05-08 - Crawler Complete sat as child of
    # BotForm for hours while top-level scan returned nothing, blocking all 4 main
    # buttons via the modal disable. This scans both scopes and dismisses any benign
    # popup (Crawler Complete / generic Notice / Information / Warning) via OK button.
    # ERROR popups stay on the dedicated ERROR handler below (different match logic +
    # Slack warning emoji), so we explicitly skip them here.
    param(
        [int]$BotsolPid,
        [System.Windows.Automation.AutomationElement]$BotForm
    )
    if (-not $BotsolPid) { return $false }
    $dismissed = $false

    $candidates = @()
    try {
        $root = [System.Windows.Automation.AutomationElement]::RootElement
        $topWins = $root.FindAll(
            [System.Windows.Automation.TreeScope]::Children,
            [System.Windows.Automation.Condition]::TrueCondition)
        foreach ($w in $topWins) {
            try {
                if ($w.Current.ProcessId -eq $BotsolPid -and $w.Current.ClassName -eq '#32770') {
                    $candidates += [pscustomobject]@{ Element = $w; Scope = 'top-level' }
                }
            } catch {}
        }
    } catch {}

    if ($BotForm) {
        try {
            $kids = $BotForm.FindAll(
                [System.Windows.Automation.TreeScope]::Children,
                [System.Windows.Automation.Condition]::TrueCondition)
            foreach ($k in $kids) {
                try {
                    if ($k.Current.ClassName -eq '#32770') {
                        $candidates += [pscustomobject]@{ Element = $k; Scope = 'botform-child' }
                    }
                } catch {}
            }
        } catch {}
    }

    foreach ($cand in $candidates) {
        $w = $cand.Element
        $popupName = $null
        try { $popupName = $w.Current.Name } catch {}
        if (-not $popupName) { $popupName = '<noname>' }

        # Skip ERROR popups - handled separately to surface a warning emoji + retry path
        if ($popupName -match 'ERROR') { continue }

        Write-Log "$($cand.Scope) popup detected: name='$popupName' class=#32770 pid=$BotsolPid"
        $btnCond = New-Object System.Windows.Automation.PropertyCondition(
            [System.Windows.Automation.AutomationElement]::ControlTypeProperty,
            [System.Windows.Automation.ControlType]::Button)
        $btns = $w.FindAll([System.Windows.Automation.TreeScope]::Descendants, $btnCond)
        $clicked = $false
        foreach ($b in $btns) {
            try {
                $bn = $b.Current.Name
                $en = $b.Current.IsEnabled
                if ($en -and $bn -match '^&?(OK|Yes|Close)$') {
                    $pat = $b.GetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern)
                    $pat.Invoke()
                    Write-Log "dismissed $($cand.Scope) '$popupName' via button '$bn'"
                    Write-Log ":white_check_mark: Botsol popup '$popupName' auto-dismissed ($($cand.Scope))"
                    $clicked = $true
                    $dismissed = $true
                    break
                }
            } catch {}
        }
        if (-not $clicked) {
            Write-Log "$($cand.Scope) popup '$popupName' had no clickable OK/Yes/Close button (btnCount=$($btns.Count))" 'warn'
        }
    }

    return $dismissed
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
    # PropertyCondition on AutomationIdProperty is flaky for these WinForms controls -
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
                        # FIX 2026-07-08 (45.5h outage): a frmBoolInput Yes/No question can mount at
                        # ANY point in the Start sequence (newer Botsol builds reorder the email /
                        # unique-businesses questions). While it is up, the NEXT prompt never mounts,
                        # so the plain-IDLE flow died at file_select after 45s -- the DONE->IDLE
                        # recovery path only worked because its frmBoolInput happened to land inside
                        # the dedicated 8s Dismiss-BoolInput window. Dismiss it inline (Yes) here so
                        # EVERY Handle-NumericPrompt wait self-clears a blocking bool; absent on
                        # older Botsol versions = no-op (this branch simply never matches).
                        if ($en -and ($aid -eq 'frmBoolInput' -or $aid -like 'frmBool*')) {
                            $bBtn = Find-DialogButton -Dialog $k -NameMatch '^&?Yes$'
                            if ($bBtn -and (Invoke-Element $bBtn)) {
                                Write-Log "$Tag BoolInput child dialog (aid=$aid) dismissed via 'Yes' while waiting for prompt"
                                Start-Sleep -Milliseconds 500
                            }
                        }
                        # Match any frm* child (frmInput, frmIntInput, frmSelectFile, frmFileInput, ...)
                        # OR Botsol's actual InputInteger / InputString AIDs (observed live 2026-05-03).
                        # The Edit-presence check below filters out frmBoolInput (no Edit, just Yes/No).
                        if ($en -and (($aid -like 'frm*' -and $aid -ne 'frmBoolInput') -or $aid -eq 'InputInteger' -or $aid -eq 'InputString' -or $aid -eq 'InputFile')) {
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

    # Some Botsol prompts gate the input Edit behind a "Limited to" radio (e.g. the
    # "How many businesses..." result-cap dialog: txtInput stays disabled until the
    # rbLimit radio is selected). Select it first so the Edit becomes enabled,
    # otherwise we'd bail below with "no enabled Edit control".
    try {
        $radioCond = New-Object System.Windows.Automation.PropertyCondition(
            [System.Windows.Automation.AutomationElement]::ControlTypeProperty,
            [System.Windows.Automation.ControlType]::RadioButton)
        foreach ($rb in $found.FindAll([System.Windows.Automation.TreeScope]::Descendants, $radioCond)) {
            $raid = [string]$rb.Current.AutomationId; $rnm = [string]$rb.Current.Name
            if ($raid -eq 'rbLimit' -or $rnm -match 'Limited') {
                try {
                    $sip = $rb.GetCurrentPattern([System.Windows.Automation.SelectionItemPattern]::Pattern)
                    if (-not $sip.Current.IsSelected) {
                        $sip.Select()
                        Write-Log "$Tag selected '$rnm' radio to enable input edit"
                        Start-Sleep -Milliseconds 400
                    } else {
                        # Log the already-selected case too (2026-07-08): the failing plain-IDLE
                        # path never showed the radio line, which masked whether the radio dialog
                        # was present at all. This makes the two states distinguishable in the log.
                        Write-Log "$Tag '$rnm' radio already selected"
                    }
                } catch {
                    try { $rb.GetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern).Invoke(); Start-Sleep -Milliseconds 400 } catch {}
                }
                break
            }
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

# ----------------------------------------------------------------------------
# Frozen-run detection (pure decision fn). Unit-tested via _test_stall.ps1.
# Returns @{ Action='reset'|'count'|'soft'|'hard'; StallTicks=int; SoftAttempted=bool }.
# 'soft' at SoftThreshold consecutive no-progress ticks; 'hard' SoftThreshold+HardThreshold.
# Single-fire of 'hard' is the caller's responsibility (kill + Clear-QueueState removes
# the RUNNING phase next tick, so this fn is never re-entered for the same dead run).
# ----------------------------------------------------------------------------
function Get-StallDecision {
    param(
        [int]$CurrentRecord,
        [int]$LastRecord,
        [int]$StallTicks,
        [bool]$SoftAttempted,
        [int]$SoftThreshold,
        [int]$HardThreshold
    )
    if ($CurrentRecord -gt $LastRecord) {
        return @{ Action = 'reset'; StallTicks = 0; SoftAttempted = $false }
    }
    $newTicks = $StallTicks + 1
    if (-not $SoftAttempted) {
        if ($newTicks -ge $SoftThreshold) {
            return @{ Action = 'soft'; StallTicks = $newTicks; SoftAttempted = $true }
        }
        return @{ Action = 'count'; StallTicks = $newTicks; SoftAttempted = $false }
    }
    if ($newTicks -ge ($SoftThreshold + $HardThreshold)) {
        return @{ Action = 'hard'; StallTicks = $newTicks; SoftAttempted = $true }
    }
    return @{ Action = 'count'; StallTicks = $newTicks; SoftAttempted = $true }
}

# Hard recovery: NON-DESTRUCTIVE. We kill NOTHING.
#  - Killing BotsolApp -> 'Select Bot' chooser strand (banned by user 2026-06-12).
#  - Killing its embedded Chrome mid-crawl is ALSO harmful: it leaves BotsolApp in a
#    "phantom-RUNNING" state (Stop stays enabled, no browser, Stop-click can't clear it),
#    which loops forever and never advances (cost: ~6.5h wedged on mexico/stay 2026-06-13).
#    Reviewing the logs, the Chrome-kill never once produced a recovery.
# So hard recovery just re-clicks Stop (the caller does this) and lets BotsolApp end its
# OWN crawl gracefully with the browser still alive. A genuinely hung crawl is left for
# the watchdog's throttled human-nudge alert; we never wedge it ourselves.
function Invoke-BotsolHardRestart {
    Write-Log "hard-recovery(non-destructive): no kill; relying on Stop + watchdog human-nudge alert"
}

# Extract-before-delete safety net. Dumps whatever rows are currently loaded in Botsol's
# grid (db.sqlite active table) into a CSV that csv_processor will ingest (dedup-safe by
# google_maps_url), BEFORE the grid is ever cleared. This is the guarantee that an
# interrupted/orphaned category is NEVER lost on a Delete. Returns:
#   'captured' - rows written and will be ingested
#   'empty'    - table had no rows (nothing to lose)
#   'error'    - extractor failed unexpectedly; caller MUST NOT delete (preserve + retry)
function Invoke-SafeExtract {
    param([string]$OutPath, [string]$Tag)
    if (-not (Test-Path $EXTRACT_SCRIPT)) { Write-Log "safe-extract: extract script missing $EXTRACT_SCRIPT" 'error'; return 'error' }
    Ensure-Dir (Split-Path $OutPath -Parent)
    $log = Join-Path 'C:\worker\logs' "extract-$Tag.log"
    try {
        $proc = Start-Process -FilePath 'python' -ArgumentList @('-u', $EXTRACT_SCRIPT, $OutPath) `
            -NoNewWindow -Wait -PassThru -RedirectStandardOutput $log -RedirectStandardError "$log.err"
    } catch { Write-Log "safe-extract: launch failed: $($_.Exception.Message)" 'error'; return 'error' }
    $out = ''
    try { $out = Get-Content $log -Raw -EA SilentlyContinue } catch {}
    if ($out -match 'Wrote\s+\d+\s+rows')          { Write-Log "safe-extract: captured -> $OutPath"; return 'captured' }
    if ($out -match 'No data table with rows found') { return 'empty' }
    Write-Log "safe-extract: unrecognized result (exit=$($proc.ExitCode)); see $log" 'warn'
    return 'error'
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

    # Honor any "Retry" clicks from the dashboard (status='queued' rows in
    # scrape_jobs). Restores source files from done/ and marks rows as
    # retry_pending. Idempotent + safe to run every tick.
    Process-RetryRequests

    # Clear any Crawler Complete / standard MessageBox popups (top-level OR child of
    # BotForm) that mask DONE phase. Done before phase detection so buttons reflect
    # the real state.
    try {
        $botPid = $win.Current.ProcessId
        if (Dismiss-BotsolModalPopup -BotsolPid $botPid -BotForm $win) {
            Start-Sleep -Milliseconds 1200
            $script:BotsolChildrenCache = $null
        }
    } catch {}

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
    # Stop button TEXT: "Stop Bot" while crawling, "Stopping...." once a Stop is in progress.
    # A stuck "Stopping...." == the dead-Chrome deadlock (BotsolApp can't finish stopping a
    # browser that already died), so re-clicking Stop is pointless + only spams the UI.
    $stopName = if ($btnStop) { try { [string]$btnStop.Current.Name } catch { '' } } else { '' }

    Write-Log "buttons: start=$startEn stop=$stopEn export=$exportEn delete=$deleteEn stopName='$stopName'"

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

    # Auto-dismiss any "ERROR! Please contact botsol support" #32770 popup that
    # leaks from a botched Start/Delete sequence. It's a plain Windows MessageBox
    # nested in BotForm with text containing 'ERROR' and an OK button.
    try {
        $errCond = New-Object System.Windows.Automation.PropertyCondition(
            [System.Windows.Automation.AutomationElement]::ClassNameProperty, '#32770')
        $errDialogs = $win.FindAll([System.Windows.Automation.TreeScope]::Descendants, $errCond)
        foreach ($d in $errDialogs) {
            try {
                $textCond = New-Object System.Windows.Automation.PropertyCondition(
                    [System.Windows.Automation.AutomationElement]::ControlTypeProperty,
                    [System.Windows.Automation.ControlType]::Text)
                $texts = $d.FindAll([System.Windows.Automation.TreeScope]::Descendants, $textCond)
                $isError = $false
                foreach ($t in $texts) {
                    try { if ($t.Current.Name -match 'ERROR|contact botsol') { $isError = $true; break } } catch {}
                }
                if ($isError) {
                    $okBtn = Find-DialogButton -Dialog $d -NameMatch '^&?OK$'
                    if ($okBtn -and (Invoke-Element $okBtn)) {
                        Write-Log "Auto-dismissed Botsol ERROR popup via OK"
                        Write-Log ":warning: Botsol error popup auto-dismissed (will retry on next tick)"
                        exit 0
                    }
                }
            } catch {}
        }
    } catch {}

    # DONE phase + no active queue state happens in two situations:
    #   A. Just-finished scrape that was started manually (no queue-state.json populated)
    #      -> data is still loaded -> need to click Delete first
    #   B. Post-Delete sticky-button quirk -> Botsol's UI keeps Delete/Export enabled
    #      after a successful Delete -> safe to reclassify as IDLE
    # Strategy: try Delete once and write a marker file with timestamp. If marker exists
    # and is < 5 min old, assume sticky quirk (case B) and reclassify as IDLE. Marker is
    # cleared once a new run starts (queue state populated).
    $deleteMarker = 'C:\worker\botsol-delete-attempted.stamp'
    if ($phase -eq 'DONE' -and (-not $state -or -not $state.current_run_id)) {
        $recentDelete = $false
        if (Test-Path $deleteMarker) {
            try {
                $age = (Get-Date) - (Get-Item $deleteMarker).LastWriteTime
                if ($age.TotalMinutes -lt 5) { $recentDelete = $true }
            } catch {}
        }
        if ($recentDelete) {
            Write-Log "DONE phase + recent delete marker -> sticky-button quirk; reclassifying as IDLE"
            $phase = 'IDLE'
        } elseif ($AUTO_DELETE_CURRENT -and $btnDelete) {
            # EXTRACT-BEFORE-DELETE GUARD: this branch used to clear "stale" data outright,
            # which is the ONE place leads were ever lost (an interrupted/orphaned category
            # leaves its rows loaded here). Never clear without first capturing them.
            $recCountry = if ($state -and $state.current_country)     { [string]$state.current_country } else { 'unknown' }
            $recStem    = if ($state -and $state.current_source_file) { [System.IO.Path]::GetFileNameWithoutExtension([string]$state.current_source_file) } else { 'orphan' }
            $recStamp   = Get-Date -Format 'yyyyMMdd-HHmm'
            $recPath    = Join-Path $OUTPUT_DIR ("{0}_{1}_recovered_{2}.csv" -f $recCountry, $recStem, $recStamp)
            $safe = Invoke-SafeExtract -OutPath $recPath -Tag "predelete-$recCountry-$recStem-$recStamp"
            if ($safe -eq 'error') {
                Write-Log "DONE+no-run-state: extract-before-delete FAILED -> NOT clicking Delete (preserving loaded rows); will retry next tick" 'warn'
                Post-Slack ":warning: Botsol: couldn't safely extract loaded rows before clearing the grid - preserving data, retrying next tick."
                exit 0
            }
            Write-Log "DONE phase + no active run state -> extract-before-delete ($safe) done; clicking Delete to clear stale data"
            if (Invoke-Element $btnDelete) {
                Dismiss-BoolInput -ButtonName 'Yes' -TimeoutSec 6 | Out-Null
                try { Set-Content -Path $deleteMarker -Value (Get-Date -Format o) -Encoding UTF8 } catch {}
                Write-Log "Delete clicked + Yes confirmed; marker written; exiting tick"
                exit 0
            } else {
                Write-Log "Delete invoke returned false; writing marker anyway to escape stuck loop (self-heal)" 'warn'
              try { Set-Content -Path $deleteMarker -Value (Get-Date -Format o) -Encoding UTF8 } catch {}
            }
        } elseif ($AUTO_START_NEXT) {
            Write-Log "DONE phase + no active run state + AUTO_START_NEXT=true -> reclassifying as IDLE"
            $phase = 'IDLE'
        }
    }

    switch ($phase) {

        'RUNNING' {
            $logText = Get-EditValue $editLog
            $parsed = Parse-LiveLog $logText
            Write-Log "running: latest_record=$($parsed.LatestRecord)  keyword='$($parsed.LatestKeyword)'"

            # --- "Stopping..." deadlock guard (root cause of the daily hang, 2026-06-15) ----------
            # When BotsolApp's scraper Chrome dies mid-crawl, a Stop never completes: the Stop
            # button text sticks at "Stopping...." forever. Re-clicking it does nothing but append
            # "Stopping the bot, please wait.." to the progress box (the spam the user saw on screen)
            # and can never recover. So: do NOT click Stop. Salvage the loaded rows ONCE (db.sqlite
            # survives; csv_processor dedups), alert ONCE for a manual restart, then hold quietly.
            if ($stopName -match 'Stopping') {
                $sticks = 0; try { $sticks = [int]$state.stopping_ticks } catch {}
                $sticks++
                if ($sticks -eq 1) {
                    $sc = if ($state -and $state.current_country)     { [string]$state.current_country } else { 'unknown' }
                    $sf = if ($state -and $state.current_source_file) { [System.IO.Path]::GetFileNameWithoutExtension([string]$state.current_source_file) } else { 'frozen' }
                    $sp = Join-Path $OUTPUT_DIR ("{0}_{1}_salvage_{2}.csv" -f $sc, $sf, (Get-Date -Format 'yyyyMMdd-HHmm'))
                    $sr = Invoke-SafeExtract -OutPath $sp -Tag "salvage-$sc-$sf"
                    Write-Log "Stopping-deadlock detected -> salvaged loaded rows ($sr); will NOT re-click Stop (avoids the 'Stopping...' spam)" 'warn'
                } else {
                    Write-Log "Stopping-deadlock: tick $sticks, holding (no Stop click)"
                }
                $alerted = $false; try { $alerted = [bool]$state.stopping_alerted } catch {}
                if ($sticks -ge 5 -and -not $alerted) {
                    Post-Slack (":rotating_light: *BOTSOL STUCK - MANUAL RESTART NEEDED* on $env:COMPUTERNAME. ``$($state.current_country)/$($state.current_source_file)`` hit the BotsolApp ``Stopping...`` deadlock (its scraper Chrome died; the Stop can't complete). Loaded rows already salvaged + ingested. :point_right: RDP into 66.70.134.73 -> relaunch BotsolApp -> click the bot + *Select*.")
                    $alerted = $true
                }
                if ($state) {
                    $state | Add-Member -NotePropertyName stopping_ticks   -NotePropertyValue $sticks  -Force
                    $state | Add-Member -NotePropertyName stopping_alerted -NotePropertyValue $alerted -Force
                    try { Write-QueueState $state } catch {}
                }
                exit 0
            }
            # ------------------------------------------------------------------------------------

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
                Write-Log (":arrows_counterclockwise: Chrome restarted during $($state.current_country)/$($state.current_source_file) at record #$($parsed.LatestRecord) (keyword: $($parsed.LatestKeyword))")
            }

            if ($state -and $state.current_run_id) {
                $patch = @{
                    last_keyword  = $parsed.LatestKeyword
                    results_found = $parsed.LatestRecord
                }
                Update-ScrapeJob -Id $state.current_run_id -Patch $patch | Out-Null

                # ---- Frozen-run detection / self-heal (see Get-StallDecision) ----
                $curRec   = [int]$parsed.LatestRecord
                $prevRec  = if ($null -ne $state.stall_last_record)     { [int]$state.stall_last_record }      else { 0 }
                $prevTk   = if ($null -ne $state.stall_ticks)           { [int]$state.stall_ticks }            else { 0 }
                $prevSoft = if ($null -ne $state.stall_soft_attempted)  { [bool]$state.stall_soft_attempted }  else { $false }
                $decision = Get-StallDecision -CurrentRecord $curRec -LastRecord $prevRec -StallTicks $prevTk -SoftAttempted $prevSoft -SoftThreshold $STALL_SOFT_TICKS -HardThreshold $STALL_HARD_TICKS

                # Persist chrome_pids + stall tracking back to queue-state.json
                try {
                    $newState = @{}
                    foreach ($prop in $state.PSObject.Properties) { $newState[$prop.Name] = $prop.Value }
                    $newState.chrome_pids = $chromePidsStr
                    if ($decision.Action -eq 'reset') {
                        $newState.stall_last_record      = $curRec
                        $newState.stall_last_progress_at = (Get-Date).ToString('o')
                    } else {
                        $newState.stall_last_record = [Math]::Max($curRec, $prevRec)
                        if (-not $state.stall_last_progress_at) { $newState.stall_last_progress_at = (Get-Date).ToString('o') }
                    }
                    $newState.stall_ticks          = $decision.StallTicks
                    $newState.stall_soft_attempted = $decision.SoftAttempted
                    Write-QueueState $newState
                } catch {}

                # NEVER click Stop during a scrape (user directive 2026-06-15). The agent only
                # acts on real completion (Crawler Complete -> DONE -> export). A frozen record means
                # BotsolApp's scraper Chrome died; clicking Stop only deadlocks it + spams "Stopping..".
                # So: at 'soft' (~30 min frozen) we just watch (let Botsol finish); at 'hard' (~46 min
                # frozen = truly hung) we salvage the loaded rows ONCE (db.sqlite -> CSV -> csv_processor,
                # dedup-safe), alert ONCE for a manual restart, leave the category file PENDING so it
                # re-scrapes after restart, then hold. No Stop, ever.
                if ($decision.Action -eq 'soft') {
                    $mins = [int]($decision.StallTicks * 2)
                    Write-Log "STALL: record frozen ~$mins min at #$curRec on $($state.current_country)/$($state.current_source_file); watching (letting Botsol finish, NO Stop)" 'warn'
                }
                elseif ($decision.Action -eq 'hard') {
                    $mins = [int]($decision.StallTicks * 2)
                    $handled = $false; try { $handled = [bool]$state.hang_handled } catch {}
                    if (-not $handled) {
                        $hc = [string]$state.current_country
                        $hs = [System.IO.Path]::GetFileNameWithoutExtension([string]$state.current_source_file)
                        Write-Log "HUNG: record frozen ~$mins min on $hc/$($state.current_source_file) (scraper Chrome died). NOT clicking Stop. Salvaging once + alerting; category stays PENDING for re-scrape." 'warn'
                        $hp = Join-Path $OUTPUT_DIR ("{0}_{1}_salvage_{2}.csv" -f $hc, $hs, (Get-Date -Format 'yyyyMMdd-HHmm'))
                        $hr = Invoke-SafeExtract -OutPath $hp -Tag "hung-$hc-$hs"
                        Write-Log "HUNG salvage: $hr -> $hp"
                        # Attempt cap: a chronic-hanger (e.g. a captcha-walled keyword) must not block
                        # the queue forever. Track hangs per category in a sidecar; after 3, move it to
                        # skipped/ (partial already salvaged) so the rotation progresses past it.
                        $hkey = "$hc/$hs"; $haFile = 'C:\worker\hang-attempts.json'; $ha = @{}
                        try { if (Test-Path $haFile) { (Get-Content $haFile -Raw | ConvertFrom-Json).PSObject.Properties | ForEach-Object { $ha[$_.Name] = [int]$_.Value } } } catch {}
                        $att = 0; if ($ha.ContainsKey($hkey)) { $att = [int]$ha[$hkey] }
                        $att++; $ha[$hkey] = $att
                        try { ($ha | ConvertTo-Json -Compress) | Set-Content $haFile -Encoding UTF8 } catch {}
                        if ($att -ge 3) {
                            try {
                                $cd = Join-Path $KEYWORDS_ROOT $hc; $sk = Join-Path $cd 'skipped'; Ensure-Dir $sk
                                $src = Join-Path $cd ([string]$state.current_source_file)
                                if (Test-Path $src) { Move-Item -LiteralPath $src -Destination (Join-Path $sk ([string]$state.current_source_file)) -Force }
                            } catch {}
                            Write-Log "HUNG: $hkey hung $att times -> moved to skipped/ (partial salvaged); queue will progress past it" 'warn'
                            Post-Slack (":no_entry: *Botsol gave up on* ``$hkey`` after $att hangs (partial leads salvaged + ingested). Moved to skipped so the queue continues. :point_right: restart BotsolApp -> click bot + Select to resume with the NEXT category.")
                        } else {
                            Write-Log "HUNG: $hkey attempt $att of 3 -> left PENDING for re-scrape after restart"
                            Post-Slack (":rotating_light: *BOTSOL STUCK - MANUAL RESTART NEEDED* on $env:COMPUTERNAME. ``$hkey`` froze (~$mins min; scraper Chrome died) - attempt $att of 3. Loaded rows salvaged; it re-scrapes after restart. :point_right: RDP into 66.70.134.73 -> relaunch BotsolApp -> click bot + Select. (Agent will NOT touch Stop.)")
                        }
                        try {
                            $st2 = @{}; foreach ($p in $state.PSObject.Properties) { $st2[$p.Name] = $p.Value }
                            $st2.hang_handled = $true
                            Write-QueueState $st2
                        } catch {}
                    } else {
                        Write-Log "HUNG: still frozen ~$mins min on $($state.current_country)/$($state.current_source_file); holding (already salvaged + alerted, no Stop)"
                    }
                }
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

            if ($EXPORT_VIA_SQLITE) {
                # SQLite-direct path: bypass the Botsol Export UI entirely.
                # Botsol's UI is single-threaded and frequently freezes ("Not Responding")
                # right after Crawler Complete, blocking Export and Save As clicks. The
                # SQLite db at %APPDATA%\Botsol\db.sqlite is the source of truth - we
                # snapshot+dump it, write into OUTPUT_DIR using Botsol's CSV format, and
                # let csv_processor pick it up identically to a real export. Then click
                # Delete Current Data to clear Botsol's grid for the next run.
                Write-Log "DONE: extracting via SQLite -> $outPath"
                if (-not (Test-Path $EXTRACT_SCRIPT)) {
                    Write-Log "extract script missing: $EXTRACT_SCRIPT" 'error'
                    if ($state.current_run_id) {
                        Update-ScrapeJob -Id $state.current_run_id -Patch @{
                            status = 'failed'
                            error  = "extract script not found: $EXTRACT_SCRIPT"
                        } | Out-Null
                    }
                    Post-Slack ":x: Botsol SQLite extract script missing for $country/$srcFile"
                    Abandon-CurrentRun -country $country -srcFile $srcFile -reason "extract script missing"
                    exit 0
                }
                Ensure-Dir (Split-Path $outPath -Parent)
                $extractLog = Join-Path 'C:\worker\logs' "extract-$country-$stem-$stamp.log"
                $proc = Start-Process -FilePath 'python' `
                    -ArgumentList @('-u', $EXTRACT_SCRIPT, $outPath) `
                    -NoNewWindow -Wait -PassThru `
                    -RedirectStandardOutput $extractLog `
                    -RedirectStandardError "$extractLog.err"
                if ($proc.ExitCode -ne 0) {
                    Write-Log "SQLite extract failed (exit=$($proc.ExitCode)); see $extractLog" 'error'
                    if ($state.current_run_id) {
                        Update-ScrapeJob -Id $state.current_run_id -Patch @{
                            status = 'failed'
                            error  = "sqlite extract exit=$($proc.ExitCode)"
                        } | Out-Null
                    }
                    Post-Slack ":x: Botsol SQLite extract failed for $country/$srcFile (exit=$($proc.ExitCode))"
                    Abandon-CurrentRun -country $country -srcFile $srcFile -reason "sqlite extract failed"
                    exit 0
                }
                Write-Log "SQLite extract complete; output at $outPath"
            } else {
                # Legacy UI-driven Export path (kept for fallback/debug).
                if (-not (Invoke-Element $btnExport)) {
                    Write-Log "failed to invoke Export Data" 'error'
                    if ($state.current_run_id) {
                        Update-ScrapeJob -Id $state.current_run_id -Patch @{
                            status = 'failed'
                            error  = 'invoke export failed'
                        } | Out-Null
                    }
                    Post-Slack ":x: Botsol export click failed for $country/$srcFile"
                    Abandon-CurrentRun -country $country -srcFile $srcFile -reason "export click failed"
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
                    Abandon-CurrentRun -country $country -srcFile $srcFile -reason "save dialog timeout"
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
                    Abandon-CurrentRun -country $country -srcFile $srcFile -reason "set filename failed"
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
                    Abandon-CurrentRun -country $country -srcFile $srcFile -reason "click save failed"
                    exit 0
                }

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
                Abandon-CurrentRun -country $country -srcFile $srcFile -reason "export no usable file"
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
            $countryFlag = switch ($country) {
                'colombia' { ':flag-co:' }
                'ecuador'  { ':flag-ec:' }
                'peru'     { ':flag-pe:' }
                'chile'    { ':flag-cl:' }
                'mexico'   { ':flag-mx:' }
                'brazil'   { ':flag-br:' }
                default    { ':earth_americas:' }
            }
            Post-Slack ("$countryFlag :checkered_flag: *Botsol finished* `"$country / $stem`"  rows=$latestN  duration=$duration")

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

            # Completed cleanly -> clear any hang-attempt counter for this category.
            try {
                $haFile = 'C:\worker\hang-attempts.json'
                if (Test-Path $haFile) {
                    $ha = @{}; (Get-Content $haFile -Raw | ConvertFrom-Json).PSObject.Properties | ForEach-Object { $ha[$_.Name] = [int]$_.Value }
                    $hkey = "$country/$stem"
                    if ($ha.ContainsKey($hkey)) { $ha.Remove($hkey); ($ha | ConvertTo-Json -Compress) | Set-Content $haFile -Encoding UTF8 }
                }
            } catch {}

            # Click Delete Current Data to clear for next run - gated by env var.
            # AUTO_DELETE_CURRENT=false keeps the in-memory data in Botsol as a safety net
            # (user can re-export manually if CSV looks wrong). After verifying the first
            # successful export, flip AUTO_DELETE_CURRENT=true in orchestrator.env.
            if ($AUTO_DELETE_CURRENT) {
                if (-not (Invoke-Element $btnDelete)) {
                    Write-Log "Delete Current Data click failed (non-fatal)" 'warn'
                }
            } else {
                Write-Log "skipping Delete Current Data (AUTO_DELETE_CURRENT=false). Verify CSV then click Delete manually, then flip the env var."
                Write-Log ":mag: Colombia / do exported + txt moved. Data still in Botsol memory as safety net; verify CSV then click Delete manually + set AUTO_DELETE_CURRENT=true."
            }

            # Record last-completed country so the next IDLE tick can detect a
            # country handoff (e.g. ecuador -> colombia) and post a special Slack.
            try { Set-Content -Path 'C:\worker\botsol-last-country.txt' -Value $country -Encoding UTF8 } catch {}

            Clear-QueueState
            exit 0
        }

        'IDLE' {
            if (-not $AUTO_START_NEXT) {
                Write-Log "IDLE but AUTO_START_NEXT=false; pausing (operator handoff required for Start Bot + numeric prompts flow)"
                # Post Slack once per pause - state-less dedupe: only notify if we just
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

            if (-not (Handle-NumericPrompt -Value $RESULT_CAP -TimeoutSec 30 -Tag 'prompt1_result_cap')) {
                Write-Log "first numeric prompt failed (continuing -- file-open may still appear)" 'warn'
                if ($jobId) {
                    Update-ScrapeJob -Id $jobId -Patch @{ status='failed'; error='prompt1 failed' } | Out-Null
                }
                Post-Slack ":x: Botsol prompt1 (result cap) failed for $($next.Country)/$($next.Stem)"
                exit 0
            }
            if (-not (Handle-NumericPrompt -Value $KEYWORD_LIMIT -TimeoutSec 30 -Tag 'prompt2_keyword_limit')) {
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
            if (-not (Handle-NumericPrompt -Value $next.FullPath -TimeoutSec 45 -Tag 'file_select')) {
                Write-Log "file-select dialog failed (path: $($next.FullPath))" 'error'
                if ($jobId) {
                    Update-ScrapeJob -Id $jobId -Patch @{
                        status = 'failed'
                        error  = 'file-select dialog failed'
                    } | Out-Null
                }
                Post-Slack ":x: Botsol file-select failed for $($next.Country)/$($next.Stem)"

                # Consecutive-failure cap (2026-07-08 audit): one bad dialog used to wedge the
                # pipeline for days (45.5h on egypt/eat_r2_1.txt) because the same file was
                # retried forever. Track fails in queue-state; after 3 in a row on the SAME
                # file, quarantine it so the queue picker takes the next one. Counter lives in
                # botsol-queue-state.json (file_select_fail_count + file_select_fail_file) and
                # is naturally reset by the fresh state written on a successful start.
                $failKey   = "$($next.Country)/$($next.FileName)"
                $failCount = 0
                if ($state -and $state.file_select_fail_file -and ([string]$state.file_select_fail_file -eq $failKey)) {
                    try { $failCount = [int]$state.file_select_fail_count } catch { $failCount = 0 }
                }
                $failCount++
                # Never quarantine while a scrape is RUNNING (Stop enabled = crawl in flight).
                if ($failCount -ge 3 -and -not $stopEn) {
                    try {
                        $quarDir = Join-Path (Join-Path $KEYWORDS_ROOT $next.Country) 'quarantine'
                        Ensure-Dir $quarDir
                        if (Test-Path -LiteralPath $next.FullPath) {
                            Move-Item -LiteralPath $next.FullPath -Destination (Join-Path $quarDir $next.FileName) -Force
                        }
                        Write-Log "file-select failed $failCount consecutive times on $failKey -> moved to quarantine/ (queue continues with next file)" 'error'
                        Post-Slack (":rotating_light: *DANGER: Botsol quarantined* ``$failKey`` after $failCount consecutive file-select failures. File moved to quarantine/; the queue continues with the next file. :point_right: inspect the file + BotsolApp dialogs on 66.70.134.73.")
                    } catch {
                        Write-Log "quarantine move failed for $failKey : $($_.Exception.Message)" 'error'
                    }
                    $fsState = @{}
                    if ($state) { foreach ($p in $state.PSObject.Properties) { $fsState[$p.Name] = $p.Value } }
                    $fsState.file_select_fail_count = 0
                    $fsState.file_select_fail_file  = ''
                    Write-QueueState $fsState
                } else {
                    $fsState = @{}
                    if ($state) { foreach ($p in $state.PSObject.Properties) { $fsState[$p.Name] = $p.Value } }
                    $fsState.file_select_fail_count = $failCount
                    $fsState.file_select_fail_file  = $failKey
                    Write-QueueState $fsState
                    Write-Log "file-select consecutive failure $failCount of 3 for $failKey (retrying next tick)" 'warn'
                }
                exit 0
            }

            $startedIso = (Get-Date).ToUniversalTime().ToString("o")
            if ($jobId) {
                Update-ScrapeJob -Id $jobId -Patch @{
                    status     = 'running'
                    started_at = $startedIso
                } | Out-Null
            }

            $nextFlag = switch ($next.Country) {
                'colombia' { ':flag-co:' }
                'ecuador'  { ':flag-ec:' }
                'peru'     { ':flag-pe:' }
                'chile'    { ':flag-cl:' }
                'mexico'   { ':flag-mx:' }
                'brazil'   { ':flag-br:' }
                default    { ':earth_americas:' }
            }
            $variantEmoji = if ($next.Variant -eq 'pruned') { ':scissors:' } else { ':seedling:' }

            # Country handoff detection: if the previous completed country differs
            # from $next.Country, we just rolled over (e.g. ecuador exhausted ->
            # colombia first kw). Post a celebratory handoff before the keyword line.
            try {
                $lastCountryFile = 'C:\worker\botsol-last-country.txt'
                if (Test-Path $lastCountryFile) {
                    $lastCountry = (Get-Content $lastCountryFile -Raw).Trim()
                    if ($lastCountry -and $lastCountry -ne $next.Country) {
                        $lastFlag = switch ($lastCountry) {
                            'colombia' { ':flag-co:' }
                            'ecuador'  { ':flag-ec:' }
                            'peru'     { ':flag-pe:' }
                            'chile'    { ':flag-cl:' }
                            'mexico'   { ':flag-mx:' }
                            'brazil'   { ':flag-br:' }
                            default    { ':earth_americas:' }
                        }
                        $lastProgress = Get-CountryProgress -Country $lastCountry
                        Post-Slack ("$lastFlag :tada: *$lastCountry COMPLETED* ($($lastProgress.Done)/$($lastProgress.Total) stems)  :arrow_right:  $nextFlag rolling into *$($next.Country)*")
                    }
                }
            } catch {}

            Write-Log ("$nextFlag :rocket: *Botsol switching keyword* -> `"$($next.Country) / $($next.Stem)`" $variantEmoji ($($next.Variant))`n   keywords=$lineCount  cap=$RESULT_CAP  limit=$KEYWORD_LIMIT")

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
            # AMBIGUOUS = main buttons disabled, usually because a child modal is up.
            # Resume orphaned Start sequences: detect InputInteger / InputString / frmSelectFile
            # left over from a previous tick that timed out, fill them in, and let next tick
            # advance the flow. Without this, the agent is stuck forever.
            $resumed = $false
            try {
                $kids = $win.FindAll([System.Windows.Automation.TreeScope]::Children,
                    [System.Windows.Automation.Condition]::TrueCondition)
                foreach ($k in $kids) {
                    try {
                        $aid = [string]$k.Current.AutomationId
                        if (-not $k.Current.IsEnabled) { continue }
                        if ($aid -eq 'frmLimitInput') {
                            # Orphaned result-cap dialog ("How many businesses...", radio-gated).
                            # Replay the remaining Start sequence inline with the CORRECT values so
                            # the later delay prompt isn't filled with RESULT_CAP. Path from queue.
                            $rcNext = Get-NextQueueItem
                            $rcWhat = if ($rcNext) { "$($rcNext.Country)/$($rcNext.FileName)" } else { '<no next>' }
                            Write-Log "AMBIGUOUS resume: orphaned result-cap dialog (frmLimitInput); replaying start for $rcWhat" 'warn'
                            if (Handle-NumericPrompt -Value $RESULT_CAP -TimeoutSec 8 -Tag 'resume_result_cap') {
                                Handle-NumericPrompt -Value $KEYWORD_LIMIT -TimeoutSec 15 -Tag 'resume_keyword_limit' | Out-Null
                                Dismiss-BoolInput -ButtonName 'Yes' -TimeoutSec 8 | Out-Null
                                if ($rcNext -and $rcNext.FullPath) {
                                    if (Handle-NumericPrompt -Value $rcNext.FullPath -TimeoutSec 45 -Tag 'resume_file_select') {
                                        # Persist queue-state so the DONE handler can export + roll to the next stem.
                                        Write-QueueState @{
                                            current_run_id      = $null
                                            current_country     = $rcNext.Country
                                            current_source_file = $rcNext.FileName
                                            started_at          = (Get-Date).ToUniversalTime().ToString("o")
                                        }
                                        Post-Slack ":wrench: botsol-agent recovered stuck result-cap dialog -> started $($rcNext.Country)/$($rcNext.Stem)"
                                    }
                                } else {
                                    Write-Log "AMBIGUOUS resume: filled cap but no next item for file-select" 'warn'
                                }
                                $resumed = $true
                            }
                            break
                        }
                        if ($aid -eq 'InputInteger' -or $aid -eq 'frmInput' -or $aid -eq 'frmIntInput') {
                            Write-Log "AMBIGUOUS resume: found stuck numeric prompt aid=$aid; filling with $RESULT_CAP" 'warn'
                            if (Handle-NumericPrompt -Value $RESULT_CAP -TimeoutSec 4 -Tag 'resume_numeric') {
                                Post-Slack ":wrench: botsol-agent resumed orphaned numeric prompt with $RESULT_CAP"
                                $resumed = $true
                            }
                            break
                        }
                        if ($aid -eq 'InputFile' -or $aid -eq 'frmSelectFile') {
                            $state2 = Read-QueueState
                            if ($state2 -and $state2.current_full_path) {
                                $kwPath = [string]$state2.current_full_path
                                Write-Log "AMBIGUOUS resume: found stuck file picker aid=$aid; filling with $kwPath" 'warn'
                                if (Handle-NumericPrompt -Value $kwPath -TimeoutSec 4 -Tag 'resume_file') {
                                    Post-Slack ":wrench: botsol-agent resumed orphaned file picker -> $kwPath"
                                    $resumed = $true
                                }
                            } else {
                                Write-Log "AMBIGUOUS resume: file picker up but queue state missing path" 'warn'
                            }
                            break
                        }
                    } catch {}
                }
            } catch {}
            if (-not $resumed) {
                Write-Log "AMBIGUOUS button combo (start=$startEn stop=$stopEn export=$exportEn delete=$deleteEn); no action" 'warn'
            }
            exit 0
        }
    }

} catch {
    $msg = $_.Exception.Message
    Write-Log "unhandled error: $msg" 'error'
    try { Post-Slack ":x: botsol-agent unhandled error: $msg" } catch {}
    exit 0
}
