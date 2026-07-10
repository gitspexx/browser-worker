# Botsol daily autofuel: keeps each rotation country topped up with fresh keyword files.
# Counts PENDING *.txt directly under keywords_v2\<country>\ (not done\ or quarantine\). For any
# rotation country below LOW_THRESHOLD it runs replenish.py across successive rounds (r2..r5) until
# one produces files, then stops. Posts ONE Slack summary. Idempotent, additive, never deletes.
# Registration as a daily scheduled task is done by the orchestrator, not this script.
$PYTHON        = 'C:\Python312\python.exe'
$REPLENISH     = 'C:\Botsol\pipeline\replenish.py'
$KEYWORDS_ROOT = 'C:\Botsol\pipeline\keywords_v2'
$log           = 'C:\worker\logs\botsol-autofuel.log'
$ENV_FILE      = 'C:\worker\orchestrator.env'
$ROUNDS        = @('r2','r3','r4','r5')
$FILES_PER_DAY = 46   # matches replenish-keywords.py fuel estimate (~46 files consumed/day)

function L($m){ "$((Get-Date -Format 'u'))  $m" | Add-Content $log }

# ---------- Config loader (same shape as botsol-agent.ps1 Read-EnvFile) ----------
function Read-EnvFile {
    param([string]$Path)
    $cfg = @{}
    if (-not (Test-Path -LiteralPath $Path)) {
        L "env file not found at $Path"
        return $cfg
    }
    foreach ($raw in (Get-Content -LiteralPath $Path -Encoding UTF8 -EA SilentlyContinue)) {
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

# ---------- Slack (same Invoke-RestMethod pattern as auto-restart.ps1 PostDanger) ----------
function Post-Slack($webhook, $text){
    if (-not $webhook) { return }
    try {
        Invoke-RestMethod -Uri $webhook -Method Post -Body (@{ text=$text } | ConvertTo-Json -Compress) -ContentType 'application/json' -TimeoutSec 15 | Out-Null
    } catch {
        L "slack post failed: $($_.Exception.Message)"
    }
}

# Count *.txt directly in keywords_v2\<country>\ (NOT recursing into done\ or quarantine\).
function Count-Pending($country){
    $cdir = Join-Path $KEYWORDS_ROOT $country
    if (-not (Test-Path -LiteralPath $cdir)) { return -1 }
    $files = Get-ChildItem -LiteralPath $cdir -Filter '*.txt' -File -EA SilentlyContinue
    if ($null -eq $files) { return 0 }
    return @($files).Count
}

# Run replenish for one country+round; return files-written count parsed from stdout.
# The Python prints '<country>: N new cities, X files written, Y skipped' and
# 'DONE [<round>]: X new keyword files, ...'. We parse the DONE line's X (authoritative total).
function Invoke-Replenish($country, $round){
    $prevOnly  = $env:ONLY_COUNTRIES
    $prevRound = $env:BOTSOL_ROUND
    $written = 0
    try {
        $env:ONLY_COUNTRIES = $country
        $env:BOTSOL_ROUND   = $round
        $out = & $PYTHON $REPLENISH 2>&1
        foreach ($ln in $out) {
            $s = [string]$ln
            $m = [regex]::Match($s, 'DONE\s*\[[^\]]+\]:\s*(\d+)\s+new keyword files')
            if ($m.Success) { $written = [int]$m.Groups[1].Value }
        }
        L "replenish $country [$round]: $written files written"
    } catch {
        L "replenish $country [$round] ERROR: $($_.Exception.Message)"
    } finally {
        # restore/clear env so a later iteration never inherits a stale value
        if ($null -eq $prevOnly)  { Remove-Item Env:\ONLY_COUNTRIES -EA SilentlyContinue } else { $env:ONLY_COUNTRIES = $prevOnly }
        if ($null -eq $prevRound) { Remove-Item Env:\BOTSOL_ROUND   -EA SilentlyContinue } else { $env:BOTSOL_ROUND   = $prevRound }
    }
    return $written
}

# ---------- main ----------
L '--- autofuel run start ---'
$cfg = Read-EnvFile $ENV_FILE
$webhook = $cfg['SLACK_WEBHOOK']

$LOW_THRESHOLD = 8
if ($cfg['AUTOFUEL_LOW_THRESHOLD']) {
    $t = 0
    if ([int]::TryParse(([string]$cfg['AUTOFUEL_LOW_THRESHOLD']).Trim(), [ref]$t) -and $t -gt 0) { $LOW_THRESHOLD = $t }
}

$rotation = @()
if ($cfg['COUNTRY_ROTATION']) {
    $rotation = ($cfg['COUNTRY_ROTATION'] -split ',') |
        ForEach-Object { $_.Trim().ToLowerInvariant() } |
        Where-Object { $_ }
}

if ($rotation.Count -eq 0) {
    L 'COUNTRY_ROTATION empty or unset — nothing to do'
    L '--- autofuel run end ---'
    return
}
L "rotation=$($rotation -join ',') low_threshold=$LOW_THRESHOLD"

$toppedUp  = @()   # names of countries that got fresh files this run
$exhausted = @()   # names where every round returned 0 (defined pools spent)
$addedFiles = 0

foreach ($country in $rotation) {
    $pending = Count-Pending $country
    if ($pending -lt 0) {
        L "  ${country}: no keywords dir, skip"
        continue
    }
    if ($pending -ge $LOW_THRESHOLD) {
        L "  ${country}: $pending pending (ok)"
        continue
    }
    L "  ${country}: $pending pending (< $LOW_THRESHOLD) — topping up"
    $refilled = $false
    foreach ($round in $ROUNDS) {
        $written = Invoke-Replenish $country $round
        if ($written -gt 0) {
            $toppedUp += $country
            $addedFiles += $written
            $refilled = $true
            L "  ${country}: refilled from $round (+$written files)"
            break
        }
    }
    if (-not $refilled) {
        $exhausted += $country
        L "  ${country}: all rounds yielded 0 — defined city pools exhausted"
    }
}

# recompute total pending across rotation after top-ups
$totalPending = 0
foreach ($country in $rotation) {
    $p = Count-Pending $country
    if ($p -gt 0) { $totalPending += $p }
}
$days = [math]::Round($totalPending / $FILES_PER_DAY, 1)
L "summary: totalPending=$totalPending across $($rotation.Count) countries ~= ${days}d fuel; toppedUp=$($toppedUp.Count) exhausted=$($exhausted.Count) addedFiles=$addedFiles"

# ---------- exhausted-alert dedupe: only alert on NEWLY exhausted countries ----------
$stateDir = 'C:\worker\state'
$exStateFile = Join-Path $stateDir 'autofuel-exhausted.json'
$priorExhausted = @()
if (Test-Path $exStateFile) {
    try { $priorExhausted = @(Get-Content $exStateFile -Raw | ConvertFrom-Json) } catch { $priorExhausted = @() }
}
$newExhausted = @($exhausted | Where-Object { $priorExhausted -notcontains $_ })
# persist current set; refueled countries drop off so they can re-alert if they exhaust again later
if (-not (Test-Path $stateDir)) { New-Item -ItemType Directory -Path $stateDir -Force | Out-Null }
try { ,@($exhausted) | ConvertTo-Json -Compress | Set-Content -Path $exStateFile -Encoding ASCII } catch { L "warn: could not write $exStateFile" }
if ($exhausted.Count -gt 0 -and $newExhausted.Count -eq 0) { L "exhausted unchanged ($($exhausted -join ', ')) - already alerted, suppressing repeat" }

# ---------- one Slack summary ----------
$topList = ($toppedUp | Select-Object -Unique) -join ', '
$newExList = ($newExhausted | Select-Object -Unique) -join ', '
$msg = $null
if ($newExhausted.Count -gt 0) {
    $msg = ":rotating_light: *BOTSOL FUEL: $($newExhausted.Count) NEWLY-EXHAUSTED COUNTRIES - AUTHOR NEW CITIES* ($newExList). Total ~${days}d left."
} elseif ($days -lt 5) {
    $tu = if ($topList) { $topList } else { 'none' }
    $msg = ":warning: *Botsol fuel low*: ~${days}d ($totalPending files). Topped up: $tu."
} elseif ($toppedUp.Count -gt 0) {
    # :fuelpump: is not a standard Slack emoji; use :chart_with_upwards_trend: per spec.
    $msg = ":chart_with_upwards_trend: *Botsol autofuel*: topped up $topList (+$addedFiles files). ~${days}d fuel."
}

if ($msg) {
    Post-Slack $webhook $msg
    L "slack: $msg"
} else {
    L 'slack: (quiet - healthy, nothing to do)'
}

L '--- autofuel run end ---'
