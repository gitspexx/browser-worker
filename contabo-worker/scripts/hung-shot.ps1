# Screenshot-on-HUNG. Captures the desktop when a botsol crawl is frozen (record counter
# stalled) so we can SEE what the scraper Chrome shows during a HUNG -- a captcha /
# "unusual traffic" / error page confirms Google datacenter-IP throttling (vs a real crash).
# ADDITIVE: reads the agent's queue-state only, never touches the scrape. Must run in the
# INTERACTIVE session (BotsolHungShot task) so CopyFromScreen sees session 1, not black.
$ErrorActionPreference='SilentlyContinue'
$log='C:\worker\logs\hung-shot.log'
function L($m){ "$((Get-Date -Format 'yyyy-MM-dd HH:mm:ss'))  $m" | Add-Content $log }
$dir='C:\worker\logs\hung'; if(-not(Test-Path $dir)){ New-Item -ItemType Directory -Force -Path $dir | Out-Null }
$STALL_TICKS_MIN = 5      # ~10 min no progress (agent ticks ~2 min) -> early enough to catch the block live
$COOLDOWN_MIN    = 20     # at most one shot per 20 min (one per freeze episode)
$stamp='C:\worker\hung-shot.stamp'
$testFlag='C:\worker\hung-shot-test.flag'   # presence forces one capture (manual screenshot tool)

$forced = Test-Path $testFlag
$qs = Get-Content 'C:\worker\botsol-queue-state.json' -Raw -EA SilentlyContinue | ConvertFrom-Json
$ticks=0; try{ $ticks=[int]$qs.stall_ticks }catch{}

if(-not $forced){
  if(-not $qs){ exit 0 }
  if($ticks -lt $STALL_TICKS_MIN){ exit 0 }                       # not stalled enough
  if(Test-Path $stamp){ try{ if(((Get-Date)-(Get-Item $stamp).LastWriteTime).TotalMinutes -lt $COOLDOWN_MIN){ exit 0 } }catch{} }
}

# capture the full virtual screen
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
$b=[System.Windows.Forms.SystemInformation]::VirtualScreen
$bmp=New-Object System.Drawing.Bitmap($b.Width,$b.Height)
$g=[System.Drawing.Graphics]::FromImage($bmp); $g.CopyFromScreen($b.X,$b.Y,0,0,$bmp.Size)
function San($s){ if($null -eq $s -or "$s".Trim() -eq ''){ return 'na' }; return ("$s" -replace '[\\/:*?"<>|\s]','_') }
$cc=San($qs.current_country); $cf=San(($qs.current_source_file -replace '\.txt','')); $rec=San($qs.stall_last_record)
$tag=if($forced){'TEST'}else{'hung'}
$fn = Join-Path $dir ("{0}-{1}-{2}_{3}-rec{4}-tick{5}.png" -f $tag,(Get-Date -Format 'yyyyMMdd-HHmmss'),$cc,$cf,$rec,$ticks)
$bmp.Save($fn); $g.Dispose(); $bmp.Dispose()
$kb=[int]((Get-Item $fn).Length/1KB)
if(-not $forced){ Set-Content $stamp (Get-Date -Format o) }
L "$tag screenshot: $cc/$cf rec=$rec ticks=$ticks -> $fn (${kb}KB)"

# Slack a heads-up (real shot only) so a freeze is visible without RDP
if(-not $forced){
  $wh=$null; foreach($e in (Get-Content 'C:\worker\orchestrator.env' -EA SilentlyContinue)){ if($e -match '^\s*SLACK_WEBHOOK\s*=\s*(.+?)\s*$'){ $wh=$Matches[1] } }
  if($wh){ try{ Invoke-RestMethod -Uri $wh -Method Post -ContentType 'application/json' -TimeoutSec 12 -Body (@{text=(":camera_with_flash: *Botsol HUNG screenshot* on $env:COMPUTERNAME -- $cc/$cf frozen at record #$rec ($ticks ticks). Saved $fn for block-diagnosis (captcha vs throttle).")}|ConvertTo-Json -Compress)|Out-Null }catch{} }
}

# keep only the last 30 shots
Get-ChildItem $dir -Filter *.png | Sort-Object LastWriteTime -Descending | Select-Object -Skip 30 | Remove-Item -Force -EA SilentlyContinue
