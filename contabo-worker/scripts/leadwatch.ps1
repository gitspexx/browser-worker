# Lead-stall watchdog: auto-recovers Botsol when it stops producing NEW leads.
# Trigger requires BOTH: (a) no new lead imported for STALL_HOURS, AND (b) no active
# scraping (phase=RUNNING / latest_record) in the last ACTIVE_MIN minutes. This avoids
# killing a slow-but-working scrape while catching wedged states (wrong bot / DONE-loop).
$ErrorActionPreference='SilentlyContinue'
$log='C:\worker\logs\leadwatch.txt'
function L($m){ "$((Get-Date -Format 'yyyy-MM-dd HH:mm:ss'))  $m" | Add-Content $log }
$STALL_HOURS   = 6
$ACTIVE_MIN    = 15
$COOLDOWN_HOURS= 3
$stamp='C:\worker\leadwatch-last-restart.stamp'

# (a) age of last NEW>0 import
$pl='C:\Botsol\pipeline\pipeline.log'
$lastNew=$null
$ln = (Get-Content $pl -EA SilentlyContinue | Select-String 'New \(after dedup\): [1-9]' | Select-Object -Last 1)
if($ln -and $ln.Line -match '^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'){ $lastNew=[datetime]::ParseExact($Matches[1],'yyyy-MM-dd HH:mm:ss',$null) }
$ageH = if($lastNew){ ((Get-Date)-$lastNew).TotalHours } else { 999 }
L ("last new lead: {0} ({1:N1}h ago)" -f ($(if($lastNew){$lastNew.ToString('MM-dd HH:mm')}else{'NONE'})),$ageH)
if($ageH -lt $STALL_HOURS){ L "healthy (<${STALL_HOURS}h) -> no action"; return }

# (b) is it actively scraping right now?
$al = (Get-Content C:\worker\logs\botsol-agent.log -Tail 12 -EA SilentlyContinue | Select-String 'phase=RUNNING|latest_record=' | Select-Object -Last 1)
$activeMinAgo = 9999
if($al -and $al.Line -match '^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'){ $activeMinAgo = ((Get-Date)-[datetime]::ParseExact($Matches[1],'yyyy-MM-dd HH:mm:ss',$null)).TotalMinutes }
L ("last scrape activity: {0:N1} min ago" -f $activeMinAgo)
if($activeMinAgo -lt $ACTIVE_MIN){ L "actively scraping (<${ACTIVE_MIN}min) -> working, defer"; return }

# cooldown
$lastRestart = if(Test-Path $stamp){ (Get-Item $stamp).LastWriteTime } else { [datetime]'2000-01-01' }
$sinceH = ((Get-Date)-$lastRestart).TotalHours
if($sinceH -lt $COOLDOWN_HOURS){ L ("STALLED ${ageH}h but last restart {0:N1}h ago (<${COOLDOWN_HOURS}h) -> wait" -f $sinceH); return }

L ("STALL CONFIRMED: ${ageH}h no new leads + idle ${activeMinAgo}min -> auto-recovery restart")
$wh=$null; foreach($e in (Get-Content C:\worker\orchestrator.env -EA SilentlyContinue)){ if($e -match '^\s*SLACK_WEBHOOK\s*=\s*(.+?)\s*$'){ $wh=$Matches[1] } }
if($wh){ try{ Invoke-RestMethod -Uri $wh -Method Post -ContentType 'application/json' -TimeoutSec 15 -Body (@{text=(":rotating_light: *Botsol lead-stall auto-recovery* on $env:COMPUTERNAME -- no new leads for {0:N1}h and idle. Restarting + re-selecting 5.1 Auto Search." -f $ageH)}|ConvertTo-Json -Compress) | Out-Null; L 'slack sent' }catch{ L "slack fail $($_.Exception.Message)" } }

1..3 | %{ Get-Process BotsolApp,chrome,chromedriver -EA SilentlyContinue | Stop-Process -Force; Start-Sleep -Seconds 2 }
Remove-Item C:\worker\botsol-delete-attempted.stamp,C:\worker\botsol-queue-state.json -EA SilentlyContinue
Start-ScheduledTask -TaskName BotsolManualLaunch; Start-Sleep -Seconds 16
Start-ScheduledTask -TaskName BotsolEnforce;      Start-Sleep -Seconds 9
Start-ScheduledTask -TaskName BotsolEnforce;      Start-Sleep -Seconds 6
Enable-ScheduledTask -TaskName BotsolAgent | Out-Null
Set-Content $stamp (Get-Date -Format o)
L 'recovery issued: relaunched, HwClick re-selects 5.1, agent enabled'
