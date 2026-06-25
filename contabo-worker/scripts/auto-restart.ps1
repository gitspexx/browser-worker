# Guarded auto-restart: (A) re-queue hung cat, (A2) stuck-chooser escalation, (B-D) hang restart. Posts button panel to #botsol.
$log='C:\worker\logs\auto-restart.txt'
function L($m){ "$((Get-Date -Format 'u'))  $m" | Add-Content $log }
$HANG_MIN=50; $COOLDOWN_MIN=15; $CHOOSER_STUCK_MIN=15
$stamp='C:\worker\last-auto-restart.txt'
$pend='C:\worker\requeue-pending.txt'
$cm='C:\worker\chooser-alerted.marker'
$base='C:\Botsol\pipeline\keywords_v2'

# Real "Select Bot" chooser detection. The (A2) escalation must NEVER trust the marker
# file alone: an orphaned marker (e.g. left by a disabled ChooserNav) made auto-restart
# kill a healthy scraper ~31x/day on 2026-06-25. Only restart if a chooser window is
# actually on screen right now.
Add-Type -TypeDefinition @"
using System;using System.Text;using System.Runtime.InteropServices;
public class ARWin {
 public delegate bool EnumProc(IntPtr h, IntPtr l);
 [DllImport("user32.dll")] public static extern bool EnumWindows(EnumProc cb, IntPtr l);
 [DllImport("user32.dll")] public static extern int GetWindowText(IntPtr h, StringBuilder s, int n);
 [DllImport("user32.dll")] public static extern bool IsWindowVisible(IntPtr h);
 public static bool Found=false;
 public static bool Cb(IntPtr h, IntPtr l){ if(IsWindowVisible(h)){ var sb=new StringBuilder(256); GetWindowText(h,sb,256); if(sb.ToString().Contains("Select Bot")){ Found=true; return false; } } return true; }
}
"@
function Test-ChooserUp(){
  [ARWin]::Found=$false
  $cb=[ARWin+EnumProc]{ param($h,$l) [ARWin]::Cb($h,$l) }
  [ARWin]::EnumWindows($cb,[IntPtr]::Zero) | Out-Null
  return [ARWin]::Found
}

function PostPanel($text){
  $bw=$null
  foreach($ln in (Get-Content 'C:\worker\orchestrator.env' -EA SilentlyContinue)){ if($ln -match '^\s*BOTSOL_WEBHOOK\s*=\s*(.+?)\s*$'){ $bw=$matches[1] } }
  if(-not $bw){ return }
  $body=@{ blocks=@(
    @{ type='section'; text=@{ type='mrkdwn'; text=$text } },
    @{ type='actions'; elements=@(
      @{ type='button'; action_id='botsol_status'; text=@{ type='plain_text'; text=':bar_chart: Status' } },
      @{ type='button'; action_id='botsol_restart'; style='danger'; text=@{ type='plain_text'; text=':arrows_counterclockwise: Restart' }; confirm=@{ title=@{type='plain_text';text='Restart Botsol?'}; text=@{type='plain_text';text='Kills + relaunches BotsolApp.'}; confirm=@{type='plain_text';text='Restart'}; deny=@{type='plain_text';text='Cancel'} } },
      @{ type='button'; action_id='botsol_solve'; text=@{ type='plain_text'; text=':computer_mouse: Solve chooser' } }
    ) }
  ) } | ConvertTo-Json -Depth 12
  try{ Invoke-RestMethod -Uri $bw -Method Post -Body $body -ContentType 'application/json' -TimeoutSec 15 | Out-Null }catch{}
}
function Cooling(){ if(Test-Path $stamp){ try{ $l=[datetime]::Parse((Get-Content $stamp -Raw).Trim()); if(((Get-Date)-$l).TotalMinutes -lt $COOLDOWN_MIN){ return $true } }catch{} }; return $false }
function Restart($why){ Get-Process BotsolApp -EA SilentlyContinue | Stop-Process -Force; Get-Process chrome,chromedriver -EA SilentlyContinue | Stop-Process -Force; Start-Sleep -Seconds 4; Start-ScheduledTask -TaskName BotsolManualLaunch; Set-Content $stamp ((Get-Date).ToString('o')); L $why }

# --- (A) process pending re-queue ---
if(Test-Path $pend){
  $line=(Get-Content $pend -Raw).Trim()
  if($line -match '^(.+?)\|(.+)$'){
    $rqcat=$matches[1]; $when=$matches[2]; $age=999; try{ $age=((Get-Date)-[datetime]::Parse($when)).TotalMinutes }catch{}
    if($age -ge 7){
      $p=($rqcat -replace '/','\'); $cc=($p -split '\\')[0]; $ff=($p -split '\\')[-1]
      $donef="$base\$cc\done\$ff"; $actf="$base\$cc\$ff"
      if(Test-Path $donef){ Move-Item $donef $actf -Force; Remove-Item $pend -Force; L "re-queued $rqcat (done->active)" }
      elseif($age -ge 40){ Remove-Item $pend -Force; L "re-queue ${rqcat}: gave up after ${age}min" }
    }
  } else { Remove-Item $pend -Force }
}

# --- (A2) stuck-chooser escalation (verified against a REAL chooser window) ---
if(Test-Path $cm){
  $cage=((Get-Date)-(Get-Item $cm).LastWriteTime).TotalMinutes
  if(-not (Test-ChooserUp)){
    Remove-Item $cm -Force -EA SilentlyContinue
    L ("orphan chooser marker (age $([int]$cage)min) but NO 'Select Bot' window visible -> deleted marker, NO restart")
  }
  elseif($cage -ge $CHOOSER_STUCK_MIN -and -not (Cooling)){
    Restart ("CHOOSER stuck $([int]$cage)min (window confirmed) -> fresh restart")
    PostPanel(":robot_face: *Botsol chooser was wedged* (~$([int]$cage)min) - fresh restart issued; hwclick will re-solve it. Tap if you want to check or intervene.")
    return
  }
}

# --- (B) cooldown ---
if(Cooling){ return }

# --- (C) detect recent confirmed long hang ---
$al='C:\worker\logs\botsol-agent.log'
if(-not (Test-Path $al)){ return }
$hung=(Get-Content $al -Tail 12 | Where-Object { $_ -match 'HUNG: still frozen ~(\d+) min' } | Select-Object -Last 1)
if(-not $hung){ return }
if($hung -notmatch '^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+\[info\] HUNG: still frozen ~(\d+) min on (.+?);'){ return }
$lt=[datetime]::Parse($matches[1]); $frozen=[int]$matches[2]; $cat=$matches[3]
if(((Get-Date)-$lt).TotalMinutes -gt 6){ return }
if($frozen -lt $HANG_MIN){ return }

# --- (D) restart + queue hung category ---
Restart "HUNG $frozen min on $cat -> AUTO-RESTART"
Set-Content $pend ("{0}|{1}" -f $cat,(Get-Date).ToString('o'))
L "queued $cat for re-scrape"
PostPanel(":robot_face: *Botsol self-healed* - '$cat' was frozen ${frozen}min (Chrome-death). Auto-restarted, chooser auto-clicked, category re-queued. Tap to check status.")
L 'done'
