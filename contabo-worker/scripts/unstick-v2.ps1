# unstick-v2: FAST recovery for the detail-panel wedge (Chrome ALIVE, crawl frozen on a
# Maps business detail). Screenshots proved these freezes have a live Maps window -- so a
# clean Stop is SAFE (it does NOT deadlock; that only happens when the scraper Chrome is
# DEAD). On a confirmed alive-stall, real-click Botsol's Stop -> the agent's next tick sees
# DONE -> extracts the loaded rows -> loads the next keyword. ~2 min vs the ~20 min restart.
#
# GATED hard: only acts when (a) stall_ticks >= MIN, (b) a live "Google Maps" Chrome window
# is present (Chrome dead -> SKIP, leave for auto-restart), (c) cooldown. Backstopped by the
# 18-min auto-restart if this no-ops. Before/after screenshots for evidence. Run interactive.
$ErrorActionPreference='SilentlyContinue'
$log='C:\worker\logs\unstick-v2.log'
function L($m){ "$((Get-Date -Format 'yyyy-MM-dd HH:mm:ss'))  $m" | Add-Content $log }
$shotDir='C:\worker\logs\unstick'; if(-not(Test-Path $shotDir)){ New-Item -ItemType Directory -Force -Path $shotDir|Out-Null }
$STALL_MIN_TICKS = 5      # ~10 min frozen before we act (after the agent's ~12min soft watch starts)
$COOLDOWN_MIN    = 12     # one attempt per freeze episode
$stamp='C:\worker\unstick-v2.stamp'

# --- gate (a): confirmed stall from the agent's queue-state ---
$qs = Get-Content 'C:\worker\botsol-queue-state.json' -Raw -EA SilentlyContinue | ConvertFrom-Json
$ticks=0; try{ $ticks=[int]$qs.stall_ticks }catch{}
if($ticks -lt $STALL_MIN_TICKS){ exit 0 }
if(Test-Path $stamp){ try{ if(((Get-Date)-(Get-Item $stamp).LastWriteTime).TotalMinutes -lt $COOLDOWN_MIN){ exit 0 } }catch{} }

Add-Type -AssemblyName UIAutomationClient; Add-Type -AssemblyName UIAutomationTypes
Add-Type -AssemblyName System.Windows.Forms; Add-Type -AssemblyName System.Drawing
Add-Type -TypeDefinition @"
using System; using System.Text; using System.Runtime.InteropServices;
public class Uv {
  public delegate bool EnumProc(IntPtr h, IntPtr l);
  [DllImport("user32.dll")] public static extern bool EnumWindows(EnumProc cb, IntPtr l);
  [DllImport("user32.dll")] public static extern int GetWindowText(IntPtr h, StringBuilder s, int n);
  [DllImport("user32.dll")] public static extern int GetClassName(IntPtr h, StringBuilder s, int n);
  [DllImport("user32.dll")] public static extern bool IsWindowVisible(IntPtr h);
  [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr h);
  [DllImport("user32.dll")] public static extern bool SetCursorPos(int x,int y);
  [DllImport("user32.dll")] public static extern void mouse_event(uint f,uint dx,uint dy,uint d,IntPtr e);
  public static bool MapsAlive=false;
  public static bool Cb(IntPtr h, IntPtr l){
    if(!IsWindowVisible(h)) return true;
    var cn=new StringBuilder(64); GetClassName(h,cn,64);
    if(cn.ToString()!="Chrome_WidgetWin_1") return true;
    var sb=new StringBuilder(512); GetWindowText(h,sb,512); var t=sb.ToString();
    if(t.Contains("Google Maps") && !t.Contains("AdsPower")){ MapsAlive=true; return false; }
    return true;
  }
  public static void Click(int x,int y){ SetCursorPos(x,y); System.Threading.Thread.Sleep(250); mouse_event(0x0002,0,0,0,IntPtr.Zero); System.Threading.Thread.Sleep(90); mouse_event(0x0004,0,0,0,IntPtr.Zero); }
}
"@
function Shot($tag){
  try{ $b=[System.Windows.Forms.SystemInformation]::VirtualScreen; $bmp=New-Object System.Drawing.Bitmap($b.Width,$b.Height)
    $g=[System.Drawing.Graphics]::FromImage($bmp); $g.CopyFromScreen($b.X,$b.Y,0,0,$bmp.Size)
    $fn=Join-Path $shotDir ("{0}-{1}.png" -f (Get-Date -Format 'yyyyMMdd-HHmmss'),$tag); $bmp.Save($fn); $g.Dispose(); $bmp.Dispose(); return $fn }catch{ return '' }
}

# --- gate (b): is the scraper Chrome ALIVE on Maps? (dead -> leave for restart) ---
$cb=[Uv+EnumProc]{ param($h,$l) [Uv]::Cb($h,$l) }
[Uv]::MapsAlive=$false; [Uv]::EnumWindows($cb,[IntPtr]::Zero) | Out-Null
$cc="$($qs.current_country)/$($qs.current_source_file)"
if(-not [Uv]::MapsAlive){ L "stall ticks=$ticks on $cc but NO live Google-Maps Chrome window -> Chrome likely DEAD, skipping (auto-restart will handle)"; exit 0 }

# --- act: real-click Botsol Stop ---
$AE=[System.Windows.Automation.AutomationElement];$TS=[System.Windows.Automation.TreeScope]
$root=$AE::RootElement
$bp=(Get-Process BotsolApp -EA SilentlyContinue).Id
$win=$null
foreach($w in $root.FindAll($TS::Children,[System.Windows.Automation.Condition]::TrueCondition)){ if($w.Current.ProcessId -eq $bp -and $w.Current.Name -match 'Crawler App'){ $win=$w; break } }
if(-not $win){ L "stall ticks=$ticks, Maps alive, but no BotsolApp main window (pid=$bp) -> skip"; exit 0 }
$stop=$win.FindFirst($TS::Descendants,(New-Object System.Windows.Automation.PropertyCondition($AE::AutomationIdProperty,'btnStop')))
if(-not $stop -or -not $stop.Current.IsEnabled){ L "stall ticks=$ticks, Maps alive, but Stop not enabled -> skip"; exit 0 }
$before=Shot "before-$($qs.current_country)-rec$($qs.stall_last_record)-tick$ticks"
$r=$stop.Current.BoundingRectangle
[Uv]::SetForegroundWindow([IntPtr]$win.Current.NativeWindowHandle)|Out-Null; Start-Sleep -Milliseconds 500
$cx=[int]($r.X+$r.Width/2); $cy=[int]($r.Y+$r.Height/2)
L "FAST-UNSTICK: Chrome alive + stall ticks=$ticks on $cc rec=$($qs.stall_last_record) -> real-click Stop @ $cx,$cy"
[Uv]::Click($cx,$cy)
Set-Content $stamp (Get-Date -Format o)
Start-Sleep -Seconds 7
$after=Shot "after-$($qs.current_country)-rec$($qs.stall_last_record)-tick$ticks"
# did Stop take? Start re-enabled = crawl ended cleanly (agent will extract+advance next tick)
$st=$win.FindFirst($TS::Descendants,(New-Object System.Windows.Automation.PropertyCondition($AE::AutomationIdProperty,'btnStart')))
$startEn = try{ [bool]$st.Current.IsEnabled }catch{ $false }
L "result: btnStart.enabled=$startEn (true = Stop worked, crawl ended; agent advances next tick). shots: $before | $after"
# Slack
$wh=$null; foreach($e in (Get-Content 'C:\worker\orchestrator.env' -EA SilentlyContinue)){ if($e -match '^\s*SLACK_WEBHOOK\s*=\s*(.+?)\s*$'){ $wh=$Matches[1] } }
if($wh){ try{ Invoke-RestMethod -Uri $wh -Method Post -ContentType 'application/json' -TimeoutSec 12 -Body (@{text=(":zap: *Botsol fast-unstick* on $env:COMPUTERNAME -- $cc wedged on a live Maps detail (rec #$($qs.stall_last_record), $ticks ticks). Clicked Stop; crawl ends + advances to next keyword. (Start re-enabled: $startEn)")}|ConvertTo-Json -Compress)|Out-Null }catch{} }
Get-ChildItem $shotDir -Filter *.png | Sort-Object LastWriteTime -Descending | Select-Object -Skip 40 | Remove-Item -Force -EA SilentlyContinue
