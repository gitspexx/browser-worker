$o="C:\worker\logs\cc4.txt"; function L($m){ "$((Get-Date -Format HH:mm:ss))  $m" | Add-Content $o }
"=== cc4 $(Get-Date -Format HH:mm:ss) ===" | Set-Content $o
Add-Type -AssemblyName System.Windows.Forms,System.Drawing
Add-Type -TypeDefinition @"
using System;using System.Text;using System.Runtime.InteropServices;
public class WE {
 public delegate bool EnumProc(IntPtr h, IntPtr l);
 [DllImport("user32.dll")] public static extern bool EnumWindows(EnumProc cb, IntPtr l);
 [DllImport("user32.dll")] public static extern int GetWindowText(IntPtr h, StringBuilder s, int n);
 [DllImport("user32.dll")] public static extern bool IsWindowVisible(IntPtr h);
 [DllImport("user32.dll")] public static extern bool GetWindowRect(IntPtr h, out RECT r);
 [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr h);
 [DllImport("user32.dll")] public static extern bool BringWindowToTop(IntPtr h);
 [DllImport("user32.dll")] public static extern bool ShowWindow(IntPtr h,int n);
 [DllImport("user32.dll")] public static extern bool SetCursorPos(int x,int y);
 [DllImport("user32.dll")] public static extern void mouse_event(uint f,uint x,uint y,uint d,IntPtr e);
 public struct RECT{public int Left,Top,Right,Bottom;}
 public static IntPtr Found=IntPtr.Zero; public static string Match="";
 public static bool Cb(IntPtr h, IntPtr l){ if(IsWindowVisible(h)){ var sb=new StringBuilder(256); GetWindowText(h,sb,256); if(sb.ToString().Contains(Match)){ Found=h; return false; } } return true; }
 public static void Click(int x,int y){ SetCursorPos(x,y); System.Threading.Thread.Sleep(150); mouse_event(2,0,0,0,IntPtr.Zero); System.Threading.Thread.Sleep(80); mouse_event(4,0,0,0,IntPtr.Zero); System.Threading.Thread.Sleep(350);} }
"@
$cb=[WE+EnumProc]{ param($h,$l) [WE]::Cb($h,$l) }
function Find($m){ [WE]::Found=[IntPtr]::Zero; [WE]::Match=$m; [WE]::EnumWindows($cb,[IntPtr]::Zero)|Out-Null; return [WE]::Found }
function Title($m){ $h=Find $m; if($h -eq [IntPtr]::Zero){return $null}; $sb=New-Object System.Text.StringBuilder 256; [WE]::GetWindowText($h,$sb,256)|Out-Null; return $sb.ToString() }
function SolveOnce(){
  $h=Find "Select Bot"; if($h -eq [IntPtr]::Zero){ L 'no chooser'; return $false }
  [WE]::ShowWindow($h,9)|Out-Null; [WE]::BringWindowToTop($h)|Out-Null; [WE]::SetForegroundWindow($h)|Out-Null; Start-Sleep -Milliseconds 700
  $r=New-Object WE+RECT; [WE]::GetWindowRect($h,[ref]$r)|Out-Null; $W=$r.Right-$r.Left; $H=$r.Bottom-$r.Top
  L "chooser rect L=$($r.Left) T=$($r.Top) W=$W H=$H"
  [WE]::Click($r.Left+[int]($W*0.68),$r.Top+[int]($H*0.55)); Start-Sleep -Milliseconds 900   # open dropdown
  [WE]::Click($r.Left+[int]($W*0.30),$r.Top+136); Start-Sleep -Milliseconds 700               # 5.1 row (top item, y~T+136)
  [WE]::Click($r.Left+[int]($W*0.84),$r.Top+[int]($H*0.55)); Start-Sleep -Milliseconds 1600    # Select
  return ((Find "Select Bot") -eq [IntPtr]::Zero)
}
# current title?
$t=Title "Crawler App"
L "current title: $t"
if($t -and $t -match '5\.1 Auto Search'){ L 'already on 5.1'; return }
# wrong bot or chooser -> if main window wrong, kill+relaunch to get chooser
if($t -and $t -notmatch 'Select Bot'){
  L "wrong bot -> kill+relaunch"
  Get-Process BotsolApp,chrome,chromedriver -EA SilentlyContinue | Stop-Process -Force; Start-Sleep -Seconds 5
  Start-ScheduledTask -TaskName BotsolManualLaunch; Start-Sleep -Seconds 18
}
# chooser-solve REMOVED 2026-06-26: BotsolHwClick (robust UIA solver) owns the chooser now.
# cc4 only corrects a WRONG bot (kill+relaunch above); hwclick solves the relaunched chooser.
if((Find 'Select Bot') -ne [IntPtr]::Zero){ L 'chooser up -> BotsolHwClick owns it (no pixel-solve here)' }
Start-Sleep -Milliseconds 800
$t=Title "Crawler App"
L "after solve title: $t"
if($t -match '5\.1 Auto Search'){
  $wh=$null; foreach($e in (Get-Content C:\worker\orchestrator.env -EA SilentlyContinue)){ if($e -match '^\s*SLACK_WEBHOOK\s*=\s*(.+?)\s*$'){ $wh=$Matches[1] } }
  if($wh){ try{ Invoke-RestMethod -Uri $wh -Method Post -ContentType 'application/json' -TimeoutSec 12 -Body (@{text=":robot_face: Botsol bot-enforcer corrected wrong bot -> 5.1 Auto Search on $env:COMPUTERNAME."}|ConvertTo-Json -Compress)|Out-Null }catch{} }
}
$b=[System.Windows.Forms.SystemInformation]::VirtualScreen;$bmp=New-Object System.Drawing.Bitmap($b.Width,$b.Height);$g=[System.Drawing.Graphics]::FromImage($bmp);$g.CopyFromScreen($b.X,$b.Y,0,0,$bmp.Size);$bmp.Save("C:\worker\logs\cc4.png");$g.Dispose();$bmp.Dispose()
"=== done ===" | Add-Content $o
