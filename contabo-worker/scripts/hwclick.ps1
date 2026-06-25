# Self-contained chooser solver (no dependency on ChooserNav timing -> no race).
# HARD-GATED to the "Select Bot" window. Steps: dismiss stray modal -> commit combo -> click Select -> verify.
$log='C:\worker\logs\hwclick.txt'
function L($m){ "$((Get-Date -Format 'HH:mm:ss'))  $m" | Add-Content $log }
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
Add-Type -AssemblyName System.Windows.Forms
Add-Type -TypeDefinition @"
using System; using System.Runtime.InteropServices;
public class Hw {
  [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr h);
  [DllImport("user32.dll")] public static extern bool SetCursorPos(int x,int y);
  [DllImport("user32.dll")] public static extern void mouse_event(uint f,uint x,uint y,uint d,IntPtr e);
  public static void Click(int x,int y){ SetCursorPos(x,y); System.Threading.Thread.Sleep(120); mouse_event(0x0002,0,0,0,IntPtr.Zero); System.Threading.Thread.Sleep(60); mouse_event(0x0004,0,0,0,IntPtr.Zero); System.Threading.Thread.Sleep(250); }
}
"@
$TS=[System.Windows.Automation.TreeScope]; $AE=[System.Windows.Automation.AutomationElement]; $CT=[System.Windows.Automation.ControlType]
$root=$AE::RootElement
$cond=New-Object System.Windows.Automation.PropertyCondition($AE::ClassNameProperty,'WindowsForms10.Window.8.app.0.141b42a_r9_ad1')
function GetChooser(){ foreach($w in $root.FindAll($TS::Children,$cond)){ if($w.Current.Name -match 'Select Bot'){ return $w } }; return $null }
function FG($w){ [Hw]::SetForegroundWindow([IntPtr]$w.Current.NativeWindowHandle) | Out-Null; Start-Sleep -Milliseconds 350 }
function ClickEl($e){ $r=$e.Current.BoundingRectangle; [Hw]::Click([int]($r.X+$r.Width/2),[int]($r.Y+$r.Height/2)) }
$win=GetChooser
if(-not $win){ return }
L "--- solve chooser pid=$($win.Current.ProcessId) ---"
FG $win
# STEP 1: dismiss any stray modal ('Please select a value' etc.) via its OK
$all=$win.FindAll($TS::Descendants,[System.Windows.Automation.Condition]::TrueCondition)
$ok=$all | Where-Object { ("" + $_.Current.Name).Trim() -match '^OK$' } | Select-Object -First 1
if($ok){ L 'modal OK present -> dismiss'; ClickEl $ok; Start-Sleep -Milliseconds 400; $win=GetChooser; if(-not $win){ L 'no chooser after modal dismiss'; return }; FG $win }
# STEP 2: commit combo selection (self, not via ChooserNav)
$combo=$win.FindFirst($TS::Descendants,(New-Object System.Windows.Automation.PropertyCondition($AE::ControlTypeProperty,$CT::ComboBox)))
if($combo){
  # Select "Google Maps Crawler 5.1 Auto Search" BY NAME. The dropdown items are NOT combo
  # descendants -- they live in a separate ComboLBox popup under the desktop ROOT, so we expand
  # then search $root. 5.0 = manual bot (no keyword flow); the "Free *" bots scrape the wrong
  # site entirely (LinkedIn/Facebook). Wrong bot -> silent wedge. (fix 2026-06-22, supersedes 06-19)
  $picked=$false
  try{ $combo.SetFocus(); Start-Sleep -Milliseconds 200 }catch{}
  try{ $combo.GetCurrentPattern([System.Windows.Automation.ExpandCollapsePattern]::Pattern).Expand(); Start-Sleep -Milliseconds 700 }catch{}
  $tgt=$null
  foreach($it in $root.FindAll($TS::Subtree,(New-Object System.Windows.Automation.PropertyCondition($AE::ControlTypeProperty,$CT::ListItem)))){
    if($it.Current.Name -eq 'Google Maps Crawler 5.1 Auto Search'){ $tgt=$it; break }
  }
  if($tgt){
    try{ $tgt.GetCurrentPattern([System.Windows.Automation.SelectionItemPattern]::Pattern).Select(); $picked=$true; L "selected 5.1 by name via SelectionItem" }
    catch{ try{ $r=$tgt.Current.BoundingRectangle; [Hw]::Click([int]($r.X+$r.Width/2),[int]($r.Y+$r.Height/2)); $picked=$true; L "selected 5.1 via hw-click" }catch{ L "select51 err $($_.Exception.Message)" } }
  } else { L 'WARN: 5.1 item not found in dropdown popup' }
  Start-Sleep -Milliseconds 400
  try{ $combo.GetCurrentPattern([System.Windows.Automation.ExpandCollapsePattern]::Pattern).Collapse() }catch{}
  L "combo selected51=$picked"
}
# STEP 3: click Select
$win=GetChooser; if(-not $win){ L 'cleared before Select'; return }
FG $win
$sel=$win.FindAll($TS::Descendants,[System.Windows.Automation.Condition]::TrueCondition) | Where-Object { ("" + $_.Current.Name).Trim() -match '^[Ss]elect$' -and $_.Current.BoundingRectangle.Width -ge 25 -and $_.Current.BoundingRectangle.Width -le 260 } | Select-Object -First 1
if($sel){ $r=$sel.Current.BoundingRectangle; L ("click Select at {0},{1}" -f [int]($r.X+$r.Width/2),[int]($r.Y+$r.Height/2)); ClickEl $sel; Start-Sleep -Milliseconds 1200
  $still=GetChooser
  if($still){ L 'Select did not clear -> checking for new modal'; $a2=$still.FindAll($TS::Descendants,[System.Windows.Automation.Condition]::TrueCondition); $ok2=$a2|Where-Object{("" + $_.Current.Name).Trim() -match '^OK$'}|Select-Object -First 1; if($ok2){ L 'modal reappeared -> dismiss (combo not committing)'; ClickEl $ok2 } }
  L ("RESULT chooser present after = {0}" -f [bool](GetChooser)) }
else { L 'no Select target matched' }


