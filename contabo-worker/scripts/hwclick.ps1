# Robust "Select Bot" chooser solver. HARD-GATED to the chooser window.
# Past wedges had two causes, both fixed here:
#  1. cc4 used GUESSED pixel coords (Top+136) -> missed when the window moved/resized.
#  2. hwclick used SelectionItem.Select which HIGHLIGHTS but does NOT commit the WinForms
#     combo -> Select hit an empty combo -> "please select a value" modal -> wedge.
# Fix: locate the dropdown item + Select control by UIA (real screen rects), commit with a
# REAL mouse click on item[0] (sets SelectedIndex, like cc4 but on the true rect), VERIFY the
# combo value == target BEFORE clicking Select (gate against the empty-combo wedge), and click
# the Select control (it's a Pane, no Invoke -> must be a real mouse click) by its UIA rect.
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
  public static void Click(int x,int y){ SetCursorPos(x,y); System.Threading.Thread.Sleep(120); mouse_event(0x0002,0,0,0,IntPtr.Zero); System.Threading.Thread.Sleep(70); mouse_event(0x0004,0,0,0,IntPtr.Zero); System.Threading.Thread.Sleep(250);} }
"@
$TS=[System.Windows.Automation.TreeScope]; $AE=[System.Windows.Automation.AutomationElement]; $CT=[System.Windows.Automation.ControlType]
$root=$AE::RootElement
$TARGET='Google Maps Crawler 5.1 Auto Search'
$cond=New-Object System.Windows.Automation.PropertyCondition($AE::ClassNameProperty,'WindowsForms10.Window.8.app.0.141b42a_r9_ad1')
function GetChooser(){ foreach($w in $root.FindAll($TS::Children,$cond)){ if($w.Current.Name -match 'Select Bot'){ return $w } }; return $null }
function FG($w){ try{ [Hw]::SetForegroundWindow([IntPtr]$w.Current.NativeWindowHandle)|Out-Null }catch{}; Start-Sleep -Milliseconds 350 }
function ClickRect($r){ [Hw]::Click([int]($r.X+$r.Width/2),[int]($r.Y+$r.Height/2)) }
function ComboVal($c){ try{ return $c.GetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern).Current.Value }catch{ return '' } }
function FindItem(){ foreach($i in $root.FindAll($TS::Subtree,(New-Object System.Windows.Automation.PropertyCondition($AE::ControlTypeProperty,$CT::ListItem)))){ if($i.Current.Name -eq $TARGET){ return $i } }; return $null }

$win=GetChooser
if(-not $win){ return }                     # gate: only act on a real chooser
L "--- solve chooser pid=$($win.Current.ProcessId) ---"
FG $win

# dismiss any stray 'please select a value' modal (OK) first
$ok = $win.FindAll($TS::Descendants,[System.Windows.Automation.Condition]::TrueCondition) | Where-Object { ("$($_.Current.Name)").Trim() -eq 'OK' } | Select-Object -First 1
if($ok){ L 'stray OK modal -> dismiss'; ClickRect $ok.Current.BoundingRectangle; Start-Sleep -Milliseconds 400; $win=GetChooser; if(-not $win){ return }; FG $win }

$combo=$win.FindFirst($TS::Descendants,(New-Object System.Windows.Automation.PropertyCondition($AE::ControlTypeProperty,$CT::ComboBox)))
if(-not $combo){ L 'NO combobox'; return }
function OpenPopup($w,$c){
  # Render the ComboLBox popup with a REAL click on the dropdown 'Open' arrow.
  # ExpandCollapsePattern alone often sets state WITHOUT rendering the popup that holds
  # the ListItems, so FindItem returns null. A real click reliably renders it.
  $arrow = $w.FindAll($TS::Descendants,[System.Windows.Automation.Condition]::TrueCondition) | Where-Object { $_.Current.ControlType -eq $CT::Button -and ("$($_.Current.Name)").Trim() -eq 'Open' } | Select-Object -First 1
  if($arrow){ ClickRect $arrow.Current.BoundingRectangle } else { try{ $c.GetCurrentPattern([System.Windows.Automation.ExpandCollapsePattern]::Pattern).Expand() }catch{} }
  Start-Sleep -Milliseconds 700
}

# COMMIT: open dropdown (real click) + REAL-click item[0] '5.1' by its UIA rect -> sets
# SelectedIndex (the thing Select actually reads). SetValue is last-resort (text-only,
# leaves SelectedIndex unset -> Select wedge), kept solely as a fallback. Up to 3 tries.
for($t=1; $t -le 3 -and (ComboVal $combo) -ne $TARGET; $t++){
  try{ $combo.SetFocus() }catch{}
  OpenPopup $win $combo
  $it=FindItem
  if($it){ ClickRect $it.Current.BoundingRectangle; Start-Sleep -Milliseconds 450; L "try $t real-click item -> combo='$(ComboVal $combo)'" }
  elseif($t -eq 3){ try{ $combo.GetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern).SetValue($TARGET); Start-Sleep -Milliseconds 300; L "try $t last-resort SetValue -> combo='$(ComboVal $combo)'" }catch{ L "try $t no item; SetValue err" } }
  else { L "try $t item not in popup yet; retry" }
  try{ $combo.GetCurrentPattern([System.Windows.Automation.ExpandCollapsePattern]::Pattern).Collapse() }catch{}
}

$val=ComboVal $combo
if($val -ne $TARGET){ L "ABORT: combo not committed (val='$val') -> NOT clicking Select (avoids wedge)"; return }
L "combo committed = '$val'"

# click Select (Pane, no Invoke -> real mouse click on its UIA rect). Up to 3 tries.
$win=GetChooser; if(-not $win){ L 'chooser cleared before Select (already done)'; return }; FG $win
function FindSelect($w){ $w.FindAll($TS::Descendants,[System.Windows.Automation.Condition]::TrueCondition) | Where-Object { ("$($_.Current.Name)").Trim() -eq 'Select' -and $_.Current.BoundingRectangle.Width -ge 40 -and $_.Current.BoundingRectangle.Width -le 220 } | Select-Object -First 1 }
$sel=FindSelect $win
if(-not $sel){ L 'no Select control matched'; return }
for($s=1; $s -le 3 -and (GetChooser); $s++){
  ClickRect $sel.Current.BoundingRectangle
  Start-Sleep -Milliseconds 1300
  L "Select click $s -> chooser present=$([bool](GetChooser))"
  $w2=GetChooser
  if($w2){
    $ok2=$w2.FindAll($TS::Descendants,[System.Windows.Automation.Condition]::TrueCondition) | Where-Object { ("$($_.Current.Name)").Trim() -eq 'OK' } | Select-Object -First 1
    if($ok2){ L 'modal reappeared -> dismiss + recommit'; ClickRect $ok2.Current.BoundingRectangle; Start-Sleep -Milliseconds 300; FG $w2; $c2=$w2.FindFirst($TS::Descendants,(New-Object System.Windows.Automation.PropertyCondition($AE::ControlTypeProperty,$CT::ComboBox))); if($c2){ try{ $c2.GetCurrentPattern([System.Windows.Automation.ExpandCollapsePattern]::Pattern).Expand(); Start-Sleep -Milliseconds 500 }catch{}; $it2=FindItem; if($it2){ ClickRect $it2.Current.BoundingRectangle; Start-Sleep -Milliseconds 300 }; try{ $c2.GetCurrentPattern([System.Windows.Automation.ExpandCollapsePattern]::Pattern).Collapse() }catch{} } }
    $sel=FindSelect $w2
    if($sel){ FG $w2 }
  }
}
L "RESULT chooser present after = $([bool](GetChooser))"
