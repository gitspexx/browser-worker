$log='C:\worker\logs\navselect.txt'
function L($m){ "$((Get-Date -Format 'HH:mm:ss'))  $m" | Add-Content $log }
"start $(Get-Date -Format 'HH:mm:ss')" | Set-Content $log
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
Add-Type -AssemblyName System.Windows.Forms
Add-Type -TypeDefinition @"
using System; using System.Runtime.InteropServices;
public class W { [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr h); }
"@
$TS=[System.Windows.Automation.TreeScope]; $AE=[System.Windows.Automation.AutomationElement]; $CT=[System.Windows.Automation.ControlType]
$root=$AE::RootElement
$cond=New-Object System.Windows.Automation.PropertyCondition($AE::ClassNameProperty,'WindowsForms10.Window.8.app.0.141b42a_r9_ad1')
$marker='C:\worker\chooser-alerted.marker'
function GetChooser(){ foreach($w in $root.FindAll($TS::Children,$cond)){ if($w.Current.Name -match 'Select Bot'){ return $w } }; return $null }
$win=GetChooser
if(-not $win){ Remove-Item $marker -EA SilentlyContinue; L 'no chooser - cleared marker'; return }
$cpid=$win.Current.ProcessId
L "chooser pid=$cpid"
function ComboVal($c){ try { return $c.GetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern).Current.Value } catch { return '' } }
# pre-select the (only licensed) bot via keyboard so the human only needs one click
[W]::SetForegroundWindow([IntPtr]$win.Current.NativeWindowHandle) | Out-Null
Start-Sleep -Milliseconds 400
$combo=$win.FindFirst($TS::Descendants,(New-Object System.Windows.Automation.PropertyCondition($AE::ControlTypeProperty,$CT::ComboBox)))
if($combo){
  try { $combo.SetFocus() } catch {}
  Start-Sleep -Milliseconds 250
  # Select "Google Maps Crawler 5.1 Auto Search" BY NAME from the ComboLBox popup under ROOT
  # (dropdown items are NOT combo descendants). Wrong bot (5.0 / Free LinkedIn etc) -> silent wedge.
  # (fix 2026-06-22, supersedes the broken 06-19 combo-descendant search)
  $picked=$false
  try{ $combo.SetFocus(); Start-Sleep -Milliseconds 150 }catch{}
  try{ $combo.GetCurrentPattern([System.Windows.Automation.ExpandCollapsePattern]::Pattern).Expand(); Start-Sleep -Milliseconds 700 }catch{}
  foreach($it in $root.FindAll($TS::Subtree,(New-Object System.Windows.Automation.PropertyCondition($AE::ControlTypeProperty,$CT::ListItem)))){
    if($it.Current.Name -eq 'Google Maps Crawler 5.1 Auto Search'){ try{ $it.GetCurrentPattern([System.Windows.Automation.SelectionItemPattern]::Pattern).Select(); $picked=$true }catch{}; break }
  }
  Start-Sleep -Milliseconds 300
  try{ $combo.GetCurrentPattern([System.Windows.Automation.ExpandCollapsePattern]::Pattern).Collapse() }catch{}
  L "bot selected (5.1 by name=$picked)"
}
# Slack alert once per chooser appearance (keyed on pid)
$wh=$null
foreach($ln in (Get-Content 'C:\worker\orchestrator.env' -EA SilentlyContinue)){ if($ln -match '^\s*SLACK_WEBHOOK\s*=\s*(.+?)\s*$'){ $wh=$Matches[1] } }
$prev=if(Test-Path $marker){ (Get-Content $marker -Raw).Trim() } else { '' }
if($prev -ne "$cpid"){
  if($wh){
    $msg=":warning: *Botsol paused on the 'Select Bot' screen* on $env:COMPUTERNAME. The Google Maps bot is *pre-selected* - RDP in and click *Select* to resume scraping. (BotsolApp rejects automated clicks on that one button.)"
    try { Invoke-RestMethod -Uri $wh -Method Post -Body (@{text=$msg}|ConvertTo-Json -Compress) -ContentType 'application/json' -TimeoutSec 15 | Out-Null; L 'slack alert sent' } catch { L "slack fail: $($_.Exception.Message)" }
  }
  Set-Content $marker "$cpid"
} else { L 'already alerted for this chooser pid' }
L 'done'


