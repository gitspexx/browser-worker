$o='C:\worker\logs\chooserinspect.txt'
"=== $(Get-Date -Format 'HH:mm:ss') chooser deep-inspect ===" | Set-Content $o
Add-Type -AssemblyName UIAutomationClient; Add-Type -AssemblyName UIAutomationTypes
$TS=[System.Windows.Automation.TreeScope]; $AE=[System.Windows.Automation.AutomationElement]; $CT=[System.Windows.Automation.ControlType]
$root=$AE::RootElement
function Pat($e,$p){ try{ $e.GetCurrentPattern($p)|Out-Null; return $true }catch{ return $false } }
$win=$null
foreach($w in $root.FindAll($TS::Children,[System.Windows.Automation.Condition]::TrueCondition)){ if($w.Current.Name -match 'Select Bot'){ $win=$w; break } }
if(-not $win){ "NO chooser window found" | Add-Content $o; return }
("window: name='{0}' class='{1}' pid={2}" -f $win.Current.Name,$win.Current.ClassName,$win.Current.ProcessId) | Add-Content $o

"--- all descendants (type | name | autoId | rect | patterns) ---" | Add-Content $o
foreach($e in $win.FindAll($TS::Descendants,[System.Windows.Automation.Condition]::TrueCondition)){
  $c=$e.Current; $r=$c.BoundingRectangle
  $pats=@()
  if(Pat $e ([System.Windows.Automation.ValuePattern]::Pattern)){$pats+='Value'}
  if(Pat $e ([System.Windows.Automation.ExpandCollapsePattern]::Pattern)){$pats+='Expand'}
  if(Pat $e ([System.Windows.Automation.SelectionItemPattern]::Pattern)){$pats+='SelItem'}
  if(Pat $e ([System.Windows.Automation.InvokePattern]::Pattern)){$pats+='Invoke'}
  if(Pat $e ([System.Windows.Automation.SelectionPattern]::Pattern)){$pats+='Selection'}
  ("  [{0}] '{1}' aid='{2}' rect={3},{4},{5}x{6} pat={7}" -f $c.ControlType.ProgrammaticName.Replace('ControlType.',''),$c.Name,$c.AutomationId,[int]$r.X,[int]$r.Y,[int]$r.Width,[int]$r.Height,($pats -join '+')) | Add-Content $o
}

"--- combo detail ---" | Add-Content $o
$combo=$win.FindFirst($TS::Descendants,(New-Object System.Windows.Automation.PropertyCondition($AE::ControlTypeProperty,$CT::ComboBox)))
if($combo){
  if(Pat $combo ([System.Windows.Automation.ValuePattern]::Pattern)){
    $vp=$combo.GetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern)
    ("combo ValuePattern: value='{0}' readonly={1}" -f $vp.Current.Value,$vp.Current.IsReadOnly) | Add-Content $o
  } else { "combo: NO ValuePattern" | Add-Content $o }
  try{ $combo.GetCurrentPattern([System.Windows.Automation.ExpandCollapsePattern]::Pattern).Expand(); Start-Sleep -Milliseconds 700; "combo expanded" | Add-Content $o }catch{ ("expand fail: {0}" -f $_.Exception.Message) | Add-Content $o }
  "--- dropdown ListItems under ROOT (the popup is a sibling, not a combo child) ---" | Add-Content $o
  $i=0
  foreach($it in $root.FindAll($TS::Subtree,(New-Object System.Windows.Automation.PropertyCondition($AE::ControlTypeProperty,$CT::ListItem)))){
    $r=$it.Current.BoundingRectangle
    ("  item[{0}] '{1}' rect={2},{3},{4}x{5} selItem={6}" -f $i,$it.Current.Name,[int]$r.X,[int]$r.Y,[int]$r.Width,[int]$r.Height,(Pat $it ([System.Windows.Automation.SelectionItemPattern]::Pattern))) | Add-Content $o
    $i++
  }
  try{ $combo.GetCurrentPattern([System.Windows.Automation.ExpandCollapsePattern]::Pattern).Collapse() }catch{}
} else { "NO combobox" | Add-Content $o }
"=== done ===" | Add-Content $o
