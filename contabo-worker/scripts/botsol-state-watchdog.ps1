# Botsol state snapshot — runs every 60s as scheduled task in interactive session.
# Captures full UI state to C:\worker\logs\botsol-state-snapshot.json so future
# sessions can read exactly what's on screen before taking action.

$OutPath = 'C:\worker\logs\botsol-state-snapshot.json'
$ErrorActionPreference = 'SilentlyContinue'

Add-Type -AssemblyName UIAutomationClient,UIAutomationTypes,WindowsBase

function Get-Snapshot {
    $snap = [ordered]@{
        ts = (Get-Date).ToString('o')
        botsol_processes = @()
        chrome_count = 0
        chromedriver_count = 0
        windows = @()
        dialogs = @()
        recommendation = ''
    }

    $procs = Get-Process -Name 'BotsolApp' -ErrorAction SilentlyContinue
    foreach ($p in $procs) {
        $snap.botsol_processes += [ordered]@{
            pid = $p.Id
            main_window_title = $p.MainWindowTitle
            start_time = $p.StartTime.ToString('o')
        }
    }
    $snap.chrome_count = (Get-Process -Name 'chrome' -ErrorAction SilentlyContinue).Count
    $snap.chromedriver_count = (Get-Process -Name 'chromedriver' -ErrorAction SilentlyContinue).Count

    $root = [System.Windows.Automation.AutomationElement]::RootElement
    $wins = $root.FindAll([System.Windows.Automation.TreeScope]::Children, [System.Windows.Automation.Condition]::TrueCondition)

    foreach ($w in $wins) {
        try {
            $name = $w.Current.Name
            $aid  = $w.Current.AutomationId
            $cls  = $w.Current.ClassName
            $isBotsol = $false
            foreach ($bp in $procs) { if ($w.Current.ProcessId -eq $bp.Id) { $isBotsol = $true; break } }
            if (-not $isBotsol) { continue }

            $winInfo = [ordered]@{
                name = $name
                aid = $aid
                class = $cls
            }

            # Capture button states + nested dialogs
            $kids = $w.FindAll([System.Windows.Automation.TreeScope]::Descendants, [System.Windows.Automation.Condition]::TrueCondition)
            $buttons = @{}
            $nestedDialogs = @()
            $editValues = @{}
            $combos = @()
            foreach ($k in $kids) {
                try {
                    $kAid = $k.Current.AutomationId
                    $kName = $k.Current.Name
                    $kClass = $k.Current.ClassName
                    $kEnabled = $k.Current.IsEnabled
                    $kType = $k.Current.ControlType.LocalizedControlType
                    if ($kType -eq 'button' -and ($kAid -in @('btnStart','btnStop','btnExport','btndelete') -or $kName -in @('Select','Yes','No','OK','Cancel','Save','Open'))) {
                        $key = if ($kAid) { $kAid } else { $kName }
                        $buttons[$key] = [ordered]@{ name = $kName; enabled = $kEnabled }
                    }
                    if ($kClass -eq '#32770' -or ($kAid -like 'frm*') -or $kAid -eq 'InputInteger' -or $kAid -eq 'InputString') {
                        $nestedDialogs += [ordered]@{
                            name = $kName
                            aid = $kAid
                            class = $kClass
                            enabled = $kEnabled
                        }
                    }
                    if ($kType -eq 'edit' -and $kEnabled) {
                        try {
                            $vp = $k.GetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern)
                            $editValues[$kAid] = $vp.Current.Value
                        } catch {}
                    }
                    if ($kType -eq 'combo box' -and $kEnabled) {
                        $combos += [ordered]@{ aid = $kAid; name = $kName }
                    }
                } catch {}
            }
            $winInfo.buttons = $buttons
            $winInfo.nested_dialogs = $nestedDialogs
            $winInfo.edit_values = $editValues
            $winInfo.combos = $combos
            $snap.windows += $winInfo
        } catch {}
    }

    # Recommendation logic
    if ($snap.botsol_processes.Count -eq 0) {
        $snap.recommendation = 'BotsolApp not running. Launch via desktop shortcut.'
    } elseif ($snap.windows.Count -eq 0) {
        $snap.recommendation = 'BotsolApp PID exists but no UIA window. Possibly hidden or not yet rendered.'
    } else {
        $w = $snap.windows[0]
        if ($w.name -match 'Select Bot') {
            $snap.recommendation = 'Select Bot dialog open. Click button name="Select" via UIA InvokePattern (NOT the license-key link).'
        } elseif ($w.nested_dialogs.Count -gt 0) {
            $d = $w.nested_dialogs[0]
            if ($d.aid -eq 'frmBoolInput') {
                $snap.recommendation = "frmBoolInput up ('$($d.name)'). Click Yes."
            } elseif ($d.aid -eq 'InputInteger' -or $d.aid -eq 'frmInput') {
                $snap.recommendation = "Numeric prompt ($($d.aid)) up ('$($d.name)'). Read label to choose RESULT_CAP=400 or KEYWORD_LIMIT=5; type + click OK/Continue."
            } elseif ($d.aid -eq 'frmSelectFile') {
                $snap.recommendation = 'File picker (frmSelectFile) up. Type keyword .txt path + click OK.'
            } elseif ($d.class -eq '#32770') {
                if ($d.name -match 'Confirm Delete') {
                    $snap.recommendation = 'Confirm Delete!! popup. Click Yes.'
                } elseif ($d.name -eq '') {
                    $snap.recommendation = '#32770 with empty name (likely error or save dialog). Inspect buttons inside.'
                } else {
                    $snap.recommendation = "#32770 popup '$($d.name)'. Inspect buttons."
                }
            } else {
                $snap.recommendation = "Modal $($d.aid) up. Inspect."
            }
        } else {
            $btn = $w.buttons
            $startEn = if ($btn.btnStart) { $btn.btnStart.enabled } else { $false }
            $stopEn  = if ($btn.btnStop)  { $btn.btnStop.enabled }  else { $false }
            $exportEn= if ($btn.btnExport){ $btn.btnExport.enabled }else { $false }
            $deleteEn= if ($btn.btndelete){ $btn.btndelete.enabled }else { $false }
            if ($stopEn -and -not $exportEn) {
                $snap.recommendation = 'RUNNING phase. Botsol is scraping. Let it run.'
            } elseif ($exportEn -and $startEn) {
                $snap.recommendation = 'DONE phase. BotsolAgent should auto-export then auto-Delete then auto-Start next.'
            } elseif ($startEn -and -not $exportEn -and -not $deleteEn) {
                $snap.recommendation = 'IDLE phase (clean). Trigger BotsolAgent — it will pick next file from queue and Start.'
            } else {
                $snap.recommendation = "AMBIGUOUS phase (start=$startEn stop=$stopEn export=$exportEn delete=$deleteEn). Likely modal not detected by snapshot."
            }
        }
    }

    return $snap
}

try {
    $snap = Get-Snapshot
    $snap | ConvertTo-Json -Depth 8 | Set-Content -Path $OutPath -Encoding UTF8
} catch {
    @{ ts = (Get-Date).ToString('o'); error = $_.Exception.Message } | ConvertTo-Json | Set-Content -Path $OutPath -Encoding UTF8
}
