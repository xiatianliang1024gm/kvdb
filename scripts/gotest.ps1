# 在本机跑 go 命令的辅助脚本：把 stdout/stderr 统一写到日志文件，避免 PowerShell 不回显。
# 用法：pwsh -NoProfile -File scripts\gotest.ps1 test ./... -v
# 引擎固定用 PowerShell 7（pwsh）。若被 5.1（powershell.exe）启动，脚本会自动切到 7 重跑。
# 结果落在仓库根的 _t.log
param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Args)

# ── 引擎规则（重要）──────────────────────────────────────────────
# 本机同时装着 PowerShell 7（C:\Program Files\PowerShell\7\pwsh.exe）和 Windows PowerShell 5.1
# （C:\WINDOWS\System32\WindowsPowerShell\v1.0\powershell.exe）。**脚本一律用 pwsh 7 跑**：
# 5.1 会把无 BOM 的 UTF-8 文件按 GBK 解码，中文注释末尾的字节会吃掉换行，
# 轻则整行代码被吞成注释（静默失效），重则报 `表达式或语句中包含意外的标记`。
# 下面这段保证即使被 5.1 启动，也会自动切到 7 重跑一遍，不依赖使用者记得用 pwsh。
if ($PSVersionTable.PSEdition -ne "Core" -and $PSCommandPath) {
  $pwsh = (Get-Command pwsh -ErrorAction SilentlyContinue).Source
  if ($pwsh) {
    & $pwsh -NoProfile -File $PSCommandPath @Args
    exit $LASTEXITCODE
  }
}
# ────────────────────────────────────────────────────────────────

$ErrorActionPreference = "Continue"
# Go 输出是 UTF-8，不设置的话 PowerShell 会按 GBK 解码导致中文乱码。
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$root = Split-Path -Parent $PSScriptRoot
$log = Join-Path $root "_t.log"
Set-Content -Encoding utf8 -Path $log -Value ("=== go " + ($Args -join " ") + "   [engine " + $PSVersionTable.PSEdition + " " + $PSVersionTable.PSVersion + "]")

# -race 需要 cgo（本机 gcc 在 C:\msys64\ucrt64\bin）。但 WorkBuddy 的 PowerShell 工具里
# $env:PATH 的改动传不到子进程，cgo 必然报 `cgo.exe: exit status 2` —— 这种情况请走
# scripts/gotest-race.sh（Bash）。用户自己开真终端跑 PowerShell 时不受此限制。
if ($Args -contains "-race") {
  Add-Content -Encoding utf8 -Path $log -Value "[warn] -race 依赖 cgo。若在 WorkBuddy 里执行，请改用 Bash: source scripts/gotest-race.sh"
}

# go.exe 位置：优先 PATH，找不到再退回默认安装路径。
$go = (Get-Command go -ErrorAction SilentlyContinue).Source
if (-not $go) { $go = "C:\Program Files\Go\bin\go.exe" }

Push-Location $root
& $go @Args *>&1 | Add-Content -Encoding utf8 -Path $log
$code = $LASTEXITCODE
Pop-Location
Add-Content -Encoding utf8 -Path $log -Value "exit=$code"
exit $code
