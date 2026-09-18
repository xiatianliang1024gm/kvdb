#!/usr/bin/env bash
# 用 MSYS2 UCRT64 的 gcc 跑 go 的 -race / cgo 测试。
#
# 为什么单独搞一个 bash 脚本（而不是塞进 gotest.ps1）：
#   PowerShell 里改 $env:PATH 不会传给子进程，cgo 阶段必然失败
#   （即使把 CC 设成 gcc 绝对路径也没用，报 `cgo.exe: exit status 2`）。
#   只有 Bash 里 export PATH 才真正生效 —— 所以 -race 只能在 Bash 里跑。
#
# 用法：
#   bash scripts/gotest-race.sh                      # 等价于 go test -race ./...
#   bash scripts/gotest-race.sh ./internal/wal/...    # 只测某个包（自动补 test -race）
#   bash scripts/gotest-race.sh test -race ./... -v   # 完整自定义
# 输出同时打到屏幕和 _race.log。
#
# 注意：在 WorkBuddy 的 Bash 工具里要改用 source 调用 ——
#   cd /c/Users/summer/repos/kvdb && source scripts/gotest-race.sh
# 因为嵌套再起一个 bash 会被沙箱拦下（它会去探测 wsl.exe，命中程序黑名单）。
# 在自己本机的 Git Bash / MSYS2 终端里，`bash scripts/gotest-race.sh` 正常可用。
set -uo pipefail

GCC_DIR="/c/msys64/ucrt64/bin"
GCC_EXE="$GCC_DIR/gcc.exe"

if [ ! -x "$GCC_EXE" ]; then
  echo "ERROR: 没找到 gcc: $GCC_EXE" >&2
  echo "MSYS2 里装 UCRT64 toolchain 可解决: pacman -S mingw-w64-ucrt-x86_64-gcc" >&2
  exit 1
fi

# 关键三步：gcc 目录进 PATH、开 cgo、指定 CC
export PATH="$GCC_DIR:$PATH"
export CGO_ENABLED=1
export CC=gcc

GO="$(command -v go 2>/dev/null || true)"
if [ -z "$GO" ]; then
  GO="/c/Program Files/Go/bin/go.exe"
fi

# 脚本所在目录的上一级 = 仓库根（用参数展开取，本机 coreutils 的 dirname 不一定可用）
SCRIPT_DIR="${BASH_SOURCE[0]%/*}"
if [ "$SCRIPT_DIR" = "${BASH_SOURCE[0]}" ]; then SCRIPT_DIR="."; fi
ROOT="$SCRIPT_DIR/.."
cd "$ROOT" || exit 1
ROOT="$(pwd)"
LOG="$ROOT/_race.log"

if [ "$#" -eq 0 ]; then
  set -- test -race ./...
else
  # 首个参数像包路径（./... 或 /abs/path）时，自动补 test -race
  case "$1" in
    .*|/*) set -- test -race "$@" ;;
  esac
fi

echo "=== go $* (CGO_ENABLED=1, CC=$CC) ==="
"$GO" "$@" > "$LOG" 2>&1
code=$?

# read 是 bash 内置命令，不依赖 cat（本机 bash 的 coreutils 可能是缺的）
while IFS= read -r line; do printf '%s\n' "$line"; done < "$LOG"
echo "exit=$code  (完整输出: _race.log)"

# 被 source 时用 return（否则会把调用方的 shell 一起关掉），直接执行时用 exit
if [ "${BASH_SOURCE[0]}" != "$0" ]; then return $code; else exit $code; fi
