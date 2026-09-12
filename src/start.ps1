# ============================================================
# 一键启动整套 battleworld 环境：网关 + 3 个物理节点
# 用法（PowerShell）:
#   .\start.ps1
# 可选参数:
#   .\start.ps1 -DbPassword "你的密码"     # 覆盖默认数据库密码
#   .\start.ps1 -NoBuild                   # 跳过 go build，直接跑已编译的二进制
# ============================================================
param(
    [string]$DbPassword = "Wu050601&&",
    [switch]$NoBuild
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# ---------- 连接配置（数据库 / Redis 均可在此改） ----------
$env:BW_DB_PASSWORD  = $DbPassword
# $env:BW_DB_HOST      = "172.31.50.252"
# $env:BW_DB_PORT      = "5432"
# $env:BW_DB_USER      = "postgres"
# $env:BW_DB_NAME      = "battleworld"
# $env:BW_REDIS_ADDR   = "172.31.50.252:6379"
# $env:BW_REDIS_PASSWORD = ""

# ---------- 1. 编译（可选） ----------
if (-not $NoBuild) {
    Write-Host ">>> 编译中..." -ForegroundColor Cyan
    go build ./...
    if ($LASTEXITCODE -ne 0) { Write-Error "编译失败"; exit 1 }
}

# ---------- 2. 启动网关（独立窗口） ----------
Write-Host ">>> 启动网关 (127.0.0.1:9310)..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PSScriptRoot'; `$env:BW_DB_PASSWORD='$DbPassword'; go run ./cmd/server"

# ---------- 3. 启动六个物理节点（每张地图一主一备候选，各独立窗口） ----------
Start-Sleep -Seconds 1

$nodes = @(
    @{ id="node-a"; map="green"; addr="127.0.0.1:9311" },
    @{ id="node-b"; map="green"; addr="127.0.0.1:9312" },
    @{ id="node-c"; map="cave";  addr="127.0.0.1:9313" },
    @{ id="node-d"; map="cave";  addr="127.0.0.1:9314" },
    @{ id="node-e"; map="ruins"; addr="127.0.0.1:9315" },
    @{ id="node-f"; map="ruins"; addr="127.0.0.1:9316" }
)

foreach ($n in $nodes) {
    Write-Host ">>> 启动节点 $($n.id) ($($n.addr))..." -ForegroundColor Cyan
    $cmd = "cd '$PSScriptRoot'; `$env:BW_DB_PASSWORD='$DbPassword'; go run ./cmd/node -id '$($n.id)' -map '$($n.map)' -addr '$($n.addr)'"
    Start-Process powershell -ArgumentList "-NoExit", "-Command", $cmd
    Start-Sleep -Milliseconds 500
}

Write-Host ""
Write-Host "==============================================" -ForegroundColor Green
Write-Host "全部启动完成。" -ForegroundColor Green
Write-Host "  网关:   127.0.0.1:9310" -ForegroundColor Green
Write-Host "  节点:   node-a..node-f (9311..9316；每张地图两个候选)" -ForegroundColor Green
Write-Host "  数据库: $env:BW_DB_PASSWORD 已注入 (host 172.31.50.252)" -ForegroundColor Green
Write-Host "==============================================" -ForegroundColor Green
