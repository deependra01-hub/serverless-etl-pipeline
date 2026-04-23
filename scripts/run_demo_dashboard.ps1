$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root ".venv\Scripts\python.exe"

if (-not (Test-Path $python)) {
    throw "Virtual environment not found at $python"
}

& $python "$root\scripts\offline_demo.py" --events 1000 --invalid-every 25

$env:DEMO_DATA_DIR = "$root\tmp\offline-demo"

$port = 8501
while (Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue) {
    $port++
}

Write-Host "Starting dashboard at http://127.0.0.1:$port"
& $python -m streamlit run "$root\dashboard\app\main.py" --server.headless true --server.address 127.0.0.1 --server.port $port
