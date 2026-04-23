$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$python = Join-Path $root ".venv\Scripts\python.exe"

if (-not (Test-Path $python)) {
    python -m venv .venv
}

if (-not (Test-Path $python)) {
    throw "Virtual environment creation failed at $python"
}

& $python -m pip install --upgrade pip
& $python -m pip install pytest jsonschema pyspark streamlit pandas deltalake boto3 confluent-kafka kubernetes prometheus-client pyyaml orjson pydantic delta-spark fastavro

Write-Host "Local environment is ready at $root\.venv"
