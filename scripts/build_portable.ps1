#Requires -Version 5.1
<#
  Sestavi slozku runtime\python s Windows embeddable Python + pip + requirements.txt.
  Spoustet z pocitace s pristupem k internetu (nebo s predpripravenou slozkou wheels\).

  Pouziti:
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\build_portable.ps1
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\build_portable.ps1 -Force
#>
param(
    [switch]$Force
)

$ErrorActionPreference = "Stop"

# TLS 1.2 (starsi Windows)
try {
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
} catch {}

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$RuntimeDir = Join-Path $ProjectRoot "runtime\python"
$ReqFile = Join-Path $ProjectRoot "requirements.txt"
$WheelsDir = Join-Path $ProjectRoot "wheels"
$EmbedVersion = "3.11.9"
$EmbedZipName = "python-$EmbedVersion-embed-amd64.zip"
$EmbedUrl = "https://www.python.org/ftp/python/$EmbedVersion/$EmbedZipName"
$GetPipUrl = "https://bootstrap.pypa.io/get-pip.py"

function Write-Step($msg) {
    Write-Host ""
    Write-Host "=== $msg ===" -ForegroundColor Cyan
}

if (-not (Test-Path $ReqFile)) {
    Write-Error "Nenalezen soubor: $ReqFile"
    exit 1
}

if (-not $Force) {
    $pyExe = Join-Path $RuntimeDir "python.exe"
    if (Test-Path $pyExe) {
        try {
            & $pyExe -c "import streamlit, pandas; print('OK')" 2>$null
            if ($LASTEXITCODE -eq 0) {
                Write-Host "runtime\python uz obsahuje funkcní instalaci. Pro preinstalaci spustte s -Force." -ForegroundColor Yellow
                exit 0
            }
        } catch {}
    }
}

Write-Step "Adresar runtime"
New-Item -ItemType Directory -Force -Path $RuntimeDir | Out-Null

$pyExe = Join-Path $RuntimeDir "python.exe"
if ((-not (Test-Path $pyExe)) -or $Force) {
    if ($Force -and (Test-Path $RuntimeDir)) {
        Write-Host "Force: mazam obsah $RuntimeDir ..."
        Remove-Item -Recurse -Force $RuntimeDir
        New-Item -ItemType Directory -Force -Path $RuntimeDir | Out-Null
    }
    Write-Step "Stahuji embeddable Python $EmbedVersion (amd64)"
    $zipPath = Join-Path $env:TEMP $EmbedZipName
    Invoke-WebRequest -Uri $EmbedUrl -OutFile $zipPath -UseBasicParsing
    Write-Host "Rozbaluji do $RuntimeDir ..."
    Expand-Archive -Path $zipPath -DestinationPath $RuntimeDir -Force
    Remove-Item $zipPath -ErrorAction SilentlyContinue
}

if (-not (Test-Path $pyExe)) {
    Write-Error "Chybi $pyExe"
    exit 1
}

Write-Step "Povoleni site-packages (python*._pth)"
$pthFiles = Get-ChildItem -Path $RuntimeDir -Filter "python*._pth" | Select-Object -First 1
if (-not $pthFiles) {
    Write-Error "Nenalezen soubor python*._pth v $RuntimeDir"
    exit 1
}
$pthPath = $pthFiles.FullName
$pth = Get-Content -Path $pthPath -Raw
$pth = $pth -replace "#import site", "import site"
if ($pth -notmatch "(?m)^import site\s*$") {
    $pth = $pth.TrimEnd() + "`r`nimport site`r`n"
}
[System.IO.File]::WriteAllText($pthPath, $pth)

Write-Step "pip (get-pip.py)"
$getPip = Join-Path $RuntimeDir "get-pip.py"
Invoke-WebRequest -Uri $GetPipUrl -OutFile $getPip -UseBasicParsing
& $pyExe $getPip --no-warn-script-location
if ($LASTEXITCODE -ne 0) {
    Write-Error "get-pip.py selhal."
    exit 1
}
Remove-Item $getPip -ErrorAction SilentlyContinue

Write-Step "Instalace zavislosti z requirements.txt"
$pipArgs = @("-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel")
& $pyExe @pipArgs
if ($LASTEXITCODE -ne 0) {
    Write-Error "Aktualizace pip selhala."
    exit 1
}

if ((Test-Path $WheelsDir) -and (Get-ChildItem -Path $WheelsDir -File -ErrorAction SilentlyContinue | Select-Object -First 1)) {
    Write-Host "Pouzivam lokalni wheels: $WheelsDir"
    & $pyExe -m pip install --no-index --find-links $WheelsDir -r $ReqFile
} else {
    Write-Host "Stahuji balicky z PyPI (potrebuje internet nebo pripravte slozku wheels\ - viz docs)."
    & $pyExe -m pip install --default-timeout=120 --retries 10 -r $ReqFile
}
if ($LASTEXITCODE -ne 0) {
    Write-Error "pip install selhal."
    exit 1
}

Write-Step "Kontrola"
& $pyExe -c "import streamlit, pandas, plotly; print('Import OK')"
if ($LASTEXITCODE -ne 0) {
    Write-Error "Kontrola importu selhala."
    exit 1
}

Write-Host ""
Write-Host "Hotovo. Spustte run_dashboard.bat (pouzije runtime\python)." -ForegroundColor Green
exit 0
