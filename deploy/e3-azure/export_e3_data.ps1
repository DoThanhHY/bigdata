param(
    [Parameter(Mandatory = $true)][string]$Date,
    [Parameter(Mandatory = $true)][string]$Start,
    [Parameter(Mandatory = $true)][string]$End,
    [double]$MaxGapS = 10.0
)

$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$compose = Join-Path $root "docker-compose.yml"
$exporter = Join-Path $root "deploy\e3-azure\export_archive.py"

Push-Location $root
try {
    & docker compose -f $compose run --rm --no-deps --entrypoint python `
        -v "${exporter}:/tmp/export_archive.py:ro" `
        raw-archive /tmp/export_archive.py `
        --input /app/data/lake/bronze/raw_aggtrades `
        --output /app/data/exports/e3-data/lake/bronze/raw_aggtrades `
        --date $Date --start $Start --end $End --max-gap-s $MaxGapS
    $exitCode = $LASTEXITCODE
}
finally {
    Pop-Location
}

if ($exitCode -ne 0) {
    throw "Export E3 that bai (exit code $exitCode)"
}

$result = Join-Path $root "data\exports\e3-data\lake\bronze\raw_aggtrades\date=$Date\part-00000.parquet"
Write-Output "[ok] Upload file nay len VM: $result"
