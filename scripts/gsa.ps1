param(
  [string]$Sport = "americanfootball_nfl",
  [switch]$Preflight,
  [switch]$Ingest,
  [switch]$Core,
  [switch]$All
)

$ErrorActionPreference = "Stop"
$repo = (Get-Location).Path
Write-Host "Repo: $repo"

function Ensure-Venv {
  if (-not (Test-Path ".\.venv\Scripts\Activate.ps1")) {
    python -m venv .venv
  }
  .\.venv\Scripts\Activate.ps1
}

function Check-Env {
  if (-not (Test-Path ".\.env.local")) {
    if (Test-Path ".\.env.template") {
      Copy-Item .\.env.template .\.env.local -Force
    } else {
      @"
ODDS_API_KEY=
DATABASE_URL=
LOG_LEVEL=INFO
"@ | Set-Content .\.env.local
    }
  }
  $envText = Get-Content .\.env.local -Raw
  if (-not ($envText -match 'ODDS_API_KEY=')) { throw "ODDS_API_KEY missing in .env.local" }
  if (-not ($envText -match 'DATABASE_URL=')) { throw "DATABASE_URL missing in .env.local" }
}

function Env-CheckPython {
  python .\services\ingestor\env_check.py
  python .\services\ingestor\odds_ping.py
}

function Patch-DB {
  # tolerant patches; run if present
  if (Test-Path .\services\db\patch_audit_logs.py) { python .\services\db\patch_audit_logs.py }
  if (Test-Path .\services\db\relax_audit_logs_module.py) { python .\services\db\relax_audit_logs_module.py }
  python .\services\db\create_views.py
}

function Ingest-League([string]$L) {
  # use module mode for imports
  python -m services.ingestor.ingest_odds --sport $L --regions us --markets h2h,spreads,totals --dry-run 0
  python .\services\ingestor\check_counts.py
  python .\services\ingestor\preview_latest_ml.py
}

function Run-Core {
  # launch Core on :8082 in a new window; keep this script return control
  $cmd = "powershell -NoExit -Command `"cd '$repo'; .\.venv\Scripts\Activate.ps1; uvicorn services.core.app:app --port 8082 --reload`""
  Start-Process cmd "/c $cmd"
  Start-Sleep -Seconds 2
  try { irm "http://127.0.0.1:8082/core/metrics" | Out-Null; Write-Host "Core is up at http://127.0.0.1:8082" -ForegroundColor Green } catch {}
}

# ---- main flow ----
Ensure-Venv
Check-Env
# force dotenv override behavior in case shell vars exist
$env:ODDS_API_KEY = $null
$env:DATABASE_URL = $null

if ($All -or $Preflight) {
  Env-CheckPython
  Patch-DB
}
if ($All -or $Ingest) {
  Ingest-League -L $Sport
}
if ($All -or $Core) {
  Run-Core
}
