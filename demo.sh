#!/usr/bin/env bash
# Usage: ./demo.sh [query]
# Restarts Redpanda, launches all workers, then runs the pipeline in a new terminal.
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$PROJECT_DIR/.venv/bin/activate"
QUERY="${1:-find plumbers in austin open now}"

# ---------------------------------------------------------------------------
# 1. Restart Redpanda
# ---------------------------------------------------------------------------
echo "==> Bringing Redpanda down..."
cd "$PROJECT_DIR"
docker compose down

echo "==> Starting Redpanda..."
docker compose up -d

echo -n "==> Waiting for Redpanda to be ready..."
until docker exec aurora-redpanda rpk cluster info &>/dev/null 2>&1; do
  printf '.'
  sleep 1
done
echo " ready!"

# ---------------------------------------------------------------------------
# Helper: open a command in a new Terminal window
# ---------------------------------------------------------------------------
open_term() {
  local cmd="$1"
  local title="$2"
  osascript <<APPLESCRIPT
tell application "Terminal"
  set w to do script "echo '=== $title ===' && $cmd"
  activate
end tell
APPLESCRIPT
}

# ---------------------------------------------------------------------------
# 2. Start workers
# ---------------------------------------------------------------------------
echo "==> Starting scrape worker..."
open_term "cd '$PROJECT_DIR' && source '$VENV' && python -m agent.worker scrape" "scrape-worker"

echo "==> Starting 3 voice workers..."
for i in 1 2 3; do
  open_term "cd '$PROJECT_DIR' && source '$VENV' && python -m agent.worker voice" "voice-worker-$i"
done

# ---------------------------------------------------------------------------
# 3. Give workers time to connect, register with consumer group, and get
#    their partition assignments before the CLI publishes any messages.
# ---------------------------------------------------------------------------
echo "==> Waiting 5s for workers to join consumer groups..."
sleep 5

# ---------------------------------------------------------------------------
# 4. Run the pipeline
# ---------------------------------------------------------------------------
echo "==> Launching pipeline in new terminal..."
open_term "cd '$PROJECT_DIR' && source '$VENV' && python -m agent.run --query '$QUERY'" "aurora-run"

echo ""
echo "All terminals launched. Watch the voice-worker windows — with 3 workers"
echo "consuming from 6 partitions, calls should now process in parallel."
echo "Trace will be written to: $PROJECT_DIR/out/<job_id>/trace.json"
