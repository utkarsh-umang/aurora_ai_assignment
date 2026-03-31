#!/usr/bin/env bash
# Usage: ./demo.sh [conversation_file]
# Restarts Redpanda, launches all workers, then runs each user message in the
# conversation file sequentially so memory is written between turns.
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$PROJECT_DIR/.venv/bin/activate"
CONVERSATION="${1:-$PROJECT_DIR/conversations/conversation_1.json}"

# Extract user messages in order using Python (always available in the venv)
QUERIES=()
while IFS= read -r line; do
  QUERIES+=("$line")
done < <(
  source "$VENV"
  python - <<EOF
import json
with open("$CONVERSATION") as f:
    data = json.load(f)
for msg in data["conversations"]:
    if msg["role"] == "user":
        print(msg["content"])
EOF
)

if [ ${#QUERIES[@]} -eq 0 ]; then
  echo "No user messages found in $CONVERSATION"
  exit 1
fi

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
# 4. Run each conversation turn sequentially in this terminal
# ---------------------------------------------------------------------------
source "$VENV"
cd "$PROJECT_DIR"

TOTAL=${#QUERIES[@]}
for i in "${!QUERIES[@]}"; do
  TURN=$((i + 1))
  QUERY="${QUERIES[$i]}"
  echo ""
  echo "==> Turn $TURN/$TOTAL: \"$QUERY\""
  echo "------------------------------------------------------------"
  python -m agent.run --query "$QUERY"
  echo "------------------------------------------------------------"
  echo "Turn $TURN complete. Trace written to out/<job_id>/trace.json"
done

echo ""
echo "All $TOTAL turns complete. Memory saved to: $PROJECT_DIR/out/memory.json"
