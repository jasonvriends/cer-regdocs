#!/usr/bin/env bash
# Start the REGDOCS indexing stage detached from the terminal, so closing the
# terminal (or a session timeout) cannot kill the run.
#
# Usage:
#   ./run-index.sh                       # normal run (default --min-quality 0.0)
#   ./run-index.sh --min-quality 0.3     # skip low-quality/garbled documents
#
# Any arguments are passed through to `regdocs.py index`.
cd "$(dirname "$0")" || exit 1

if pgrep -f "regdocs\.py index" > /dev/null; then
    echo "An index run is already active. Watch it with: tail -f index.log"
    exit 1
fi

nohup setsid .venv/bin/python regdocs.py index "$@" > index.log 2>&1 < /dev/null &

echo "Indexing started in the background — safe to close this terminal."
echo "  Watch:  tail -f index.log"
echo "  Status: .venv/bin/python regdocs.py stats"
echo "  Stop:   pkill -f 'regdocs.py index'"
