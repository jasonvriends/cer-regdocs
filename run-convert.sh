#!/usr/bin/env bash
# Start the REGDOCS conversion detached from the terminal, so closing the
# terminal (or a session timeout) cannot kill the run.
#
# Usage:
#   ./run-convert.sh                     # normal run
#   ./run-convert.sh --max-worker-mem 26 # big-document pass with higher memory limit
#
# Any arguments are passed through to `regdocs.py convert`.
cd "$(dirname "$0")" || exit 1

if pgrep -f "regdocs\.py convert" > /dev/null; then
    echo "A convert run is already active. Watch it with: tail -f convert.log"
    exit 1
fi

nohup setsid .venv/bin/python regdocs.py convert "$@" > convert.log 2>&1 < /dev/null &

echo "Conversion started in the background — safe to close this terminal."
echo "  Watch:  tail -f convert.log"
echo "  Status: .venv/bin/python regdocs.py stats"
echo "  Stop:   pkill -f 'regdocs.py convert'"
