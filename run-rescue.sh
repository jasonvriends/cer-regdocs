#!/usr/bin/env bash
# Start the REGDOCS rescue stage detached from the terminal, so closing the
# terminal (or a session timeout) cannot kill the run.
#
# Usage:
#   ./run-rescue.sh                    # text-layer fallback only (fast)
#   ./run-rescue.sh --vision           # also OCR true scans via a local vision model (slow)
#
# Any arguments are passed through to `regdocs.py rescue`.
cd "$(dirname "$0")" || exit 1

if pgrep -f "regdocs\.py (convert|rescue)" > /dev/null; then
    echo "A convert or rescue run is already active. Watch it with: tail -f rescue.log"
    exit 1
fi

nohup setsid .venv/bin/python regdocs.py rescue "$@" > rescue.log 2>&1 < /dev/null &

echo "Rescue started in the background — safe to close this terminal."
echo "  Watch:  tail -f rescue.log"
echo "  Status: .venv/bin/python regdocs.py stats"
echo "  Stop:   pkill -f 'regdocs.py rescue'"
