#!/bin/bash
# Wrapper: loads Telegram credentials from ~/.yad2.env, then runs the monitor.
# Never put tokens in crontab directly.
set -a
# shellcheck source=/dev/null
source "$HOME/.yad2.env"
set +a
exec /Users/evgeniyre/AIcode/Yad2/yad2-venv/bin/python3 \
    /Users/evgeniyre/AIcode/Yad2/yad2_monitor.py "$@"
