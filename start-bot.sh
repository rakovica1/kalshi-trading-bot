#!/bin/bash
tmux new-session -d -s nightrader "cd /Users/ng/kalshi-trading-bot && PYTHONPATH=src python3 -m kalshi_bot.web"
echo "Nightrader bot started in tmux session 'nightrader'"
echo "To view: tmux attach -t nightrader"
echo "To detach: Ctrl+B, then D"
