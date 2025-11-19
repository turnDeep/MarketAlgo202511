#!/bin/bash
# Load environment variables for cron jobs
if [ -f /app/.env ]; then
    set -a
    source /app/.env
    set +a
fi
