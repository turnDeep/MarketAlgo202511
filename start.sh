#!/bin/bash

# Create logs directory
mkdir -p /app/logs

# Start cron in foreground
echo "Starting cron service..."
cron -f
