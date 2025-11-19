#!/bin/bash
LOG_DIR="/app/logs"
mkdir -p $LOG_DIR

echo "$(date): Starting IBD Screeners..." >> $LOG_DIR/ibd_screeners.log
cd /app
python run_ibd_screeners.py >> $LOG_DIR/ibd_screeners.log 2>&1
echo "$(date): IBD Screeners completed" >> $LOG_DIR/ibd_screeners.log
