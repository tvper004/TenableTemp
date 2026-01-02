#!/bin/sh
# Run your script

# Source and destination file paths
SRC_FILE="/usr/src/app/scripts/state.json"
DEST_FILE="/usr/src/app/reports/state.json"

mkdir -p /usr/src/app/reports

# Check if the destination file does not exist
if [ ! -f "$DEST_FILE" ]; then
    # If it does not exist, copy the source file to the destination
    cp "$SRC_FILE" "$DEST_FILE"
fi

sleep 20

# Legacy Scripts (Commented out for Refactor)
#echo "Initial Pull: Starting" 
#date
#/usr/local/bin/python /usr/src/app/scripts/VickyTopiaReportCLI.py --allreports >> /var/log/initialsync.log 2>&1
#echo "Initial Pull: Completed" 
#date
#echo "Starting Scheduler"
#date
#/usr/local/bin/python /usr/src/app/scripts/launcher.py

# New Data Lakehouse Architecture Entrypoint
echo "🚀 Starting Data Lakehouse ETL Orchestrator..."
/usr/local/bin/python /usr/src/app/scripts/etl_orchestrator.py >> /var/log/etl.log 2>&1
echo "✅ ETL Pipeline Completed."

# Keep container alive
tail -f /dev/null
