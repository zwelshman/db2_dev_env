#!/bin/bash

# Start DB2 in the background
/var/db2_setup/lib/setup_db2_instance.sh &

# Wait for DB2 to be ready
echo "Waiting for DB2 to start..."
sleep 120

# Execute initialization scripts as db2inst1 user
if [ -f /var/custom/init-db.sql ]; then
    echo "Running initialization script..."
    su - db2inst1 -c "db2 -tvf /var/custom/init-db.sql" || true
fi

# Keep container running
wait
