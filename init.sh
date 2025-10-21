#!/bin/bash

echo "Waiting for DB2 to be ready (this takes 3-5 minutes)..."
sleep 200

echo "Running initialization script...">
docker-compose exec -T db su - db2inst1 -c "db2 -tvf /var/custom/init-db.sql"

echo "Database initialization complete!"
```

### 4. **Directory Structure**
```
project/
├── docker-compose.yml
├── .env
├── init.sh
└── init-scripts/
    └── init-db.sql
