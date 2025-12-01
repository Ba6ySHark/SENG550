#!/bin/bash
set -e

echo "Starting Airflow standalone..."
airflow standalone &
AIRFLOW_PID=$!

echo "Waiting for Airflow to initialize..."
sleep 45

echo "Resetting Airflow admin password..."
for i in {1..10}; do
    if airflow users reset-password -u admin -p admin 2>/dev/null; then
        echo "Password reset successfully"
        break
    elif [ $i -eq 1 ]; then
        echo "User doesn't exist, creating admin user..."
        airflow users create \
            --username admin \
            --firstname Admin \
            --lastname User \
            --role Admin \
            --email admin@example.com \
            --password admin 2>/dev/null || true
    fi
    sleep 5
done

echo "Airflow initialization complete!"
echo "Login credentials: admin / admin"
echo "Airflow UI: http://localhost:8080"

wait $AIRFLOW_PID

