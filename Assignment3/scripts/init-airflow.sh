#!/bin/bash
set -e

echo "Waiting for Airflow to be ready..."
sleep 30

echo "Resetting Airflow admin password..."
airflow users reset-password -u admin -p admin || echo "Password reset failed or user doesn't exist, creating user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin 2>/dev/null || echo "User already exists, password reset attempted"

echo "Airflow initialization complete!"
echo "Login credentials: admin / admin"

