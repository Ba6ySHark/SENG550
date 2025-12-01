## Installing Apache Spark, Airflow and Redis using Docker

For Apache Spark:
```bash
docker run -d --name spark \
  -p 4040:4040 \
  -v "$PWD":/workspace \
  apache/spark:4.1.0-preview4-scala2.13-java21-python3-r-ubuntu \
  sleep infinity
```

This runs the container in detached mode (`-d`) and keeps it running with `sleep infinity`. To run commands inside the container:
```bash
docker exec -it spark bash
```

Or to run the processing script directly:
```bash
docker exec -it spark python3 /workspace/processing/full/process_orders.py
```

For Apache Airflow:
```bash
cd airflow
export AIRFLOW_UID=50000
docker compose up airflow-init
```
```bash
docker compose up
```

For Redis:
```bash
docker run -d --name redis -p 6379:6379 redis
```