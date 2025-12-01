## Dependencies

To install dependencies run:
```bash
virtualenv venv
```

```bash
source venv/bin/activate
```

```bash
pip3 install -r requirements.txt
```

## Part 0

It is possible to start all of the needed services by running:
```bash
docker compose up -d
```

**After running the docker compose:**
- Redis is installed in the Spark container
- Airflow admin password is set to `admin`

**Note:** Airflow UI can be accessed at http://localhost:8080
**Note:** The incremental processing DAG runs automatically every 4 seconds. It will process any new data in `data/incremental/raw/` folders.

## Part 1

To run full processing:
```bash
docker compose exec spark python3 /workspace/processing/full/process_orders.py
```

**Note:** `split_orders.py` was used to slice the original orders.csv into raw data

## Part 2

### Overview
1. The Airflow DAG `incremental_orders_processing` runs every 4 seconds automatically.

2. Monitor the processing:
   - Airflow UI: http://localhost:8080
   - Check processed output: `data/incremental/processed/orders.csv`
   - Check Redis status via: `docker exec -it redis redis-cli GET last_processed_day`

### Redis Tracking Logic

1. **Last Processed Day Storage**: 
   - Redis key: `last_processed_day`
   - Value: Integer representing the highest day number that has been processed
   - Example: If days 0, 1, 2, 3 are processed, Redis stores `3`

2. **Unprocessed Days Detection**:
   - On each run, the script scans `data/incremental/raw/` for numeric folder names (0, 1, 2, etc.)
   - For each folder, it checks if CSV files exist
   - Compares available days with `last_processed_day` from Redis
   - Processes only days where `day > last_processed_day`

3. **Processing**:
   ```
   Start → Check Redis (last_processed_day) → Scan folders → Find unprocessed days → 
   Process new days → Update Redis → Append to output CSV
   ```

4. **If Multiple Unprocessed Days**:
   - If multiple new folders are created, they are all processed together in a single run
   - Example: If folders 3, 4, 5 are added, all three are processed in one run
   - Redis is updated to the maximum processed day

5. **If Redis Is Empty**:
   - If Redis is empty, `last_processed_day` returns -1
   - This causes all available days to be processed
   - System reprocesses everything from scratch

### Redis Impact
Using Redis does not lead to data inconsistency in this case, since we are only storing the number of the last processed day. Therefore, in the case Redis data is lost we would simply re-process all of the days. This may lead to worse efficiency, but not to data inconsistency.

## Part 3