# Part 2: Incremental Data Aggregation

## Redis Tracking Logic

The incremental processing system uses Redis to track which days have been processed, enabling efficient incremental loading.

### How It Works:

1. **Last Processed Day Storage**: 
   - Redis key: `last_processed_day`
   - Value: Integer representing the highest day number that has been processed
   - Example: If days 0, 1, 2, 3 are processed, Redis stores `3`

2. **Unprocessed Day Detection**:
   - On each run, the script scans `data/incremental/raw/` for numeric folder names (0, 1, 2, etc.)
   - For each folder, it checks if CSV files exist
   - Compares available days with `last_processed_day` from Redis
   - Processes only days where `day > last_processed_day`

3. **Processing Flow**:
   ```
   Start → Read Redis (last_processed_day) → Scan folders → Find unprocessed days → 
   Process new days → Update Redis → Append to output CSV
   ```

4. **Multiple Days Handling**:
   - If multiple new folders arrive between Airflow runs, they are all processed together in a single execution
   - Example: If folders 3, 4, 5 are added, all three are processed in one run
   - Redis is updated to the maximum processed day (5 in this case)

5. **Redis Unavailability**:
   - If Redis is unavailable or cleared, `last_processed_day` returns -1
   - This causes all available days to be processed (since all days > -1)
   - System reprocesses everything from scratch, ensuring no data loss

### Benefits:
- **Efficiency**: Only processes new data, avoiding redundant work
- **Idempotency**: Can safely re-run without duplicating data
- **Resilience**: If Redis fails, system falls back to full reprocessing

---

## Redis Durability and System Consistency

### Question: Does Redis data loss cause inconsistency?

**Answer: No, it does not cause data inconsistency, but it causes inefficiency.**

### Impact Analysis:

1. **Data Consistency**: ✅ **Maintained**
   - The processed output CSV file (`data/incremental/processed/orders.csv`) is the source of truth
   - Redis is only used for **tracking** which days have been processed, not for storing actual data
   - If Redis is lost, the system reprocesses all data, but the final output remains correct
   - The aggregation logic ensures that reprocessing produces the same results (idempotent operations)

2. **Potential Issues**:
   - **Inefficiency**: If Redis is lost, the system reprocesses already-processed data
   - **Performance**: Reprocessing all data takes longer than incremental processing
   - **Resource Usage**: Unnecessary CPU and I/O for reprocessing known data

3. **Why No Data Loss Occurs**:
   - The output CSV file persists on disk (not in Redis)
   - The aggregation uses `groupBy` and `sum`, which are idempotent
   - Reprocessing and appending produces the same aggregated results
   - The `_append_to_csv` method properly merges existing and new data

### Mitigation Strategies:

1. **Redis Persistence** (Current Implementation):
   - Redis is configured with `--appendonly yes` in docker-compose.yaml
   - This enables AOF (Append-Only File) persistence
   - Redis writes all write operations to disk, surviving container restarts

2. **Additional Mitigations** (For Production):

   a. **Periodic Backup of Redis State**:
      ```bash
      # Backup Redis key
      redis-cli GET last_processed_day > backup.txt
      ```

   b. **Dual Tracking**:
      - Store `last_processed_day` in both Redis and a file
      - Use file as fallback if Redis is unavailable
      - Example: `data/incremental/.last_processed_day`

   c. **Checkpoint Files**:
      - Create a checkpoint file after each successful processing
      - Contains metadata: last processed day, timestamp, checksum
      - Redis becomes a fast cache, file becomes durable storage

   d. **Database Instead of Redis**:
      - Use PostgreSQL/MySQL to store processing state
      - Provides ACID guarantees and durability
      - More reliable for critical tracking information

   e. **Redis Cluster with Replication**:
      - Use Redis Sentinel or Cluster mode
      - Automatic failover and data replication
      - Higher availability and durability

3. **Current System Behavior**:
   - If Redis is cleared: System reprocesses all data (safe but inefficient)
   - If Redis is unavailable: System reprocesses all data (safe but inefficient)
   - **No data corruption or inconsistency occurs** - only performance impact

### Conclusion:

The system is **designed to be resilient** to Redis failures. While Redis data loss causes inefficiency (reprocessing), it does **not** cause data inconsistency because:
- Output data is stored persistently in CSV files
- Aggregation operations are idempotent
- Reprocessing produces identical results
- The system gracefully falls back to full processing when Redis is unavailable

For production systems, implementing additional durability measures (file-based checkpoints, database storage, or Redis persistence) would eliminate the inefficiency while maintaining the current safety guarantees.

