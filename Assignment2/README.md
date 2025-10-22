# Overview
This project uses .env variables and DB connector packages and therefore requires the user to install some dependencies. They are saved in *requirements.txt* and can be installed via:

```bash
pip3 install -r requirements.txt
```

# Collaboration
This assignment was made in collaboraton of Steven Susorov (30197973) and Firdovsi Aliyev (30178471)

**Note:** For each query there is a screenshot that has the same name, but a different file extension.

# Part 0 - 5
Python scripts for performing operations and uploading data are saved in /scripts/python directory.

For Postgres, queries are saved under /scripts/sql

For MongoDB, queries are saved under /scripts/js

# Part 8
Intorducing MongoDB pipeline into the architcture results in certain benefits as well as some drawbacks:

## Benefits
- Query Performance (MongoDB allows for faster reads compared to Postgres)
- Scalability (Since MongoDB is better for distributed reads)

## Drawbacks
- Data Redundancy (Since data is duplicated into one more database across systems)
- Development Effort (Since there's now extra code to develop and maintain)
- Data Latency (Even with scheduled data uploads, the reports are still not truly real-time)

# Part 9 - Incremental Load

## a. Ignoring already inserted orders
We can introduce a timestamp for the MongoDB that would keep track of when was the last time we synced the database. We can then modify our upload script to compare timestamps from out Postgres database to filter out operations that preceed the last database sync.

## b. Scheduling pipeline runs
We can run the pipeline by either creating a cron job that would run *etl.py* in set intervals.

## c. Tradeoffs
Full-reload is more resource heavy and requires the database manager to manually run the script, which inflates data latency. On the other hand, it keeps data in both databases synced, which results in better data integrity.

Incremental updates require less computation and better data latency, but may compromise data integrity since full database sync is not perfomed on every run.
