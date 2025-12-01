import os
import shutil
import glob
import sys

spark_python_path = "/opt/spark/python"
if os.path.exists(spark_python_path) and spark_python_path not in sys.path:
    sys.path.insert(0, spark_python_path)

spark_lib_path = "/opt/spark/python/lib"
if os.path.exists(spark_lib_path):
    for file in os.listdir(spark_lib_path):
        if file.endswith(".zip"):
            zip_path = os.path.join(spark_lib_path, file)
            if zip_path not in sys.path:
                sys.path.insert(0, zip_path)

try:
    import redis
except ImportError:
    redis = None

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum


class IncrementalOrderProcessor:
    def __init__(self, redis_host='redis', redis_port=6379, redis_key='last_processed_day'):
        self.project_root = "/workspace"
        self.spark = None
        self.redis_client = None
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_key = redis_key

    def _validate_environment(self):
        if not os.path.exists("/workspace") or not os.path.exists("/workspace/data/incremental/raw"):
            print("Error: This script must be run inside the Spark Docker container.")
            print("\nPlease run the script inside the Docker container:")
            print("  docker exec -it spark python3 /workspace/processing/incremental/process_orders.py")
            sys.exit(1)

    def _connect_redis(self):
        if redis is None:
            print("Warning: redis module not installed. Continuing without Redis.")
            print("Install with: docker exec -it spark pip install redis")
            self.redis_client = None
            return
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                decode_responses=True,
                socket_connect_timeout=5
            )
            self.redis_client.ping()
        except Exception as e:
            print(f"Warning: Could not connect to Redis: {e}")
            print("Continuing without Redis - will process all data from scratch")
            self.redis_client = None

    def _get_last_processed_day(self):
        if not self.redis_client:
            return -1
        try:
            value = self.redis_client.get(self.redis_key)
            return int(value) if value else -1
        except Exception as e:
            print(f"Warning: Error reading from Redis: {e}")
            return -1

    def _set_last_processed_day(self, day):
        if not self.redis_client:
            return
        try:
            self.redis_client.set(self.redis_key, str(day))
        except Exception as e:
            print(f"Warning: Error writing to Redis: {e}")

    def _get_available_days(self):
        raw_dir = os.path.join(self.project_root, "data/incremental/raw")
        days = []
        if os.path.exists(raw_dir):
            for item in os.listdir(raw_dir):
                item_path = os.path.join(raw_dir, item)
                if os.path.isdir(item_path) and item.isdigit():
                    csv_files = glob.glob(os.path.join(item_path, "*.csv"))
                    if csv_files:
                        days.append(int(item))
        return sorted(days)

    def _get_unprocessed_days(self):
        last_processed = self._get_last_processed_day()
        available_days = self._get_available_days()
        unprocessed = [day for day in available_days if day > last_processed]
        return sorted(unprocessed)

    def _create_spark_session(self):
        self.spark = SparkSession.builder \
            .appName("IncrementalOrderAggregation") \
            .getOrCreate()

    def _unpivot_data(self, df):
        metadata_cols = ["order_id", "order_dow", "order_hour_of_day", "days_since_prior_order"]
        category_cols = [c for c in df.columns if c not in metadata_cols]
        stack_expr = f"stack({len(category_cols)}, " + \
                     ", ".join([f"'{c}', `{c}`" for c in category_cols]) + \
                     ") as (category, items_count)"
        select_exprs = ["order_dow as day_of_week", "order_hour_of_day as hour_of_day"]
        select_exprs.append(stack_expr)
        return df.selectExpr(*select_exprs)

    def _read_existing_data(self, output_path):
        if os.path.exists(output_path) and os.path.isfile(output_path):
            try:
                return self.spark.read.option("header", "true").option("inferSchema", "true").csv(output_path)
            except Exception as e:
                print(f"Warning: Could not read existing data: {e}")
                return None
        return None

    def _append_to_csv(self, new_df, output_path):
        existing_df = self._read_existing_data(output_path)
        if existing_df:
            combined_df = existing_df.union(new_df)
            aggregated_df = combined_df.groupBy("day_of_week", "hour_of_day", "category") \
                .agg(_sum("items_count").alias("items_count"))
        else:
            aggregated_df = new_df.groupBy("day_of_week", "hour_of_day", "category") \
                .agg(_sum("items_count").alias("items_count"))
        
        aggregated_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path + ".tmp")
        part_files = glob.glob(os.path.join(output_path + ".tmp", "part-*.csv"))
        if part_files:
            if os.path.exists(output_path):
                if os.path.isdir(output_path):
                    shutil.rmtree(output_path)
                else:
                    os.remove(output_path)
            shutil.copy(part_files[0], output_path)
            shutil.rmtree(output_path + ".tmp")

    def process(self):
        self._validate_environment()
        self._connect_redis()
        unprocessed_days = self._get_unprocessed_days()
        
        if not unprocessed_days:
            print("No new data to process.")
            return
        
        print(f"Processing days: {unprocessed_days}")
        self._create_spark_session()
        
        input_paths = []
        for day in unprocessed_days:
            day_dir = os.path.join(self.project_root, f"data/incremental/raw/{day}")
            csv_files = glob.glob(os.path.join(day_dir, "*.csv"))
            input_paths.extend(csv_files)
        
        if not input_paths:
            print("No CSV files found to process.")
            self.spark.stop()
            return
        
        if len(input_paths) == 1:
            df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(input_paths[0])
        else:
            dfs = []
            for path in input_paths:
                day_df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(path)
                dfs.append(day_df)
            from functools import reduce
            from pyspark.sql import DataFrame
            df = reduce(DataFrame.unionByName, dfs)
        unpivoted_df = self._unpivot_data(df)
        filtered_df = unpivoted_df.filter(col("items_count") > 0)
        
        output_path = os.path.join(self.project_root, "data/incremental/processed/orders.csv")
        self._append_to_csv(filtered_df, output_path)
        
        max_processed_day = max(unprocessed_days)
        self._set_last_processed_day(max_processed_day)
        print(f"Processed up to day {max_processed_day}")
        
        self.spark.stop()


if __name__ == "__main__":
    processor = IncrementalOrderProcessor()
    processor.process()

