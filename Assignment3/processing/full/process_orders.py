import os
import shutil
import glob
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

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

class OrderProcessor:
    def __init__(self):
        self.project_root = "/workspace"
        self.spark = None

    def _validate_environment(self):
        if not os.path.exists("/workspace") or not os.path.exists("/workspace/data/raw"):
            print("Error: This script must be run inside the Spark Docker container.")
            print("\nPlease run the script inside the Docker container:")
            print("  docker exec -it spark python3 /workspace/processing/full/process_orders.py")
            print("\nOr if the container is not running, start it first:")
            print('  docker run -d --name spark -p 4040:4040 -v "$PWD":/workspace apache/spark:4.1.0-preview4-scala2.13-java21-python3-r-ubuntu sleep infinity')
            sys.exit(1)

    def _create_spark_session(self):
        self.spark = SparkSession.builder \
            .appName("OrderAggregation") \
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

    def _write_single_csv(self, df, output_path):
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path + ".tmp")
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
        self._create_spark_session()
        input_path = os.path.join(self.project_root, "data/raw/*/*.csv")
        output_path = os.path.join(self.project_root, "data/processed/orders.csv")
        df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
        unpivoted_df = self._unpivot_data(df)
        filtered_df = unpivoted_df.filter(col("items_count") > 0)
        aggregated_df = filtered_df.groupBy("day_of_week", "hour_of_day", "category") \
            .agg(_sum("items_count").alias("items_count"))
        self._write_single_csv(aggregated_df, output_path)
        self.spark.stop()


if __name__ == "__main__":
    processor = OrderProcessor()
    processor.process()