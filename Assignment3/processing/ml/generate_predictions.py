import os
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
from pyspark.ml import PipelineModel
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, concat_ws, lit


class PredictionCacheGenerator:
    def __init__(self, redis_host='redis', redis_port=6379):
        self.project_root = "/workspace"
        self.spark = None
        self.model_path = os.path.join(self.project_root, "processing/ml/model")
        self.redis_client = None
        self.redis_host = redis_host
        self.redis_port = redis_port

    def _validate_environment(self):
        if not os.path.exists("/workspace"):
            print("Please run the script inside the Docker container:")
            print("  docker exec -it spark python3 /workspace/processing/ml/generate_predictions.py")
            sys.exit(1)

    def _create_spark_session(self):
        self.spark = SparkSession.builder \
            .appName("PredictionCacheGeneration") \
            .getOrCreate()

    def _connect_redis(self):
        if redis is None:
            print("Redis module not installed")
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
            print("Connected to Redis successfully")
        except Exception as e:
            print(f"Warning: Could not connect to Redis: {e}")
            self.redis_client = None

    def _load_models(self):
        pipeline_path = os.path.join(self.model_path, "pipeline")
        model_path = os.path.join(self.model_path, "rf_model")
        
        if not os.path.exists(pipeline_path) or not os.path.exists(model_path):
            raise FileNotFoundError("Model not found. Please run train.py first.")
        
        pipeline_model = PipelineModel.load(pipeline_path)
        rf_model = RandomForestRegressionModel.load(model_path)
        print("Models loaded successfully")
        return pipeline_model, rf_model

    def _get_all_combinations(self):
        days = list(range(7))
        hours = list(range(24))
        
        processed_path = os.path.join(self.project_root, "data/processed/orders.csv")
        incremental_path = os.path.join(self.project_root, "data/incremental/processed/orders.csv")
        
        if os.path.exists(incremental_path):
            df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(incremental_path)
        elif os.path.exists(processed_path):
            df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(processed_path)
        else:
            raise FileNotFoundError("No processed data found.")
        
        categories = [row.category for row in df.select("category").distinct().collect()]
        
        combinations = []
        for day in days:
            for hour in hours:
                for category in categories:
                    combinations.append((day, hour, category))
        
        schema = StructType([
            StructField("day_of_week", IntegerType(), True),
            StructField("hour_of_day", IntegerType(), True),
            StructField("category", StringType(), True)
        ])
        
        df_combinations = self.spark.createDataFrame(combinations, schema)
        print(f"Generated {len(combinations)} combinations")
        return df_combinations

    def _generate_predictions(self, pipeline_model, rf_model, df_combinations):
        df_transformed = pipeline_model.transform(df_combinations)
        predictions = rf_model.transform(df_transformed)
        
        predictions_with_key = predictions.select(
            concat_ws(":", col("day_of_week"), col("hour_of_day"), col("category")).alias("key"),
            col("prediction").alias("value")
        )
        
        return predictions_with_key

    def _store_in_redis(self, predictions_df):
        if not self.redis_client:
            print("Redis not available, skipping storage")
            return
        
        predictions = predictions_df.collect()
        count = 0
        
        for row in predictions:
            key = row.key
            value = str(round(float(row.value), 2))
            try:
                self.redis_client.set(key, value)
                count += 1
            except Exception as e:
                print(f"Error storing key {key}: {e}")
        
        print(f"Stored {count} predictions in Redis")

    def generate(self):
        self._validate_environment()
        self._connect_redis()
        self._create_spark_session()
        
        print("Loading models...")
        pipeline_model, rf_model = self._load_models()
        
        print("Generating all combinations...")
        df_combinations = self._get_all_combinations()
        
        print("Generating predictions...")
        predictions_df = self._generate_predictions(pipeline_model, rf_model, df_combinations)
        
        print("Storing predictions in Redis...")
        self._store_in_redis(predictions_df)
        
        print("Prediction cache generation completed successfully!")
        self.spark.stop()


if __name__ == "__main__":
    generator = PredictionCacheGenerator()
    generator.generate()

