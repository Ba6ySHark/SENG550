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

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator


class OrderPredictorTrainer:
    def __init__(self):
        self.project_root = "/workspace"
        self.spark = None
        self.model_path = os.path.join(self.project_root, "processing/ml/model")

    def _validate_environment(self):
        if not os.path.exists("/workspace") or not os.path.exists("/workspace/data/processed"):
            print("Please run the script inside the Docker container:")
            print("  docker exec -it spark python3 /workspace/processing/ml/train.py")
            sys.exit(1)

    def _create_spark_session(self):
        self.spark = SparkSession.builder \
            .appName("OrderPredictionTraining") \
            .getOrCreate()

    def _load_data(self):
        processed_path = os.path.join(self.project_root, "data/processed/orders.csv")
        incremental_path = os.path.join(self.project_root, "data/incremental/processed/orders.csv")
        
        if os.path.exists(incremental_path):
            print(f"Loading data from: {incremental_path}")
            df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(incremental_path)
        elif os.path.exists(processed_path):
            print(f"Loading data from: {processed_path}")
            df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(processed_path)
        else:
            raise FileNotFoundError("No processed data found. Please run Part 1 or Part 2 first.")
        
        return df

    def _prepare_features(self, df):
        category_indexer = StringIndexer(inputCol="category", outputCol="category_index", handleInvalid="keep")
        
        assembler = VectorAssembler(
            inputCols=["day_of_week", "hour_of_day", "category_index"],
            outputCol="features"
        )
        
        pipeline = Pipeline(stages=[category_indexer, assembler])
        pipeline_model = pipeline.fit(df)
        df_transformed = pipeline_model.transform(df)
        
        return df_transformed, pipeline_model

    def _train_model(self, df):
        num_categories = df.select("category_index").distinct().count()
        max_bins = max(32, num_categories + 1)
        
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="items_count",
            numTrees=20,
            maxDepth=10,
            maxBins=max_bins,
            seed=42
        )
        
        model = rf.fit(df)
        return model

    def _save_model(self, model, pipeline_model):
        model_dir = self.model_path
        if os.path.exists(model_dir):
            import shutil
            shutil.rmtree(model_dir)
        
        os.makedirs(model_dir, exist_ok=True)
        model.write().overwrite().save(os.path.join(model_dir, "rf_model"))
        pipeline_model.write().overwrite().save(os.path.join(model_dir, "pipeline"))
        print(f"Model saved to: {model_dir}")

    def train(self):
        self._validate_environment()
        self._create_spark_session()
        
        print("Loading processed data...")
        df = self._load_data()
        print(f"Loaded {df.count()} records")
        
        print("Preparing features...")
        df_transformed, pipeline_model = self._prepare_features(df)
        
        print("Training Random Forest model...")
        model = self._train_model(df_transformed)
        
        print("Saving model...")
        self._save_model(model, pipeline_model)
        
        print("Training completed successfully!")
        self.spark.stop()


if __name__ == "__main__":
    trainer = OrderPredictorTrainer()
    trainer.train()

