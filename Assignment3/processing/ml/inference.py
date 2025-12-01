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
from pyspark.ml import PipelineModel
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


class OrderPredictor:
    def __init__(self):
        self.project_root = "/workspace"
        self.spark = None
        self.model_path = os.path.join(self.project_root, "processing/ml/model")
        self.pipeline_model = None
        self.rf_model = None

    def _validate_environment(self):
        if not os.path.exists("/workspace"):
            print("Please run the script inside the Docker container:")
            print("  docker exec -it spark python3 /workspace/processing/ml/inference.py")
            sys.exit(1)

    def _create_spark_session(self):
        self.spark = SparkSession.builder \
            .appName("OrderPredictionInference") \
            .getOrCreate()

    def _load_models(self):
        pipeline_path = os.path.join(self.model_path, "pipeline")
        model_path = os.path.join(self.model_path, "rf_model")
        
        if not os.path.exists(pipeline_path) or not os.path.exists(model_path):
            raise FileNotFoundError("Model not found. Please run train.py first.")
        
        self.pipeline_model = PipelineModel.load(pipeline_path)
        self.rf_model = RandomForestRegressionModel.load(model_path)
        print("Models loaded successfully")

    def predict(self, day_of_week, hour_of_day, category):
        if self.pipeline_model is None or self.rf_model is None:
            self._validate_environment()
            self._create_spark_session()
            self._load_models()
        
        schema = StructType([
            StructField("day_of_week", IntegerType(), True),
            StructField("hour_of_day", IntegerType(), True),
            StructField("category", StringType(), True)
        ])
        
        data = [(day_of_week, hour_of_day, category)]
        df = self.spark.createDataFrame(data, schema)
        
        df_transformed = self.pipeline_model.transform(df)
        predictions = self.rf_model.transform(df_transformed)
        
        predicted_count = predictions.select("prediction").first()[0]
        return float(predicted_count)

    def cleanup(self):
        if self.spark:
            self.spark.stop()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Predict order items count")
    parser.add_argument("--day", type=int, required=True, help="Day of week (0-6)")
    parser.add_argument("--hour", type=int, required=True, help="Hour of day (0-23)")
    parser.add_argument("--category", type=str, required=True, help="Product category")
    
    args = parser.parse_args()
    
    predictor = OrderPredictor()
    try:
        predicted = predictor.predict(args.day, args.hour, args.category)
        print(f"Predicted items count: {predicted:.2f}")
        print(f"Input: day={args.day}, hour={args.hour}, category={args.category}")
    finally:
        predictor.cleanup()


if __name__ == "__main__":
    main()

