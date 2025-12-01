import os
import sys

try:
    import redis
except ImportError:
    redis = None


class CachedOrderPredictor:
    def __init__(self, redis_host='redis', redis_port=6379):
        self.redis_client = None
        self.redis_host = redis_host
        self.redis_port = redis_port

    def _connect_redis(self):
        if redis is None:
            print("Error: redis module not installed.")
            sys.exit(1)
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                decode_responses=True,
                socket_connect_timeout=5
            )
            self.redis_client.ping()
        except Exception as e:
            print(f"Error: Could not connect to Redis: {e}")
            sys.exit(1)

    def predict(self, day_of_week, hour_of_day, category):
        if self.redis_client is None:
            self._connect_redis()
        
        key = f"{day_of_week}:{hour_of_day}:{category}"
        
        try:
            value = self.redis_client.get(key)
            if value is None:
                print(f"Warning: No prediction found for key: {key}")
                return None
            return float(value)
        except Exception as e:
            print(f"Error reading from Redis: {e}")
            return None


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Predict order items count from Redis cache")
    parser.add_argument("--day", type=int, required=True, help="Day of week (0-6)")
    parser.add_argument("--hour", type=int, required=True, help="Hour of day (0-23)")
    parser.add_argument("--category", type=str, required=True, help="Product category")
    
    args = parser.parse_args()
    
    predictor = CachedOrderPredictor()
    predicted = predictor.predict(args.day, args.hour, args.category)
    
    if predicted is not None:
        print(f"Predicted items count: {predicted:.2f}")
        print(f"Input: day={args.day}, hour={args.hour}, category={args.category}")
        print(f"Source: Redis cache")
    else:
        print("Run generate_predictions.py")
        sys.exit(1)


if __name__ == "__main__":
    main()

