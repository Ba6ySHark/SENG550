#!/bin/bash

echo "Installing redis in Spark container..."
if python3 -c "import redis" 2>/dev/null; then
    echo "Redis already installed"
else
    pip install --quiet redis || {
        echo "Warning: Failed to install redis, continuing anyway..."
    }
fi

echo "Verifying redis installation..."
if python3 -c "import redis; print('Redis installed successfully')" 2>/dev/null; then
    echo "Redis is ready"
else
    echo "Warning: Redis module not available, but continuing..."
fi

echo "Spark initialization complete!"

