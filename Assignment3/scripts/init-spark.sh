#!/bin/bash

echo "Installing required Python packages in Spark container..."

packages=("redis" "numpy")
for package in "${packages[@]}"; do
    if python3 -c "import ${package}" 2>/dev/null; then
        echo "${package} already installed"
    else
        echo "Installing ${package}..."
        pip install --quiet ${package} || {
            echo "Warning: Failed to install ${package}, continuing anyway..."
        }
    fi
done

echo "Verifying installations..."
if python3 -c "import redis; import numpy; print('All packages installed successfully')" 2>/dev/null; then
    echo "All packages are ready"
else
    echo "Warning: Some packages may not be available, but continuing..."
fi

echo "Spark initialization complete!"

