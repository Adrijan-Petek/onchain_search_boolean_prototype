#!/bin/bash
echo "Starting On-Chain Boolean Search demo..."

# Create output directories
mkdir -p charts outputs

# Run the demo
python src/demo_boolean.py

echo "Demo complete!"
echo "Charts in: charts/"
echo "Outputs in: outputs/"
