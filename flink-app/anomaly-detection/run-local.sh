#!/bin/bash

# Set AWS credentials (replace with your actual values)
# export AWS_ACCESS_KEY_ID="your_access_key_here"
# export AWS_SECRET_ACCESS_KEY="your_secret_key_here"
# export AWS_REGION="us-east-1"

# Navigate to the project directory
cd "$(dirname "$0")"

# Compile the project
echo "Compiling project..."
mvn clean package

# Build classpath
echo "Building classpath..."
CLASSPATH=$(mvn dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q):target/classes

# Run the Flink job
echo "Starting Flink job..."
java -cp "$CLASSPATH" \
  -DAWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  -DAWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  -DAWS_REGION="$AWS_REGION" \
  com.amazonaws.proserve.workshop.AnomalyDetection -f application.properties