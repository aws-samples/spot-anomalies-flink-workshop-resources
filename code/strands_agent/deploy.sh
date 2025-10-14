#!/bin/bash

# Deploy Agent to ECR
set -e

# Get AWS account ID and region
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=${AWS_DEFAULT_REGION}

echo "Deploying Agent to account: $ACCOUNT_ID in region: $REGION"

# Login to ECR
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com

# Create ECR repository if it doesn't exist
echo "Creating ECR repository..."
aws ecr describe-repositories --repository-names anomaly-detection-agent --region $REGION 2>/dev/null || \
    aws ecr create-repository --repository-name anomaly-detection-agent --region $REGION

# Build and push Agent
echo "Building Agent..."
docker build --platform linux/arm64 -t anomaly-detection-agent:latest .
docker tag anomaly-detection-agent:latest $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/anomaly-detection-agent:latest
docker push $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/anomaly-detection-agent:latest

echo "Agent deployed successfully!"
echo "Image: $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/anomaly-detection-agent:latest"
