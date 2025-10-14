#!/bin/bash

# Deploy MCP server to ECR
set -e

# Get AWS account ID and region
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=${AWS_DEFAULT_REGION}

echo "Deploying MCP server to account: $ACCOUNT_ID in region: $REGION"

# Login to ECR
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com

# Create ECR repository if it doesn't exist
echo "Creating ECR repository..."
aws ecr describe-repositories --repository-names incident-management-mcp --region $REGION 2>/dev/null || \
    aws ecr create-repository --repository-name incident-management-mcp --region $REGION

# Build and push MCP server
echo "Building MCP server..."
docker build --platform linux/arm64 -t incident-management-mcp:latest .
docker tag incident-management-mcp:latest $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/incident-management-mcp:latest
docker push $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/incident-management-mcp:latest

echo "MCP server deployed successfully!"
echo "Image: $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/incident-management-mcp:latest"
