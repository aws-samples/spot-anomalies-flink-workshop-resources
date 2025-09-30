#!/bin/bash

# Usage: ./deploy.sh <account-id> <region>
if [ $# -ne 2 ]; then
    echo "Usage: $0 <account-id> <region>"
    exit 1
fi

ACCOUNT_ID=$1
REGION=$2
REPO_NAME="anomaly-detection-agent"
IMAGE_TAG="latest"

echo "Building and pushing Strands agent to ECR..."
echo "Account: $ACCOUNT_ID"
echo "Region: $REGION"

# Create ECR repository if it doesn't exist
aws ecr describe-repositories --repository-names $REPO_NAME --region $REGION 2>/dev/null || \
aws ecr create-repository --repository-name $REPO_NAME --region $REGION

# Get ECR login token
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com

# Build image
docker build -t $REPO_NAME:$IMAGE_TAG .

# Tag for ECR
docker tag $REPO_NAME:$IMAGE_TAG $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME:$IMAGE_TAG

# Push to ECR
docker push $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME:$IMAGE_TAG

echo "Image pushed to: $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME:$IMAGE_TAG"
