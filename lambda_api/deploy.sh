#!/bin/bash
# Deploy the EPA GHG API to AWS Lambda
# Uses Lambda Function URLs (FREE) instead of API Gateway ($$$)

set -e

STACK_NAME="epa-ghg-api"
REGION="${AWS_REGION:-us-east-1}"

echo "============================================"
echo "EPA GHG API - Lambda Deployment"
echo "============================================"
echo ""
echo "Cost-saving features:"
echo "  - Lambda Function URL (FREE vs API Gateway)"
echo "  - ARM64 architecture (20% cheaper)"
echo "  - Parquet files (faster queries, less compute)"
echo ""

# Check for SAM CLI
if ! command -v sam &> /dev/null; then
    echo "ERROR: AWS SAM CLI is not installed."
    echo "Install it with: brew install aws-sam-cli"
    exit 1
fi

# Build the Lambda package
echo "Building Lambda package..."
sam build --use-container

# Deploy to AWS (SAM will create/use a managed S3 bucket)
echo ""
echo "Deploying to AWS..."
sam deploy \
    --stack-name $STACK_NAME \
    --region $REGION \
    --capabilities CAPABILITY_IAM \
    --resolve-s3 \
    --no-confirm-changeset \
    --no-fail-on-empty-changeset

# Get the Function URL
echo ""
echo "Getting deployment outputs..."
FUNCTION_URL=$(aws cloudformation describe-stacks \
    --stack-name $STACK_NAME \
    --region $REGION \
    --query 'Stacks[0].Outputs[?OutputKey==`ApiEndpoint`].OutputValue' \
    --output text)

echo ""
echo "============================================"
echo "DEPLOYMENT COMPLETE!"
echo "============================================"
echo ""
echo "Function URL: $FUNCTION_URL"
echo ""
echo "Test endpoints:"
echo "  curl ${FUNCTION_URL}ghgp/api/version"
echo "  curl -X POST ${FUNCTION_URL}ghgp/api/list/sectors -H 'Content-Type: application/json' -d '{\"reportingYear\": 2023}'"
echo ""
echo "To use with your local FLIGHT site, update config.json:"
echo "  {\"BASE_API_URL\": \"${FUNCTION_URL}ghgp/api\"}"
echo ""
echo "Estimated costs (at low usage):"
echo "  - Lambda: ~$0.20 per 1M requests + compute"
echo "  - Function URL: FREE"
echo "  - S3 GET requests: ~$0.40 per 1M requests"
echo ""
