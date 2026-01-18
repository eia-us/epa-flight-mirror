#!/bin/bash
# Deploy the EPA GHG API to AWS Lambda using AWS CLI (no SAM required)
# Uses Lambda Function URLs (FREE) instead of API Gateway

set -e

FUNCTION_NAME="epa-ghg-api"
REGION="${AWS_REGION:-us-east-1}"
ROLE_NAME="epa-ghg-api-role"
S3_BUCKET="epa-backups-eia"
S3_PREFIX="epa_ghg_tables_parquet"

echo "============================================"
echo "EPA GHG API - Lambda Deployment (AWS CLI)"
echo "============================================"
echo ""
echo "Cost-saving features:"
echo "  - Lambda Function URL (FREE vs API Gateway)"
echo "  - ARM64 architecture (20% cheaper)"
echo "  - Parquet files (faster queries, less compute)"
echo ""

# Get AWS account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "AWS Account: $ACCOUNT_ID"
echo "Region: $REGION"
echo ""

# Create IAM role for Lambda (if not exists)
echo "Setting up IAM role..."
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

# Trust policy for Lambda
cat > /tmp/trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create role if it doesn't exist
if ! aws iam get-role --role-name $ROLE_NAME 2>/dev/null; then
    echo "Creating IAM role: $ROLE_NAME"
    aws iam create-role \
        --role-name $ROLE_NAME \
        --assume-role-policy-document file:///tmp/trust-policy.json \
        --description "Role for EPA GHG API Lambda function"

    # Attach basic Lambda execution policy
    aws iam attach-role-policy \
        --role-name $ROLE_NAME \
        --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

    # Create and attach S3 read policy
    cat > /tmp/s3-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::${S3_BUCKET}",
        "arn:aws:s3:::${S3_BUCKET}/${S3_PREFIX}/*"
      ]
    }
  ]
}
EOF

    aws iam put-role-policy \
        --role-name $ROLE_NAME \
        --policy-name S3ReadAccess \
        --policy-document file:///tmp/s3-policy.json

    echo "Waiting for role to propagate..."
    sleep 10
else
    echo "IAM role already exists: $ROLE_NAME"
fi

# Create deployment package
echo ""
echo "Creating deployment package..."
cd "$(dirname "$0")"

# Create a temp directory for packaging
rm -rf /tmp/lambda-package
mkdir -p /tmp/lambda-package

# Install dependencies for ARM64 Linux
echo "Installing dependencies..."
pip install --platform manylinux2014_aarch64 --target /tmp/lambda-package --implementation cp --python-version 3.11 --only-binary=:all: duckdb -q

# Copy Lambda function
cp lambda_function.py /tmp/lambda-package/

# Create zip
cd /tmp/lambda-package
rm -f /tmp/lambda-deployment.zip
zip -r /tmp/lambda-deployment.zip . -q
cd -

PACKAGE_SIZE=$(du -h /tmp/lambda-deployment.zip | cut -f1)
echo "Package size: $PACKAGE_SIZE"

# Check if function exists
echo ""
if aws lambda get-function --function-name $FUNCTION_NAME --region $REGION 2>/dev/null; then
    echo "Updating existing Lambda function..."
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --zip-file fileb:///tmp/lambda-deployment.zip \
        --region $REGION \
        --architectures arm64 \
        --output text --query 'FunctionArn'

    # Wait for update to complete
    aws lambda wait function-updated --function-name $FUNCTION_NAME --region $REGION

    # Update configuration
    aws lambda update-function-configuration \
        --function-name $FUNCTION_NAME \
        --region $REGION \
        --timeout 30 \
        --memory-size 1024 \
        --environment "Variables={S3_BUCKET=${S3_BUCKET},S3_PREFIX=${S3_PREFIX},S3_REGION=${REGION}}" \
        --output text --query 'FunctionArn'
else
    echo "Creating new Lambda function..."
    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --runtime python3.11 \
        --architectures arm64 \
        --role $ROLE_ARN \
        --handler lambda_function.lambda_handler \
        --zip-file fileb:///tmp/lambda-deployment.zip \
        --timeout 30 \
        --memory-size 1024 \
        --environment "Variables={S3_BUCKET=${S3_BUCKET},S3_PREFIX=${S3_PREFIX},S3_REGION=${REGION}}" \
        --region $REGION \
        --description "EPA GHG Data API - queries Parquet files from S3" \
        --output text --query 'FunctionArn'
fi

# Wait for function to be ready
echo "Waiting for function to be ready..."
aws lambda wait function-active --function-name $FUNCTION_NAME --region $REGION

# Create or get Function URL
echo ""
echo "Setting up Function URL..."
FUNCTION_URL=$(aws lambda get-function-url-config --function-name $FUNCTION_NAME --region $REGION 2>/dev/null | grep -o '"FunctionUrl": "[^"]*"' | cut -d'"' -f4 || echo "")

if [ -z "$FUNCTION_URL" ]; then
    echo "Creating Function URL..."
    FUNCTION_URL=$(aws lambda create-function-url-config \
        --function-name $FUNCTION_NAME \
        --auth-type NONE \
        --cors 'AllowOrigins=*,AllowMethods=GET,AllowMethods=POST,AllowMethods=OPTIONS,AllowHeaders=Content-Type,AllowHeaders=Accept' \
        --region $REGION \
        --output text --query 'FunctionUrl')

    # Add permission for public access
    aws lambda add-permission \
        --function-name $FUNCTION_NAME \
        --statement-id FunctionURLAllowPublicAccess \
        --action lambda:InvokeFunctionUrl \
        --principal "*" \
        --function-url-auth-type NONE \
        --region $REGION 2>/dev/null || true
else
    echo "Function URL already exists"
fi

# Clean up
rm -f /tmp/trust-policy.json /tmp/s3-policy.json /tmp/lambda-deployment.zip
rm -rf /tmp/lambda-package

echo ""
echo "============================================"
echo "DEPLOYMENT COMPLETE!"
echo "============================================"
echo ""
echo "Function URL: $FUNCTION_URL"
echo ""
echo "Test endpoints:"
echo "  curl \"${FUNCTION_URL}ghgp/api/version\""
echo "  curl -X POST \"${FUNCTION_URL}ghgp/api/list/sectors\" -H 'Content-Type: application/json' -d '{\"reportingYear\": 2023}'"
echo ""
echo "To use with your local FLIGHT site, update config.json:"
echo "  {\"BASE_API_URL\": \"${FUNCTION_URL}ghgp/api\"}"
echo ""
echo "Estimated monthly costs (low usage):"
echo "  - Lambda compute: ~\$0.20 per 1M requests"
echo "  - Function URL: FREE (no API Gateway!)"
echo "  - S3 GET requests: ~\$0.40 per 1M requests"
echo "  - Data transfer: First 100GB/month FREE"
echo ""
