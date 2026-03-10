#!/usr/bin/env bash
# Build and deploy the Storage Browser React app to S3 + CloudFront.
# Reads required values from terraform outputs — run `terraform apply` first.
# Usage: ./scripts/deploy-storage-browser.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="${REPO_ROOT}/terraform"
APP_DIR="${REPO_ROOT}/storage-browser"
PROFILE="${AWS_PROFILE:-mco}"
REGION="${AWS_REGION:-us-west-2}"

echo "=== Reading Terraform outputs ==="
cd "${TF_DIR}"
IDENTITY_POOL_ID="$(terraform output -raw storage_browser_identity_pool_id)"
APP_BUCKET="$(terraform output -raw storage_browser_app_bucket)"
CF_DIST_ID="$(terraform output -raw storage_browser_cloudfront_id)"
S3_BUCKET="$(terraform output -raw s3_bucket_name)"

echo "  Identity Pool : ${IDENTITY_POOL_ID}"
echo "  App Bucket    : ${APP_BUCKET}"
echo "  CloudFront ID : ${CF_DIST_ID}"
echo "  Data Bucket   : ${S3_BUCKET}"

echo "=== Writing .env ==="
cd "${APP_DIR}"
cat > .env <<EOF
VITE_IDENTITY_POOL_ID=${IDENTITY_POOL_ID}
VITE_S3_BUCKET=${S3_BUCKET}
VITE_AWS_REGION=${REGION}
EOF

echo "=== Installing dependencies ==="
npm install

echo "=== Building ==="
npm run build

echo "=== Syncing to S3 ==="
aws s3 sync dist/ "s3://${APP_BUCKET}/" \
  --profile "${PROFILE}" \
  --delete \
  --no-progress

echo "=== Invalidating CloudFront cache ==="
aws cloudfront create-invalidation \
  --distribution-id "${CF_DIST_ID}" \
  --paths "/*" \
  --profile "${PROFILE}" \
  --output text --query 'Invalidation.Id'

BROWSER_URL="$(cd "${TF_DIR}" && terraform output -raw storage_browser_url)"
echo ""
echo "=== Done ==="
echo "Storage Browser: ${BROWSER_URL}"
