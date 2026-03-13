#!/usr/bin/env bash
# Build the Docker image from the repo root and push it to ECR.
# Usage: ./scripts/ecr-push.sh [image-tag]   (default tag: latest)
set -euo pipefail

REGION="us-west-2"
ACCOUNT_ID="202533506375"
PROFILE="mco"
REPO_NAME="mco-drought-conus"
IMAGE_TAG="${1:-latest}"

ECR_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=== Authenticating with ECR ==="
aws ecr get-login-password \
  --region "${REGION}" \
  --profile "${PROFILE}" \
  | docker login --username AWS --password-stdin "${ECR_URI}"

echo "=== Building image (context: ${REPO_ROOT}) ==="
docker build \
  --platform linux/amd64 \
  --tag "${REPO_NAME}:${IMAGE_TAG}" \
  --file "${REPO_ROOT}/Dockerfile" \
  "${REPO_ROOT}"

echo "=== Tagging and pushing ==="
docker tag "${REPO_NAME}:${IMAGE_TAG}" "${ECR_URI}:${IMAGE_TAG}"
docker push "${ECR_URI}:${IMAGE_TAG}"

if [ "${IMAGE_TAG}" != "latest" ]; then
  docker tag "${REPO_NAME}:${IMAGE_TAG}" "${ECR_URI}:latest"
  docker push "${ECR_URI}:latest"
fi

echo "=== Done: ${ECR_URI}:${IMAGE_TAG} ==="
