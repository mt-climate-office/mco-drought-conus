#!/usr/bin/env bash
# One-time backfill: set Cache-Control on existing latest/ objects in S3.
#
# Objects published before the pipeline set Cache-Control carry no caching
# header at all, so browsers and CloudFront apply heuristic caching and serve
# stale COGs for hours after a republish. This stamps every existing latest/
# object with Cache-Control: no-cache (revalidate-before-serve; unchanged
# objects still answer with cheap 304s via their ETags). New uploads get the
# header from run_once.sh, so this only needs to run once.
#
# Usage:
#   AWS_PROFILE=mco bash scripts/backfill-cache-control.sh
#   AWS_BUCKET=mco-gridmet AWS_PROFILE=mco bash scripts/backfill-cache-control.sh

set -euo pipefail

AWS_BUCKET="${AWS_BUCKET:-mco-gridmet}"

# In-place copy with REPLACE rewrites all metadata, so Content-Type must be
# restated per file class or it would be dropped.
backfill_prefix() {
  local prefix="$1"
  echo "=== Backfilling s3://${AWS_BUCKET}/${prefix} ==="
  aws s3 cp "s3://${AWS_BUCKET}/${prefix}" "s3://${AWS_BUCKET}/${prefix}" \
    --recursive --exclude "*" --include "*.tif" \
    --metadata-directive REPLACE \
    --cache-control "no-cache" --content-type "image/tiff"
  aws s3 cp "s3://${AWS_BUCKET}/${prefix}" "s3://${AWS_BUCKET}/${prefix}" \
    --recursive --exclude "*" --include "*.csv" \
    --metadata-directive REPLACE \
    --cache-control "no-cache" --content-type "text/csv"
  aws s3 cp "s3://${AWS_BUCKET}/${prefix}" "s3://${AWS_BUCKET}/${prefix}" \
    --recursive --exclude "*" --include "*.txt" \
    --metadata-directive REPLACE \
    --cache-control "no-cache" --content-type "text/plain"
}

backfill_prefix "derived/conus_drought/latest/"
backfill_prefix "derived/conus_drought_web/latest/"

echo "=== Backfill complete ==="
echo "Spot check:"
aws s3api head-object \
  --bucket "$AWS_BUCKET" \
  --key "derived/conus_drought_web/latest/spei_30d_rolling-30.tif" \
  --query '{CacheControl: CacheControl, ContentType: ContentType, LastModified: LastModified}'
