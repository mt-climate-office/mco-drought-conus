# EFS is not used. The pipeline caches GridMET data in S3 (see run_once.sh).
# The filesystem below was created but never had a mount target due to
# NetworkDenyPolicy restricting elasticfilesystem:CreateMountTarget.
# It is intentionally absent from this config so Terraform destroys it.
