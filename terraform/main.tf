terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Uncomment to store state in S3 (recommended for teams).
  # First run `terraform apply` in terraform/bootstrap/ to create the bucket.
  # Shared state bucket. Auth comes from the environment (AWS_PROFILE=mco
  # locally, OIDC role credentials in CI) — never hardcode a profile here.
  backend "s3" {
    bucket       = "mco-terraform-state"
    key          = "mco-drought-conus/terraform.tfstate"
    region       = "us-west-2"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}
