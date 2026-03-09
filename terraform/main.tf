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
  # backend "s3" {
  #   bucket  = "mco-terraform-state"
  #   key     = "mco-drought-conus/terraform.tfstate"
  #   region  = "us-west-2"
  #   profile = "mco"
  # }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}
