data "aws_vpc" "existing" {
  id = var.vpc_id
}

data "aws_subnets" "vpc" {
  filter {
    name   = "vpc-id"
    values = [var.vpc_id]
  }
}
