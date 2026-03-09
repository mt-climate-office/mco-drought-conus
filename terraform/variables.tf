variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "aws_profile" {
  description = "AWS SSO profile name (as in ~/.aws/config)"
  type        = string
  default     = "mco"
}

variable "aws_account_id" {
  description = "AWS account ID"
  type        = string
}

variable "project_name" {
  description = "Project name used for resource naming and tagging"
  type        = string
  default     = "mco-drought-conus"
}

variable "vpc_id" {
  description = "Existing VPC ID in which to place Fargate tasks and EFS mount targets"
  type        = string
}

variable "subnet_ids" {
  description = <<-EOT
    Subnet IDs for Fargate tasks and EFS mount targets.
    Must be public subnets with an Internet Gateway route when assign_public_ip = true.
    Leave empty to use all subnets in the VPC (discovered automatically).
  EOT
  type        = list(string)
  default     = []
}

variable "extra_security_group_ids" {
  description = "Existing security group IDs to attach to Fargate tasks in addition to the project-managed one"
  type        = list(string)
  default     = []
}

variable "assign_public_ip" {
  description = <<-EOT
    Assign a public IP to Fargate tasks.
    Required when tasks run in public subnets and need internet access (GridMET downloads).
    Set to false if tasks run in private subnets behind a NAT gateway.
  EOT
  type        = bool
  default     = true
}

variable "s3_bucket_name" {
  description = "S3 bucket name for public output data"
  type        = string
  default     = "mco-gridmet"
}

variable "schedule_time" {
  description = "Cron expression for the nightly run, evaluated in schedule_timezone"
  type        = string
  default     = "cron(0 18 * * ? *)" # 6:00 PM daily
}

variable "schedule_timezone" {
  description = "IANA timezone for the schedule"
  type        = string
  default     = "America/Denver"
}

variable "fargate_cpu" {
  description = "Fargate task CPU units (1 vCPU = 1024). Must be one of: 256, 512, 1024, 2048, 4096, 8192, 16384"
  type        = number
  default     = 8192 # 8 vCPU
}

variable "fargate_memory" {
  description = "Fargate task memory in MiB. For 8 vCPU, valid values: 16384–61440 in 4096 increments"
  type        = number
  default     = 32768 # 32 GB (nearest valid Fargate value above 30 GB for 8 vCPU)
}

variable "fargate_ephemeral_storage_gib" {
  description = "Fargate ephemeral storage in GiB (min 21, max 200). Holds GridMET cache + interim data during the run."
  type        = number
  default     = 200
}

variable "container_cores" {
  description = "Number of parallel cores passed to the R pipeline (CORES env var)"
  type        = number
  default     = 8
}

variable "gridmet_refresh_years" {
  description = "Number of recent years to force re-download on each run (GRIDMET_REFRESH_YEARS)"
  type        = number
  default     = 2
}

variable "start_year" {
  description = "Earliest year to download from GridMET (START_YEAR)"
  type        = number
  default     = 1991
}

variable "clim_periods" {
  description = "Climatological reference period specs (CLIM_PERIODS). Comma-separated; e.g. 'rolling:30,full'"
  type        = string
  default     = "rolling:30,full"
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 90
}
