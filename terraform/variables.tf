variable "region" {
  description = "AWS region for every resource."
  type        = string
  default     = "ap-southeast-2"
}

variable "aws_profile" {
  description = "AWS CLI profile Terraform authenticates with (null = default credential chain)."
  type        = string
  default     = "lakehouse"
}

variable "instance_type" {
  description = "One instance runs Redpanda, the producer, Spark and Dagster: 4 vCPU / 16 GB."
  type        = string
  default     = "t3.xlarge"
}

variable "max_hours" {
  description = "The instance powers itself off after this many hours, even if nobody runs destroy."
  type        = number
  default     = 6
}

variable "budget_usd" {
  description = "Monthly cost budget for the account; alerts at 80% actual and 100% forecast."
  type        = number
  default     = 5
}

variable "budget_email" {
  description = "Where budget alerts go. Set it in terraform.tfvars (git-ignored)."
  type        = string
}

variable "bundle_path" {
  description = "Source bundle built from the repo: git archive --format=tar.gz -o build/lakehouse.tar.gz HEAD"
  type        = string
  default     = "../build/lakehouse.tar.gz"
}
