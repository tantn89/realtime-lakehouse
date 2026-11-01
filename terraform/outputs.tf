output "instance_id" {
  value = aws_instance.lakehouse.id
}

output "bucket" {
  value = aws_s3_bucket.lakehouse.id
}

output "athena_workgroup" {
  value = aws_athena_workgroup.lakehouse.name
}

locals {
  # aws_profile = null means the default credential chain: no --profile flag.
  ssm_session = "aws ssm start-session --target ${aws_instance.lakehouse.id} --region ${var.region}${var.aws_profile == null ? "" : " --profile ${var.aws_profile}"}"
}

output "shell" {
  description = "Interactive shell on the instance (needs the Session Manager plugin)."
  value       = local.ssm_session
}

output "dagster_ui" {
  description = "Forward the Dagster UI to http://localhost:3000 (needs the Session Manager plugin)."
  value       = "${local.ssm_session} --document-name AWS-StartPortForwardingSession --parameters portNumber=3000,localPortNumber=3000"
}
