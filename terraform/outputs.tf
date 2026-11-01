output "instance_id" {
  value = aws_instance.lakehouse.id
}

output "bucket" {
  value = aws_s3_bucket.lakehouse.id
}

output "athena_workgroup" {
  value = aws_athena_workgroup.lakehouse.name
}

output "shell" {
  description = "Interactive shell on the instance (needs the Session Manager plugin)."
  value       = "aws ssm start-session --target ${aws_instance.lakehouse.id} --region ${var.region} --profile ${var.aws_profile}"
}

output "dagster_ui" {
  description = "Forward the Dagster UI to http://localhost:3000 (needs the Session Manager plugin)."
  value       = "aws ssm start-session --target ${aws_instance.lakehouse.id} --region ${var.region} --profile ${var.aws_profile} --document-name AWS-StartPortForwardingSession --parameters portNumber=3000,localPortNumber=3000"
}
