# Short-lived AWS run of the lakehouse: the same compose stack on one EC2 instance, writing
# Iceberg tables to S3 through the Glue Data Catalog, queried from Athena.
#   terraform apply  ->  run and measure  ->  terraform destroy

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  bucket     = "realtime-lakehouse-${local.account_id}-${var.region}"
  # Iceberg namespaces become Glue databases. Declared here so destroy removes them, with the
  # tables Spark registered in them.
  namespaces = ["bronze", "silver", "ops"]
  bundle_key = "bundle/lakehouse.tar.gz"
}

# --- Storage: one bucket for the warehouse, the code bundle and Athena results ---------------

resource "aws_s3_bucket" "lakehouse" {
  bucket        = local.bucket
  force_destroy = true # throwaway run: destroy deletes the data too
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "bundle" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = local.bundle_key
  source = var.bundle_path
  etag   = filemd5(var.bundle_path)
}

# --- Catalog ---------------------------------------------------------------------------------

resource "aws_glue_catalog_database" "namespace" {
  for_each = toset(local.namespaces)
  name     = each.key
}

resource "aws_athena_workgroup" "lakehouse" {
  name          = "realtime-lakehouse"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true
    bytes_scanned_cutoff_per_query  = 1073741824 # 1 GB: a runaway query costs at most half a cent

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lakehouse.id}/athena-results/"
    }
  }
}

# --- Identity: the instance's role, the only credentials in the run ---------------------------

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "instance" {
  name               = "realtime-lakehouse-instance"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

data "aws_iam_policy_document" "lakehouse_access" {
  statement {
    sid       = "WarehouseBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lakehouse.arn]
  }
  statement {
    sid       = "WarehouseObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lakehouse.arn}/*"]
  }
  statement {
    # Spark's GlueCatalog and DuckDB (through Glue's Iceberg REST endpoint) manage and read
    # tables in the lakehouse databases only.
    sid = "GlueCatalog"
    actions = [
      "glue:GetCatalog", "glue:GetCatalogs",
      "glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase",
      "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable",
      "glue:DeleteTable",
    ]
    resources = concat(
      ["arn:aws:glue:${var.region}:${local.account_id}:catalog"],
      [for ns in local.namespaces : "arn:aws:glue:${var.region}:${local.account_id}:database/${ns}"],
      [for ns in local.namespaces : "arn:aws:glue:${var.region}:${local.account_id}:table/${ns}/*"],
    )
  }
}

resource "aws_iam_role_policy" "lakehouse_access" {
  name   = "lakehouse-access"
  role   = aws_iam_role.instance.id
  policy = data.aws_iam_policy_document.lakehouse_access.json
}

# Shell access through SSM: no SSH key, no inbound port.
resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.instance.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "instance" {
  name = "realtime-lakehouse-instance"
  role = aws_iam_role.instance.name
}

# --- Compute ---------------------------------------------------------------------------------

data "aws_ssm_parameter" "ubuntu" {
  name = "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
}

data "aws_vpc" "default" {
  default = true
}

resource "aws_security_group" "instance" {
  name        = "realtime-lakehouse"
  description = "Outbound only: Wikimedia stream, Docker Hub, AWS APIs. No inbound."
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "lakehouse" {
  ami                                  = data.aws_ssm_parameter.ubuntu.value
  instance_type                        = var.instance_type
  iam_instance_profile                 = aws_iam_instance_profile.instance.name
  vpc_security_group_ids               = [aws_security_group.instance.id]
  associate_public_ip_address          = true # outbound internet without a NAT gateway
  instance_initiated_shutdown_behavior = "stop"

  metadata_options {
    http_tokens = "required" # IMDSv2 only
    # Containers sit one network hop behind the host; they need the instance credentials.
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = 40
    encrypted   = true
  }

  user_data = templatefile("${path.module}/user-data.sh.tftpl", {
    region      = var.region
    bucket      = aws_s3_bucket.lakehouse.id
    bundle_key  = local.bundle_key
    account_id  = local.account_id
    max_minutes = var.max_hours * 60
  })
  user_data_replace_on_change = true

  tags = {
    Name = "realtime-lakehouse"
  }

  depends_on = [aws_s3_object.bundle, aws_glue_catalog_database.namespace]
}

# --- Cost guard ------------------------------------------------------------------------------

resource "aws_budgets_budget" "monthly" {
  name         = "realtime-lakehouse"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_email]
  }
}
