resource "aws_dynamodb_table" "dataset" {
  name         = "${var.environment_name}-dataset"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "name"
  table_class  = "STANDARD"

  attribute {
    name = "name"
    type = "S"
  }

  tags = {
    Name = var.environment_name
  }
}

resource "aws_dynamodb_table" "archived-metadata" {
  name         = "${var.environment_name}-archived-metadata"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pipeline_token"
  table_class  = "STANDARD"

  attribute {
    name = "pipeline_token"
    type = "S"
  }

  tags = {
    Name = var.environment_name
  }
}

resource "aws_dynamodb_table" "dataset-status-summary" {
  name         = "${var.environment_name}-dataset-status-summary"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pipeline_token"
  range_key    = "created_at"
  table_class  = "STANDARD"

  attribute {
    name = "pipeline_token"
    type = "S"
  }

  attribute {
    name = "created_at"
    type = "N"
  }

  tags = {
    Name = var.environment_name
  }
}

resource "aws_dynamodb_table" "dataset-status" {
  name         = "${var.environment_name}-dataset-status"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pipeline_token"
  range_key    = "created_at"
  table_class  = "STANDARD"

  attribute {
    name = "pipeline_token"
    type = "S"
  }

  attribute {
    name = "created_at"
    type = "N"
  }

  tags = {
    Name = var.environment_name
  }
}

resource "aws_dynamodb_table" "file-notifier-message" {
  name         = "${var.environment_name}-file-notifier-message"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"
  table_class  = "STANDARD"

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  attribute {
    name = "id"
    type = "S"
  }

  tags = {
    Name = var.environment_name
  }
}

resource "aws_dynamodb_table" "data-pull" {
  name         = "${var.environment_name}-data-pull"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "dataset"
  table_class  = "STANDARD"

  attribute {
    name = "dataset"
    type = "S"
  }

  tags = {
    Name = var.environment_name
  }
}

resource "aws_dynamodb_table" "cdc-mapper" {
  name         = "${var.environment_name}-cdc-mapper"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "name"
  table_class  = "STANDARD"

  attribute {
    name = "name"
    type = "S"
  }

  tags = {
    Name = var.environment_name
  }
}

resource "aws_dynamodb_table" "cdc-last-read" {
  name         = "${var.environment_name}-cdc-last-read"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "name"
  table_class  = "STANDARD"

  attribute {
    name = "name"
    type = "S"
  }

  tags = {
    Name = var.environment_name
  }
}
