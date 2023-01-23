resource "aws_s3_account_public_access_block" "block_public" {
  block_public_acls       = true
  block_public_policy     = true
  restrict_public_buckets = true
  ignore_public_acls      = true
}

## config bucket
resource "aws_s3_bucket" "config" {
  bucket = "${var.environment_name}-config"

  tags = {
    Name = var.environment_name
  }
}

resource "aws_s3_bucket_cors_configuration" "config" {
  bucket = aws_s3_bucket.config.bucket

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "PUT", "POST"]
    allowed_origins = ["*"]
    expose_headers  = ["Connection", "Server", "Date"]
    max_age_seconds = 3600
  }
}

## raw bucket
resource "aws_s3_bucket" "raw" {
  bucket = "${var.environment_name}-raw"

  tags = {
    Name = var.environment_name
  }
}

## raw-plus
resource "aws_s3_bucket" "raw-plus" {
  bucket = "${var.environment_name}-raw-plus"

  tags = {
    Name = var.environment_name
  }
}

## temp
resource "aws_s3_bucket" "temp" {
  bucket = "${var.environment_name}-temp"

  tags = {
    Name = var.environment_name
  }
}

## staging
resource "aws_s3_bucket" "staging" {
  bucket = "${var.environment_name}-staging"

  tags = {
    Name = var.environment_name
  }
}