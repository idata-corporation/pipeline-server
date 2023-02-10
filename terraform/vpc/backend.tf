terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.17.1"
    }
  }

  backend "s3" {
    bucket = "idata-poc-terraform-states"
    key    = "idata-poc/vpc"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
}