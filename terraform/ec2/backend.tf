terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.17.1"
    }
  }

  backend "s3" {
    bucket = "idata-poc-terraform-states"
    key    = "idata-poc/ec2"
    region = "us-east-1"
  }
}

provider "aws" {
  region = "us-east-1"
}