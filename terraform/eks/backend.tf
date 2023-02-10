terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.17.1"
    }
  }

  backend "s3" {
    bucket = "idata-poc-terraform-states"
    key    = "idata-poc/eks"
    region = "us-east-1"
  }
}

provider "aws" {
  region = "us-east-1"
}

data "terraform_remote_state" "vpc" {
  backend = "s3"
  config = {
    bucket = "idata-poc-terraform-states"
    key    = "idata-poc/vpc"
    region = "us-east-1"
  }
}