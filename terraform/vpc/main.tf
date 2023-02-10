locals {
  region = "us-east-1"
  tags = {
    Owner       = "idata"
    Environment = "poc"
    Terraform   = "true"
  }
}

################################################################################
# VPC Module
################################################################################

module "vpc" {
  source = "terraform-aws-modules/vpc/aws"

  name = "${var.environment_name}-vpc"
  cidr = "10.4.0.0/16"

  azs              = ["us-east-1a", "us-east-1b", "us-east-1c"]
  public_subnets   = ["10.4.10.0/24", "10.4.11.0/24", "10.4.12.0/24"]
  private_subnets  = ["10.4.20.0/24", "10.4.21.0/24", "10.4.22.0/24"]
  database_subnets = ["10.4.30.0/24", "10.4.31.0/24", "10.4.32.0/24"]

  enable_nat_gateway   = true
  single_nat_gateway   = true
  enable_dns_hostnames = true

  # one_nat_gateway_per_az = false
  # enable_vpn_gateway     = false
  # enable_dns_support   = true

  enable_ipv6                     = true
  assign_ipv6_address_on_creation = true
  create_egress_only_igw          = true

  public_subnet_ipv6_prefixes   = [0, 1, 2]
  private_subnet_ipv6_prefixes  = [3, 4, 5]
  database_subnet_ipv6_prefixes = [6, 7, 8]

  create_database_subnet_group = true

  public_subnet_tags = {
    "kubernetes.io/cluster/${var.environment_name}" = "shared"
    "kubernetes.io/role/elb"              = 1
  }

  private_subnet_tags = {
    "kubernetes.io/cluster/${var.environment_name}" = "shared"
    "kubernetes.io/role/internal-elb"     = 1
  }

  enable_flow_log                      = true
  create_flow_log_cloudwatch_iam_role  = true
  create_flow_log_cloudwatch_log_group = true

  tags = local.tags
}

################################################################################
# VPC Endpoints Module
################################################################################

module "vpc_endpoints" {
  source = "terraform-aws-modules/vpc/aws//modules/vpc-endpoints"

  vpc_id             = module.vpc.vpc_id
  security_group_ids = [aws_security_group.endpoints.id]

  endpoints = {
    s3 = {
      service = "s3"
      tags    = { Name = "s3-vpc-endpoint" }
    },
    dynamodb = {
      service         = "dynamodb"
      service_type    = "Gateway"
      route_table_ids = flatten([module.vpc.private_route_table_ids])
      #policy          = data.aws_iam_policy_document.dynamodb_endpoint_policy.json
      tags            = { Name = "dynamodb-vpc-endpoint" }
    },
    elasticmapreduce = {
      service             = "elasticmapreduce"
      private_dns_enabled = true
      subnet_ids          = module.vpc.private_subnets
    },
    ec2 = {
      service             = "ec2"
      private_dns_enabled = true
      subnet_ids          = module.vpc.private_subnets
      security_group_ids  = [aws_security_group.ec2.id]
    },
    emr-containers = {
      service             = "emr-containers"
      private_dns_enabled = true
      subnet_ids          = module.vpc.private_subnets
    },
    athena = {
      service             = "athena"
      private_dns_enabled = true
      subnet_ids          = module.vpc.private_subnets
    },
    sqs = {
      service             = "sqs"
      private_dns_enabled = true
      subnet_ids          = module.vpc.private_subnets
    }
    sns = {
      service             = "sns"
      private_dns_enabled = true
      subnet_ids          = module.vpc.private_subnets
    }
  }

  tags = merge(local.tags, {
    Endpoint = "true"
  })
}

################################################################################
# Supporting Resources
################################################################################

resource "aws_security_group" "endpoints" {
  name   = "endpoints"
  vpc_id = module.vpc.vpc_id

  ingress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = [module.vpc.vpc_cidr_block]
    ipv6_cidr_blocks = ["::/0"]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}

resource "aws_security_group" "ec2" {
  name_prefix = "${var.environment_name}-ec2"
  description = "Allow TLS inbound traffic"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description = "TLS from VPC"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = [module.vpc.vpc_cidr_block]
  }

  tags = local.tags
}

data "aws_iam_policy_document" "dynamodb_endpoint_policy" {
  statement {
    effect    = "Deny"
    actions   = ["dynamodb:*"]
    resources = ["*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "StringNotEquals"
      variable = "aws:SourceVpc"

      values = [module.vpc.vpc_id]
    }
  }
}

data "aws_iam_policy_document" "generic_endpoint_policy" {
  statement {
    effect    = "Deny"
    actions   = ["*"]
    resources = ["*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "StringNotEquals"
      variable = "aws:SourceVpc"

      values = [module.vpc.vpc_id]
    }
  }
}
