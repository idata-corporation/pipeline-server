provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)

  exec {
    api_version = "client.authentication.k8s.io/v1alpha1"
    command     = "aws"
    # This requires the awscli to be installed locally where Terraform is executed
    args = ["eks", "get-token", "--cluster-name", module.eks.cluster_id]
  }
}

locals {
  cluster_version = "1.22"
  region          = "us-east-1"

  tags = {
    GithubRepo = "terraform-aws-eks"
    GithubOrg  = "terraform-aws-modules"
  }
}

data "aws_caller_identity" "current" {}

################################################################################
# EKS Module
################################################################################

module "eks" {
  source = "terraform-aws-modules/eks/aws"

  cluster_name                    = var.environment_name
  cluster_version                 = local.cluster_version
  cluster_endpoint_private_access = true
  cluster_endpoint_public_access  = true

  enable_irsa = true

  cluster_addons = {
    coredns = {
      resolve_conflicts = "OVERWRITE"
    }
    kube-proxy = {}
    vpc-cni = {
      resolve_conflicts = "OVERWRITE"
    }
  }

  cluster_encryption_config = [{
    provider_key_arn = aws_kms_key.eks.arn
    resources        = ["secrets"]
  }]

  cluster_tags = {
    # This should not affect the name of the cluster primary security group
    # Ref: https://github.com/terraform-aws-modules/terraform-aws-eks/pull/2006
    # Ref: https://github.com/terraform-aws-modules/terraform-aws-eks/pull/2008
    Name = var.environment_name
  }

  vpc_id     = data.terraform_remote_state.vpc.outputs.vpc_id
  subnet_ids = data.terraform_remote_state.vpc.outputs.private_subnets

  # Extend cluster security group rules
  cluster_security_group_additional_rules = {
    egress_nodes_ephemeral_ports_tcp = {
      description                = "To node 1025-65535"
      protocol                   = "tcp"
      from_port                  = 1025
      to_port                    = 65535
      type                       = "egress"
      source_node_security_group = true
    }
  }

  # Extend node-to-node security group rules
  node_security_group_additional_rules = {
    ingress_self_all = {
      description = "Node to node all ports/protocols"
      protocol    = "-1"
      from_port   = 0
      to_port     = 0
      type        = "ingress"
      self        = true
    }
    egress_all = {
      description      = "Node all egress"
      protocol         = "-1"
      from_port        = 0
      to_port          = 0
      type             = "egress"
      cidr_blocks      = ["0.0.0.0/0"]
      ipv6_cidr_blocks = ["::/0"]
    }
  }

# aws-auth configmap
  manage_aws_auth_configmap = true

  aws_auth_node_iam_role_arns_non_windows = [
    module.eks_managed_node_group.iam_role_arn,
  ]
  aws_auth_fargate_profile_pod_execution_role_arns = [
    module.fargate_profile.fargate_profile_pod_execution_role_arn
  ]

  aws_auth_roles = [
    {
      rolearn  = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/aws-service-role/emr-containers.amazonaws.com/AWSServiceRoleForAmazonEMRContainers"
      username = "emr-containers"
    },
  ]

  aws_auth_users = [
    {
      userarn  = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:user/github_action"
      username = "github"
      groups   = ["system:masters"]
    },
    {
      userarn  = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:user/tfearn"
      username = "tfearn"
      groups   = ["system:masters"]
    },
  ]

  tags = local.tags
}
module "eks_managed_node_group" {
  source = "terraform-aws-modules/eks/aws//modules/eks-managed-node-group"

  name            = "separate-eks-mng"
  cluster_name    = module.eks.cluster_id
  cluster_version = module.eks.cluster_version

  vpc_id     = data.terraform_remote_state.vpc.outputs.vpc_id
  subnet_ids = data.terraform_remote_state.vpc.outputs.private_subnets

  cluster_primary_security_group_id = module.eks.cluster_primary_security_group_id

  vpc_security_group_ids = [
    module.eks.cluster_security_group_id,
  ]

  min_size     = 1
  max_size     = 3
  desired_size = 1

  instance_types = ["m5.4xlarge"]
  capacity_type  = "ON_DEMAND"

  labels = {
    Environment = "poc"
    GithubRepo  = "terraform-aws-eks"
    GithubOrg   = "terraform-aws-modules"
  }

  update_config = {
    max_unavailable_percentage = 50 # or set `max_unavailable`
  }

  #iam_role_additional_policies = [aws_iam_policy.node_additional.arn]

  tags = merge(local.tags, { Separate = "eks-managed-node-group" })
}

module "fargate_profile" {
  source = "terraform-aws-modules/eks/aws//modules/fargate-profile"

  name         = "separate-fargate-profile"
  cluster_name = module.eks.cluster_id

  subnet_ids = data.terraform_remote_state.vpc.outputs.private_subnets

  selectors = [
    {
      namespace = "kube-system"
      labels = {
        k8s-app = "kube-dns"
      }
    },
    {
      namespace = "default"
    },
    {
      namespace = "emr-containers"
    }
  ]

  tags = local.tags

  timeouts = {
    create = "10m"
    delete = "10m"
  }
}

# References to resources that do not exist yet when creating a cluster will cause a plan failure due to https://github.com/hashicorp/terraform/issues/4149
# There are two options users can take
# 1. Create the dependent resources before the cluster => `terraform apply -target <your policy or your security group> and then `terraform apply`
#   Note: this is the route users will have to take for adding additonal security groups to nodes since there isn't a separate "security group attachment" resource
# 2. For addtional IAM policies, users can attach the policies outside of the cluster definition as demonstrated below
resource "aws_iam_role_policy_attachment" "additional" {
  policy_arn = aws_iam_policy.node_additional.arn
  role       = module.eks_managed_node_group.iam_role_name
}


################################################################################
# Supporting Resources
################################################################################

module "vpc_cni_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 4.12"

  role_name_prefix      = "VPC-CNI-IRSA"
  attach_vpc_cni_policy = true
  vpc_cni_enable_ipv6   = false

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:aws-node"]
    }
  }

  tags = local.tags
}

resource "aws_security_group" "additional" {
  name_prefix = "${var.environment_name}-additional"
  vpc_id      = data.terraform_remote_state.vpc.outputs.vpc_id

  ingress {
    from_port = 22
    to_port   = 22
    protocol  = "tcp"
    cidr_blocks = [
      "10.0.0.0/8",
      "172.16.0.0/12",
      "192.168.0.0/16",
    ]
  }

  tags = local.tags
}

resource "aws_kms_key" "eks" {
  description             = "EKS Secret Encryption Key"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = local.tags
}

resource "aws_kms_key" "ebs" {
  description             = "Customer managed key to encrypt EKS managed node group volumes"
  deletion_window_in_days = 7
  policy                  = data.aws_iam_policy_document.ebs.json
}

# This policy is required for the KMS key used for EKS root volumes, so the cluster is allowed to enc/dec/attach encrypted EBS volumes
data "aws_iam_policy_document" "ebs" {
  # Copy of default KMS policy that lets you manage it
  statement {
    sid       = "Enable IAM User Permissions"
    actions   = ["kms:*"]
    resources = ["*"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
  }

  # Required for EKS
  statement {
    sid = "Allow service-linked role use of the CMK"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:DescribeKey"
    ]
    resources = ["*"]

    principals {
      type = "AWS"
      identifiers = [
        "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/aws-service-role/autoscaling.amazonaws.com/AWSServiceRoleForAutoScaling", # required for the ASG to manage encrypted volumes for nodes
        module.eks.cluster_iam_role_arn,                                                                                                            # required for the cluster / persistentvolume-controller to create encrypted PVCs
      ]
    }
  }

  statement {
    sid       = "Allow attachment of persistent resources"
    actions   = ["kms:CreateGrant"]
    resources = ["*"]

    principals {
      type = "AWS"
      identifiers = [
        "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/aws-service-role/autoscaling.amazonaws.com/AWSServiceRoleForAutoScaling", # required for the ASG to manage encrypted volumes for nodes
        module.eks.cluster_iam_role_arn,                                                                                                            # required for the cluster / persistentvolume-controller to create encrypted PVCs
      ]
    }

    condition {
      test     = "Bool"
      variable = "kms:GrantIsForAWSResource"
      values   = ["true"]
    }
  }
}

# This is based on the LT that EKS would create if no custom one is specified (aws ec2 describe-launch-template-versions --launch-template-id xxx)
# there are several more options one could set but you probably dont need to modify them
# you can take the default and add your custom AMI and/or custom tags
#
# Trivia: AWS transparently creates a copy of your LaunchTemplate and actually uses that copy then for the node group. If you DONT use a custom AMI,
# then the default user-data for bootstrapping a cluster is merged in the copy.

resource "aws_launch_template" "external" {
  name_prefix            = "external-eks-ex-"
  description            = "EKS managed node group external launch template"
  update_default_version = true

  block_device_mappings {
    device_name = "/dev/xvda"

    ebs {
      volume_size           = 100
      volume_type           = "gp2"
      delete_on_termination = true
    }
  }

  monitoring {
    enabled = true
  }

  # Disabling due to https://github.com/hashicorp/terraform-provider-aws/issues/23766
  # network_interfaces {
  #   associate_public_ip_address = false
  #   delete_on_termination       = true
  # }

  # if you want to use a custom AMI
  # image_id      = var.ami_id

  # If you use a custom AMI, you need to supply via user-data, the bootstrap script as EKS DOESNT merge its managed user-data then
  # you can add more than the minimum code you see in the template, e.g. install SSM agent, see https://github.com/aws/containers-roadmap/issues/593#issuecomment-577181345
  # (optionally you can use https://registry.terraform.io/providers/hashicorp/cloudinit/latest/docs/data-sources/cloudinit_config to render the script, poc: https://github.com/terraform-aws-modules/terraform-aws-eks/pull/997#issuecomment-705286151)
  # user_data = base64encode(data.template_file.launch_template_userdata.rendered)

  tag_specifications {
    resource_type = "instance"

    tags = {
      Name      = "external_lt"
      CustomTag = "Instance custom tag"
    }
  }

  tag_specifications {
    resource_type = "volume"

    tags = {
      CustomTag = "Volume custom tag"
    }
  }

  tag_specifications {
    resource_type = "network-interface"

    tags = {
      CustomTag = "EKS poc"
    }
  }

  tags = {
    CustomTag = "Launch template custom tag"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "tls_private_key" "this" {
  algorithm = "RSA"
}

resource "aws_key_pair" "this" {
  key_name_prefix = var.environment_name
  public_key      = tls_private_key.this.public_key_openssh

  tags = local.tags
}

resource "aws_security_group" "remote_access" {
  name_prefix = "${var.environment_name}-remote-access"
  description = "Allow remote SSH access"
  vpc_id      = data.terraform_remote_state.vpc.outputs.vpc_id

  ingress {
    description = "SSH access"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = local.tags
}

resource "aws_iam_policy" "node_additional" {
  name        = "${var.environment_name}-additional"
  description = "poc usage of node additional policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "*",
        ]
        Effect   = "Allow"
        Resource = "*"
      }
    ]
  })

  tags = local.tags
}

data "aws_ami" "eks_default" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amazon-eks-node-${local.cluster_version}-v*"]
  }
}

data "aws_ami" "eks_default_arm" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amazon-eks-arm64-node-${local.cluster_version}-v*"]
  }
}

data "aws_ami" "eks_default_bottlerocket" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["bottlerocket-aws-k8s-${local.cluster_version}-x86_64-*"]
  }
}
