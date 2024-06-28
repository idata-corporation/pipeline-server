### ECR

resource "aws_ecr_repository" "idata-pipeline" {
  name                 = "idata-pipeline"
  image_tag_mutability = "MUTABLE"

  tags = {
    project = "idata-pipeline"
  }
}

### EC2

resource "aws_key_pair" "deployer" {
  key_name   = "pipeline-server-key"
  public_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJugXaq7Kxcv48f947SGQr5XS1SBnMzipDvV/yAVXRxR tfearn@MacBook-Pro.local"
}

resource "aws_security_group" "pipeline_server_sg" {
  name   = "pipeline_server_sg"
  vpc_id = "vpc-008d58466f16764f0"

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = -1
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "idata-pipeline-server" {
  ami           = "ami-0eaf7c3456e7b5b68"
  instance_type = "t3.xlarge"

  subnet_id                   = "subnet-016ee18482730b0ee"
  vpc_security_group_ids      = [aws_security_group.pipeline_server_sg.id]
  key_name                    = aws_key_pair.deployer.key_name

  tags = {
    Name = "idata-pipeline"
  }
}