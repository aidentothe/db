# AWS Infrastructure for Spark + S3 Integration
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# VPC for Spark cluster
resource "aws_vpc" "spark_vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "spark-cluster-vpc"
  }
}

# Internet Gateway
resource "aws_internet_gateway" "spark_igw" {
  vpc_id = aws_vpc.spark_vpc.id

  tags = {
    Name = "spark-cluster-igw"
  }
}

# Public subnet
resource "aws_subnet" "spark_public_subnet" {
  vpc_id                  = aws_vpc.spark_vpc.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = true

  tags = {
    Name = "spark-public-subnet"
  }
}

# Route table
resource "aws_route_table" "spark_public_rt" {
  vpc_id = aws_vpc.spark_vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.spark_igw.id
  }

  tags = {
    Name = "spark-public-rt"
  }
}

# Route table association
resource "aws_route_table_association" "spark_public_rta" {
  subnet_id      = aws_subnet.spark_public_subnet.id
  route_table_id = aws_route_table.spark_public_rt.id
}

# Security group for Spark cluster
resource "aws_security_group" "spark_sg" {
  name        = "spark-cluster-sg"
  description = "Security group for Spark cluster"
  vpc_id      = aws_vpc.spark_vpc.id

  # SSH access
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Spark Master Web UI
  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Spark Master
  ingress {
    from_port   = 7077
    to_port     = 7077
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  # Spark Worker Web UI
  ingress {
    from_port   = 8081
    to_port     = 8081
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Spark History Server
  ingress {
    from_port   = 18080
    to_port     = 18080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Backend API
  ingress {
    from_port   = 5000
    to_port     = 5000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # All outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "spark-cluster-sg"
  }
}

# IAM role for EC2 instances
resource "aws_iam_role" "spark_ec2_role" {
  name = "spark-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

# IAM policy for S3 access
resource "aws_iam_policy" "spark_s3_policy" {
  name        = "spark-s3-policy"
  description = "Policy for Spark to access S3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.spark_data_bucket.arn,
          "${aws_s3_bucket.spark_data_bucket.arn}/*"
        ]
      }
    ]
  })
}

# Attach policy to role
resource "aws_iam_role_policy_attachment" "spark_s3_policy_attachment" {
  role       = aws_iam_role.spark_ec2_role.name
  policy_arn = aws_iam_policy.spark_s3_policy.arn
}

# Instance profile
resource "aws_iam_instance_profile" "spark_instance_profile" {
  name = "spark-instance-profile"
  role = aws_iam_role.spark_ec2_role.name
}

# S3 bucket for data storage
resource "aws_s3_bucket" "spark_data_bucket" {
  bucket = "${var.project_name}-spark-data-${random_string.bucket_suffix.result}"
}

resource "random_string" "bucket_suffix" {
  length  = 8
  special = false
  upper   = false
}

# S3 bucket versioning
resource "aws_s3_bucket_versioning" "spark_data_bucket_versioning" {
  bucket = aws_s3_bucket.spark_data_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Key pair for SSH access
resource "aws_key_pair" "spark_key" {
  key_name   = "spark-cluster-key"
  public_key = file(var.public_key_path)
}

# EC2 instance for Spark master
resource "aws_instance" "spark_master" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.master_instance_type
  key_name               = aws_key_pair.spark_key.key_name
  vpc_security_group_ids = [aws_security_group.spark_sg.id]
  subnet_id              = aws_subnet.spark_public_subnet.id
  iam_instance_profile   = aws_iam_instance_profile.spark_instance_profile.name

  user_data = base64encode(templatefile("${path.module}/user-data.sh", {
    s3_bucket = aws_s3_bucket.spark_data_bucket.bucket
  }))

  tags = {
    Name = "spark-master"
    Type = "master"
  }
}

# Data source for Ubuntu AMI
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}