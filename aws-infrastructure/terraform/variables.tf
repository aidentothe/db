variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-west-2"
}

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "db-spark"
}

variable "master_instance_type" {
  description = "EC2 instance type for Spark master"
  type        = string
  default     = "t3.large"
}

variable "worker_instance_type" {
  description = "EC2 instance type for Spark workers"
  type        = string
  default     = "t3.medium"
}

variable "worker_count" {
  description = "Number of Spark worker nodes"
  type        = number
  default     = 2
}

variable "public_key_path" {
  description = "Path to the public key file for SSH access"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}