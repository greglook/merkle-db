# Main VPC infrastructure resources


### Provider Config ###

variable "region" {
  description = "AWS region to operate in"
  default = "us-west-2"
}

provider "aws" {
  region = "us-west-2"
}



### SSH Key Pair ###

variable "key_name" {
  description = "Desired name of AWS key pair"
  default     = "mantle"
}

variable "public_key_path" {
  description = "Path to SSH public key."
  default     = "~/.ssh/id_rsa.pub"
}

variable "private_key_path" {
  description = "Path to SSH private key."
  default     = "~/.ssh/id_rsa"
}

resource "aws_key_pair" "terraform_key_pair" {
  key_name   = "${var.key_name}"
  public_key = "${file(var.public_key_path)}"
}



### VPC ###

variable "vpc_cidr" {
  description = "CIDR block for VPC."
  default     = "10.248.0.0/16"
}

variable "emr_subnet_cidr" {
  description = "CIDR block for EMR cluster hosts."
  default     = "10.248.8.0/24"
}

variable "support_subnet_cidr" {
  description = "CIDR block for supporting hosts."
  default     = "10.248.16.0/24"
}

resource "aws_vpc" "main" {
  cidr_block = "${var.vpc_cidr}"
  enable_dns_hostnames = true

  tags {
    name = "merkle-db-bench"
  }
}

resource "aws_subnet" "emr" {
  vpc_id     = "${aws_vpc.main.id}"
  cidr_block = "${var.emr_subnet_cidr}"

  tags {
    name = "merkle-db-bench emr"
  }
}

resource "aws_subnet" "support" {
  vpc_id     = "${aws_vpc.main.id}"
  cidr_block = "${var.support_subnet_cidr}"

  tags {
    name = "merkle-db-bench support"
  }
}



### Networking ###

resource "aws_internet_gateway" "main" {
  vpc_id = "${aws_vpc.main.id}"
}

resource "aws_route_table" "main" {
  vpc_id = "${aws_vpc.main.id}"

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = "${aws_internet_gateway.main.id}"
  }
}

resource "aws_main_route_table_association" "main" {
  vpc_id         = "${aws_vpc.main.id}"
  route_table_id = "${aws_route_table.main.id}"
}
