# Main VPC infrastructure resources


### Provider Config ###

variable "region" {
  description = "AWS region to operate in"
  default = "us-west-2"
}

variable "availability_zone" {
  description = "AWS availability zone to use within the region."
  default = "us-west-2c"
}

provider "aws" {
  region = "${var.region}"
}



### SSH Key Pair ###

variable "public_key_path" {
  description = "Path to SSH public key."
  default     = "~/.ssh/id_rsa.pub"
}

variable "private_key_path" {
  description = "Path to SSH private key."
  default     = "~/.ssh/id_rsa"
}

resource "aws_key_pair" "benchmark" {
  key_name   = "merkle-db-benchmark"
  public_key = "${file(var.public_key_path)}"
}



### VPC ###

variable "vpc_cidr" {
  description = "CIDR block for VPC."
  default     = "10.248.0.0/16"
}

variable "support_subnet_cidr" {
  description = "CIDR block for supporting hosts."
  default     = "10.248.1.0/24"
}

variable "cluster_subnet_cidr" {
  description = "CIDR block for EMR cluster hosts."
  default     = "10.248.16.0/20"
}

resource "aws_vpc" "main" {
  cidr_block = "${var.vpc_cidr}"
  enable_dns_hostnames = true

  tags {
    Name = "merkle-db-bench"
  }
}

resource "aws_subnet" "support" {
  vpc_id     = "${aws_vpc.main.id}"
  cidr_block = "${var.support_subnet_cidr}"

  availability_zone = "${var.availability_zone}"

  tags {
    Name = "merkle-db-bench support"
  }
}

resource "aws_subnet" "cluster" {
  vpc_id     = "${aws_vpc.main.id}"
  cidr_block = "${var.cluster_subnet_cidr}"

  availability_zone = "${var.availability_zone}"

  tags {
    Name = "merkle-db-bench cluster"
  }
}



### Networking ###

resource "aws_internet_gateway" "igw" {
  vpc_id = "${aws_vpc.main.id}"
}

resource "aws_eip" "nat" {
  vpc = true
}

resource "aws_nat_gateway" "ngw" {
  allocation_id = "${aws_eip.nat.id}"
  subnet_id     = "${aws_subnet.support.id}"

  depends_on = ["aws_internet_gateway.igw"]
}

resource "aws_route_table" "support" {
  vpc_id = "${aws_vpc.main.id}"

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = "${aws_internet_gateway.igw.id}"
  }

  tags {
    Name = "merkle-db-bench support"
  }
}

resource "aws_route_table" "cluster" {
  vpc_id = "${aws_vpc.main.id}"

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = "${aws_nat_gateway.ngw.id}"
  }

  tags {
    Name = "merkle-db-bench cluster"
  }
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id       = "${aws_vpc.main.id}"
  service_name = "com.amazonaws.${var.region}.s3"
}

resource "aws_vpc_endpoint_route_table_association" "cluster_s3" {
  route_table_id  = "${aws_route_table.cluster.id}"
  vpc_endpoint_id = "${aws_vpc_endpoint.s3.id}"
}

resource "aws_main_route_table_association" "main" {
  vpc_id         = "${aws_vpc.main.id}"
  route_table_id = "${aws_route_table.support.id}"
}

resource "aws_route_table_association" "cluster" {
  subnet_id      = "${aws_subnet.cluster.id}"
  route_table_id = "${aws_route_table.cluster.id}"
}
