# Monitoring node for capturing benchmarking metrics.

### Security ###

resource "aws_security_group" "monitor" {
  name        = "monitor"
  description = "Benchmark monitor server"
  vpc_id      = "${aws_vpc.main.id}"

  tags {
    Name = "merkle-db monitor"
  }

  ingress {
    description = "SSH"
    protocol    = "tcp"
    from_port   = 22
    to_port     = 22
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "HTTP"
    protocol    = "tcp"
    from_port   = 80
    to_port     = 80
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Riemann metrics"
    protocol    = "tcp"
    from_port   = 5555
    to_port     = 5555
    cidr_blocks = ["${aws_vpc.main.cidr_block}"]
  }

  ingress {
    description = "Riemann websockets"
    from_port   = 5556
    to_port     = 5556
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "Internet"
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }
}



### Monitor Instance ###

variable "monitor_ami" {
  description = "Base AMI to use for the monitor instance"
  default = "ami-0afae182eed9d2b46"
}

variable "monitor_instance_type" {
  description = "Instance type to use for the monitor instance"
  default = "c5.2xlarge"
}

resource "aws_instance" "monitor" {
  ami           = "${var.monitor_ami}"
  instance_type = "${var.monitor_instance_type}"
  subnet_id     = "${aws_subnet.support.id}"
  key_name      = "${aws_key_pair.benchmark.id}"
  ebs_optimized = true

  vpc_security_group_ids = ["${aws_security_group.monitor.id}"]
  associate_public_ip_address = true

  tags {
    Name = "merkle-db monitor"
  }

  user_data = <<EOF
#!/usr/bin/env bash
echo $(hostname --ip) monitor >> /etc/hosts
echo monitor > /etc/hostname
hostname -F /etc/hostname
EOF
}
