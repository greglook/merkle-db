# Monitoring node for capturing benchmarking metrics.

### Security ###

resource "aws_security_group" "elb" {
  name        = "elb"
  description = "ELB security group"
  vpc_id      = "${aws_vpc.main.id}"

  tags {
    Name = "merkle-db-bench elb"
  }

  # http
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # https
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

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
    cidr_blocks = ["${aws_vpc.main.cidr_block}"]
  }

  # TODO: proxy this with ELB?
  ingress {
    description = "Riemann dashboard"
    from_port   = 4567
    to_port     = 4567
    protocol    = "tcp"
    cidr_blocks = ["${aws_vpc.main.cidr_block}"]
  }

  # TODO: proxy this with ELB?
  #ingress {
  #  description = "Grafana web interface"
  #  protocol    = "tcp"
  #  from_port   = 3000
  #  to_port     = 3000
  #  security_groups = ["${aws_security_group.elb.id}"]
  #}

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
  default = "..."
}

variable "monitor_instance_type" {
  description = "Instance type to use for the monitor instance"
  default = "c5.large"
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



### ELB ###

#resource "aws_elb" "monitor" {
#  name            = "merkle-db-bench"
#  security_groups = ["${aws_security_group.elb.id}"]
#  subnets         = ["${aws_subnet.support.id}"]
#  instances       = ["${aws_instance.monitor.id}"]
#  internal        = false
#
#  idle_timeout = 30
#  connection_draining = true
#  connection_draining_timeout = 300
#  cross_zone_load_balancing = false
#
#  tags {
#    Name = "merkle-db-bench"
#  }
#
#  # http
#  listener {
#    lb_port = 80
#    lb_protocol = "http"
#    instance_port = 3000
#    instance_protocol = "http"
#  }
#
#  health_check {
#    healthy_threshold = 2
#    unhealthy_threshold = 2
#    timeout = 15
#    target = "HTTP:3000/login"
#    interval = 60
#  }
#}
#
#output "monitor_elb_url" {
#  value = "${aws_elb.monitor.dns_name}"
#}
