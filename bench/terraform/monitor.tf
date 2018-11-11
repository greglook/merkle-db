# Monitoring node for capturing benchmarking metrics.


### Security Group ###

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

variable "monitor_instance_type" {
  description = "Instance type to use for the monitor instance"
  default = "c5.2xlarge"
}

data "aws_ami" "ubuntu" {
  owners = ["099720109477"] # Canonical
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-bionic-18.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_instance" "monitor" {
  ami           = "${data.aws_ami.ubuntu.id}"
  instance_type = "${var.monitor_instance_type}"
  key_name      = "${aws_key_pair.benchmark.id}"
  subnet_id     = "${aws_subnet.support.id}"

  vpc_security_group_ids = ["${aws_security_group.monitor.id}"]
  associate_public_ip_address = true

  tags {
    Name = "merkle-db monitor"
  }

  root_block_device = {
    volume_type           = "gp2"
    volume_size           = 256
    delete_on_termination = true
  }

  provisioner "remote-exec" {
    inline = [
      "sudo echo $(hostname --ip) monitor >> /etc/hosts",
      "sudo echo monitor > /etc/hostname",
      "sudo hostname -F /etc/hostname",
      "sudo apt-get install -y python python-apt",
    ]

    connection {
      type        = "ssh"
      user        = "ubuntu"
      private_key = "${file(var.private_key_path)}"
    }
  }

  provisioner "local-exec" {
    command = "cd ../ansible; ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook -u ubuntu -i '${aws_instance.monitor.public_ip},' monitor.yml"
  }

  depends_on = ["aws_internet_gateway.igw"]
}

output "monitor_instance_dns" {
  value = "${aws_instance.monitor.public_dns}"
}
