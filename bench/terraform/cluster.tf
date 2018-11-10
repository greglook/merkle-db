# EMR cluster resources


### IAM Cluster Role ###

data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "emr_cluster" {
  statement {
    sid = "ClusterActions"
    # TODO: lock these down
    actions = [
      "cloudwatch:*",
      "dynamodb:*",
      "ec2:Describe*",
      "elasticmapreduce:Describe*",
      "elasticmapreduce:ListBootstrapActions",
      "elasticmapreduce:ListClusters",
      "elasticmapreduce:ListInstanceGroups",
      "elasticmapreduce:ListInstances",
      "elasticmapreduce:ListSteps",
      "kinesis:CreateStream",
      "kinesis:DeleteStream",
      "kinesis:DescribeStream",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:MergeShards",
      "kinesis:PutRecord",
      "kinesis:SplitShard",
      "rds:Describe*",
      "s3:*",
      "sdb:*",
      "sns:*",
      "sqs:*",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role" "emr_cluster" {
  name = "merkle-db.emr.cluster"
  assume_role_policy = "${data.aws_iam_policy_document.ec2_assume_role.json}"
}

resource "aws_iam_role_policy" "emr_cluster" {
  name   = "cluster-actions"
  role   = "${aws_iam_role.emr_cluster.id}"
  policy = "${data.aws_iam_policy_document.emr_cluster.json}"
}

resource "aws_iam_instance_profile" "emr_cluster" {
  name = "merkle-db.emr.cluster"
  role = "${aws_iam_role.emr_cluster.name}"
}



### IAM EMR Role ###

data "aws_iam_policy_document" "emr_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type = "Service"
      identifiers = ["elasticmapreduce.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "emr_service" {
  statement {
    sid = "EMRActions"
    # TODO: lock these down
    actions = [
      "ec2:AuthorizeSecurityGroupEgress",
      "ec2:AuthorizeSecurityGroupIngress",
      "ec2:CancelSpotInstanceRequests",
      "ec2:CreateNetworkInterface",
      #"ec2:CreateSecurityGroup",
      "ec2:CreateTags",
      "ec2:DeleteNetworkInterface",
      #"ec2:DeleteSecurityGroup",
      "ec2:DeleteTags",
      "ec2:Describe*",
      "ec2:DetachNetworkInterface",
      "ec2:ModifyImageAttribute",
      "ec2:ModifyInstanceAttribute",
      "ec2:RequestSpotInstances",
      "ec2:RevokeSecurityGroupEgress",
      "ec2:RunInstances",
      "ec2:TerminateInstances",
      "ec2:DeleteVolume",
      "ec2:DetachVolume",
      "iam:GetRole",
      "iam:GetRolePolicy",
      "iam:ListInstanceProfiles",
      "iam:ListRolePolicies",
      "iam:PassRole",
      #"s3:CreateBucket",
      "s3:Get*",
      "s3:List*",
      "sdb:BatchPutAttributes",
      "sdb:Select",
      #"sqs:CreateQueue",
      "sqs:Delete*",
      "sqs:GetQueue*",
      "sqs:PurgeQueue",
      "sqs:ReceiveMessage",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role" "emr_service" {
  name = "merkle-db.emr.service"
  assume_role_policy = "${data.aws_iam_policy_document.emr_assume_role.json}"
}

resource "aws_iam_role_policy" "emr_service" {
  name   = "merkle-db.emr.service"
  role   = "${aws_iam_role.emr_service.id}"
  policy = "${data.aws_iam_policy_document.emr_service.json}"
}



### Networking ###

resource "aws_security_group" "master" {
  name        = "merkle-db.emr.master"
  description = "EMR cluster master security group"
  vpc_id      = "${aws_vpc.main.id}"

  #lifecycle {
  #  ignore_changes = ["ingress", "egress"]
  #}

  # TODO: lock this down
  ingress {
    description = "Allow all traffic"
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
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

resource "aws_security_group" "worker" {
  name        = "merkle-db.emr.worker"
  description = "EMR cluster worker security group"
  vpc_id      = "${aws_vpc.main.id}"

  #depends_on = ["aws_subnet.cluster"]

  #lifecycle {
  #  ignore_changes = ["ingress", "egress"]
  #}

  # TODO: lock this down
  ingress {
    description = "Allow all traffic"
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
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



### EMR Cluster ###

variable "master_type" {
  description = "Instance size to use for the EMR cluster master"
  default = "m3.xlarge"
}

variable "worker_type" {
  description = "Instance size to use for the EMR cluster workers"
  default = "m3.xlarge"
}

variable "worker_count" {
  description = "Instance size to use for the EMR cluster workers"
  default = 3
}

resource "aws_emr_cluster" "benchmark" {
  name          = "merkledb-benchmark"
  release_label = "emr-4.6.0"
  applications  = ["Spark"]

  ec2_attributes {
    subnet_id                         = "${aws_subnet.cluster.id}"
    emr_managed_master_security_group = "${aws_security_group.master.id}"
    emr_managed_slave_security_group  = "${aws_security_group.worker.id}"
    instance_profile                  = "${aws_iam_instance_profile.emr_cluster.arn}"
  }

  # TODO: vars
  master_instance_type = "${var.master_type}"
  core_instance_type   = "${var.worker_type}"
  core_instance_count  = "${var.worker_count}"

  #bootstrap_action {
  #  path = "s3://elasticmapreduce/bootstrap-actions/run-if"
  #  name = "runif"
  #  args = ["instance.isMaster=true", "echo running on master node"]
  #}

  # ???
  configurations = "test-fixtures/emr_configurations.json"

  service_role = "${aws_iam_role.emr_service.arn}"
}
