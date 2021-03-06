MerkleDB Benchmark Cluster
==========================

This directory holds configuration and scripts for running benchmarking tasks
against the MerkleDB code. This is accomplished by spinning up an Elastic Map
Reduce (EMR) cluster in AWS along with some supporting infrastructure to capture
metrics from the running jobs.

![cluster archictecture](img/cluster-diagram.png)

The diagram above shows the high-level architecture of the cluster. An EMR
cluster with a single master and multiple worker nodes is paired with an EC2
instance which serves as a gateway and a monitor for the task metrics.


## Cluster Setup

Follow these steps to prepare a cluster for testing.

### Tools

In order to use these configs, you'll need the following tools installed:

- [aws-cli](https://aws.amazon.com/cli/)
- [terraform](https://www.terraform.io/)
- [ansible](https://www.ansible.com/)

You'll also need an AWS account and a corresponding IAM access keypair. Running
the cluster will cost some money, but it can be torn down once the tests are
complete.

### Data Storage

In order to hold the test data, task jars, and outputs, you will need to set up
an S3 bucket that will persist between runs. You will need to provide this bucket name to
Terraform; you can either do that at runtime or save it to a variable file in
`terraform/terraform.tfvars`:

```
# Custom benchmark settings
s3_data_bucket = "BUCKET_NAME"
```

You can also use this file to configure other variables such as the cluster
`master_type`, `worker_type`, and `cluster_size`.

### Bootstrap Resources

The EMR cluster uses [bootstrap actions](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-bootstrap.html)
to set up some extra software. This pulls scripts from S3 and executes them on
each machine beforee the cluster starts. Specifically, this installs
[Solanum](https://github.com/greglook/solanum) and the related configuration to
perform host monitoring, and the [riemann-jvm-profiler](https://github.com/amperity/riemann-jvm-profiler)
jar.

Clone the riemann-jvm-profiler repo and build the uberjar, then copy it into the
[bootstrap](bootstrap) directory.

```sh
$ git clone git@github.com:amperity/riemann-jvm-profiler.git
$ cd riemann-jvm-profiler
$ lein uberjar
$ cp target/riemann-jvm-profiler-0.1.0-standalone.jar ../bootstrap/riemann-jvm-profiler.jar
```

**TODO:** publish a release so this doesn't need to be built manually

### Build Infrastructure

Once ready, spin up the benchmark cluster with terraform:

```sh
$ cd terraform
$ terraform init
$ terraform apply
```

This will run for a while and create all of the necessary AWS resources. Part of
this process involves using Ansible to configure the monitor instance, which is
responsible for collecting metrics from the tests. Once the command returns, you
should see a few outputs including the names of the monitor and cluster master
instances:

```
Outputs:

cluster_id = j-XXXXXXXXXXXXX
cluster_master_dns = ip-XX-XX-XX-XX.us-west-2.compute.internal
monitor_instance = ec2-XX-XX-XX-XX.us-west-2.compute.amazonaws.com
```

Open the `monitor_instance` address in a browser and you should see the
benchmark landing page, with links to the various dashboards for the cluster.

### Connect SOCKS Proxy

The public dashboards allow for a read-only view of things, but in order to
fully interact with the cluster you will need to set up a SOCKS proxy to route
traffic into the VPC. In a terminal, run the following to open a dynamic tunnel:

```sh
$ ssh -N -D 8157 ubuntu@$(terraform output monitor_instance)
```

Now, you can either manually configure proxy settings in your browser or use a
plugin which will dynamically enable it such as
[FoxyProxy](https://addons.mozilla.org/en-US/firefox/addon/foxyproxy-standard/).


## Running Tests

Build an uberjar with the task code and upload it to the bucket:

```sh
$ aws s3 cp target/uberjar/task.jar s3://my-test-bucket/jars/task.jar
```

Write out a [JSON file](tasks/load-db.json) describing the task step to run,
then use the [run-task](run-task.sh) script to launch it:

```sh
$ ./run-task.sh tasks/load-db.json
```


## Local Development

For developing or running the benchmark locally, you can use
[docker-compose](https://docs.docker.com/compose/) to manage a cluster of
containers that provide the same monitoring functionality.

```sh
$ cd docker
$ docker-compose up -d
```

This will provide you with services on the following ports:
- InfluxDB (8086)
- Riemann (5555)
- [riemann-dash](http://localhost:4567) (4567)
- [Grafana](http://localhost:3000/) (3000)

If you're working on the Riemann rules streams, use this command to reload them:

```sh
$ docker-compose kill -s SIGHUP riemann
```
