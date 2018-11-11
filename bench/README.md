MerkleDB Benchmark
==================

This directory holds some configuration and scripts for running benchmarks
against the current version of the MerkleDB code.


## Storage Setup

First, set up a long-lived S3 bucket which will hold:
- test dataset(s)
- riemann-jvm-profiler.jar
- solanum installer script
- published task uberjars
- directory for EMR job logs
- blocks

All but the first one could be put in an ephemeral S3 bucket which is part of
the TF state, or just set a lifecycle policy on those prefixes.


## Cluster Bootstrapping

The EMR cluster uses [bootstrap actions](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-bootstrap.html)
to set up some extra software. This pulls scripts from S3 and executes them on
each machine beforee the cluster starts. Specifically, this installs
[Solanum](https://github.com/greglook/solanum) and the related configuration to
perform host monitoring, and the [riemann-jvm-profiler](https://github.com/amperity/riemann-jvm-profiler)
jar.

In order to prepare for these steps, the correct scripts need to be uploaded to
the S3 data bucket being used:

- `s3://${var.s3_data_bucket}/bootstrap/install-solanum`
- `s3://${var.s3_data_bucket}/bootstrap/install-profiler`

Could use Terraform's `aws_s3_bucket_object` resource to ensure local scripts
are uploaded, but would it try to destroy them after?


## Riemann/Influx/Grafana

Set up an instance that has Riemann, riemann-dash, InfluxDB, and Grafana
installed. Use nginx to reverse-proxy the two dashboard endpoints. If we can set
up an SSH tunnel from this instance to the cluster master (or some kind of IPsec
tunnel?) then it should proxy the spark application pages as well.

This will only work well if there's a good way to edit the dashboards and then
save the updated configs for application to the next run... perhaps part of the
shutdown can be an export command, and part of the spin-up can be a programmatic
import.

- `/data/riemann/dashboards.json`
- `/data/grafana/dashboards/`


## Data Collection

There are three primary sources of metrics data we're interested in collecting:
- Host-level metrics like executor cpu, memory, disk, and network utilization.
- Application metrics about method calls, block IO, spark phases, etc.
- Profiler metrics from the `riemann-profiler.jar` agent.

The first two are amenable to graphing in Grafana - the last one probably isn't,
unless there's the right panel type for it. Can be exposed well in riemann-dash,
but that's a realtime view. Ideally, the riemann rules should log profiler data
to a file where it can be analyzed to generate a static flame graph for each
spark task phase after the run completes.


## Other Thoughts

- Script to manage submitting jobs to the EMR cluster and monitoring/recording
  the results.
- Better: unattended "build uberjar, publish, spin up cluster, run ten times, then shut down"
- Save test results in a rich format, since different tasks have different
  inputs/metrics.
- Use a terraform null-resource to run the final tunnel-setup script once the
  cluster is up?


## Load Test

- prepare fresh ref tracker and block store
- inputs are dataset name and table params
  - ! start time
  - ! repository HEAD commit
  - ! record dataset/table and table params
- load table as fast as possible
  - ! input data size
  - ! input data rows
  - ! load elapsed
  - ! table stats
  - during load(?) sample n records for later querying
  - ! sampled record ids
  - table/read the sampled records, check results
  - table/read some nonexistent records, check results
  - ! elapsed read times
