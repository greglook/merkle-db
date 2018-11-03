MerkleDB Benchmark
==================

This directory holds some configuration and scripts for running benchmarks
against the current version of the MerkleDB code.


## Storage Setup

First, set up a long-lived S3 bucket which will hold:
- test dataset(s)
- riemann-profiler.jar
- published task uberjars
- directory for EMR job logs
- blocks

All but the first one could be put in an ephemeral S3 bucket which is part of
the TF state, or just set a lifecycle policy on those prefixes.


## Building AMIs

First, need to build at least two AMIs. Have this scripted - look into Packer,
maybe? Could just use Ansible as well.

### Spark Worker

**TODO:** can we even provide a custom AMI for this? If not, need to figure out
good bootstrapping steps.

- Need to provide the `riemann-profiler.jar` on the host somewhere that the job
  can instrument itself with.
- Ideally, run Solanum on each worker to report host-level resource utilization.

### Riemann/Influx/Grafana

Pre-build an AMI that has Riemann, riemann-dash, InfluxDB, and Grafana
installed. Use nginx to reverse-proxy the two dashboard endpoints.  Potentially
proxy to the spark master as well if that's feasible?

This will only work well if there's a good way to edit the dashboards and then
save the updated configs for application to the next run... perhaps part of the
shutdown can be an export command, and part of the spin-up can be a programmatic
import.


## Cluster Lifecycle

Once the AMIs are ready, set various Terraform variables and `terraform up` to
get the cluster running. This should also spin up a monitoring instance and
configure it for viewing.

When all the experiments are done, use `terraform destroy` to clean up.


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

- Terraform config for setting up an entire test VPC and EMR cluster.
- Script to manage submitting jobs to the EMR cluster and monitoring/recording
  the results.
- Better: unattended "build uberjar, publish, spin up cluster, run ten times, then shut down"
- Save test results in a rich format, since different tasks have different
  inputs/metrics.


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
