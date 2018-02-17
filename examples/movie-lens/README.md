MovieLens Recommender
=====================

This project demonstrates the usage of Apache Spark with MerkleDB as the backing
data store.


## Setup

Running this project requires a copy of the [MovieLens
Dataset](http://movielens.org/), which can be downloaded
[here](http://files.grouplens.org/datasets/movielens/). To get started, fetch
and extract a copy of the dataset locally:

```shell
$ mkdir data && cd data
$ wget http://files.grouplens.org/datasets/movielens/ml-latest.zip
$ unzip ml-latest.zip
$ cd ..
```


## Loading Data

Now that we have the raw data available, the next step is to load it into a set
of MerkleDB tables and combine them into a database.

```shell
$ lein run load-db data/ml-latest
```


## Parameter Discovery

...


## Model Training

...


## Making Recommendations

...


## References

- https://github.com/jadianes/spark-movie-lens/blob/master/notebooks/building-recommender.ipynb
- https://stackoverflow.com/questions/30446706/impementing-custom-spark-rdd-in-java


## Notes

Need to demonstrate:

### Versioning

- actually save the db versions in a ref tracker

### Update Phase

Update an existing table with new raw data:

- take input data RDD
- map to pairRDD of [key updates]
- look up current partitions in table
- group-by partitions each update belongs to
- for each [id partition [key updates]], update the partition to produce
  one-or-more new partitions
- collect updated partitions and build a new index tree over them

### Cluster Deployment

Demonstrate running the job on EMR against data in S3.

- Terraform config needed?
- Host uberjar in S3, pick up settings from environment
- Host input data in S3
- Host ref tracker in DynamoDB?
