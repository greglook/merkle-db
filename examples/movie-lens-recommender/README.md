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

### Load Phase

Constructing merkle-db tables from raw dataset input:

- take input CSV
- convert to RDD of rows
- map to pairRDD of [key record]
- sort by key
- split pairRDD into partition-sized chunks
- convert each chunk into a merkle-db partition node
- build index tree over partition-node metadata

### Read Phase

Performing computation over a merkle-db table:

Parallel of table ns methods for keys, scan, read which return an RDD instead of
a collection.

**TODO:** how does this fit dataframe? seems like that's almost more like the
table itself.


### Update Phase

Update an existing table with new raw data:

- take input data RDD
- map to pairRDD of [key updates]
- look up current partitions in table
- group-by partitions each update belongs to
- for each [id partition [key updates]], update the partition to produce
  one-or-more new partitions
- collect updated partitions and build a new index tree over them
