# Shardsim - ScyllaDB / Apache Cassandra sharding simulator

## Background

Both [ScyllaDB](https://www.scylladb.com) and [Apache Cassandra](https://cassandra.apache.org) automatically distribute data across nodes based on a randomized algorithm. ScyllaDB in addition distributes data within a node among cores (using a static algorithm). These distributions are susceptible to over-utilization of a node or a core; since a cluster in general runs at the speed of the slowest node, this has significant applications on throughput.

Shardsim is a program that simulates the node- and core- data distribution algorithm with various parameters.

The parameters are:

 - `--nodes` - the number of nodes in the cluster. The simulation assumes RF=1 and no data centers.
 - `--vnodes` - the number of vnodes the database was configured with
 - `--shards` - the number of shards (logical cores) per node (ScyllaDB specific)
 - `--ignore-msb-bits` - ScyllaDB parameter to adjust the sharding algorithm to reduce shard over-utilization (to be described in a future blog post)
 
## Building

`shardsim` requires a C++ compiler, cmake, and boost to be installed.

```sh
cmake .
make
./shardsim
```

## Examples

12-node cluster with 32 vnodes, 24 logical cores, old ScyllaDB sharding algorithm:

```
$ ./shardsim --nodes 12 --vnodes 32 --shards 24 --ignore-msb-bits 0
12 nodes, 32 vnodes, 24 shards
maximum node overcommit:  1.32249
maximum shard overcommit: 5.302944
```

Some poor node is overcommitted by 32% over the average, and a single logical core is overcommitted 5X! Let's fix it by using 256 vnodes and the new ScyllaDB sharding algorithm:

```
$ ./shardsim --nodes 12 --vnodes 256 --shards 24 --ignore-msb-bits 12
12 nodes, 256 vnodes, 24 shards
maximum node overcommit:  1.06922
maximum shard overcommit: 1.088612
```

Success! Node overcommit is just 7% over the average, while shard overcommit is just 9%.

## Trademark notice

Apache Cassandra® and Apache® are either trademarks or registered trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a> in the United States and/or other countries.
