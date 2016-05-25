#!/usr/bin/python3

import random, itertools, argparse
from pyinter import Interval, interval, IntervalSet
from collections import defaultdict

vnodes = 2560
shards = 12
nodes = 10

rand = random.Random()

class Vnode(object):
    def __init__(self, token, node):
        self._token = token
        self._node = node
    def token(self):
        return self._token
    def node(self):
        return self._node
    def __repr__(self):
        return '({} -> {})'.format(self._token, self._node)

# returns a list of Vnode objects for the input node; vnodes indicates
# vnode count
def make_node(node, vnodes):
    return sorted([Vnode(rand.uniform(0, 1), node) for x in range(vnodes)],
                  key=Vnode.token)


# constructs a sorted list of Vnode objects for a data center
def make_ring(nodes, vnodes):
    return sorted(itertools.chain.from_iterable([make_node(n, vnodes)
                                                 for n in range(nodes)]),
                  key=Vnode.token)

# pads a ring with fake Vnode objects at token 0 and token 1
# used to avoid wraparound
def pad_ring(ring):
    return ([Vnode(0., ring[0].node())]
            + ring
            + [Vnode(1., ring[-1].node())])

# converts a ring to a dict node->IntervalSet
def make_node_intervals(ring):
    ret = defaultdict(IntervalSet)
    ring = pad_ring(ring)
    pairs = itertools.zip_longest(ring[:-1], ring[1:])
    for pair in pairs:
        ival = interval.openclosed(pair[0].token(), pair[1].token())
        ret[pair[1].node()].add(ival)
    return ret

# returns a dict node->load for a given ring
def ring_loads(ring):
    ret = defaultdict(float)
    for node, iset in make_node_intervals(ring).items():
        for ival in iset:
            ret[node] += ival.upper_value - ival.lower_value
    return ret

# returns the node imbalance: the ratio between the node with the
# highest load to the node with the lowest load
def node_imbalance(ring):
    loads = ring_loads(ring).values()
    return max(loads) / min(loads)


# converts a ring to a dict (node, shard)->IntervalSet
def make_shard_intervals(ring):
    node_intervals = make_node_intervals(ring)
    shard_ranges = dict()
    for x in range(shards):
        shard_ranges[x] = interval.openclosed(x/shards, (x+1)/shards)
    ret = dict()
    for node, node_ivals in node_intervals.items():
        for shard, shard_ival in shard_ranges.items():
            iset = IntervalSet([shard_ival])
            ret[(node, shard)] = node_ivals.intersection(iset)
    return ret

# returns a dict (node, shard)->load for a given ring
def shard_loads(ring, shards):
    ret = defaultdict(float)
    for node_shard, iset in make_shard_intervals(ring).items():
        for ival in iset:
            ret[node_shard] += ival.upper_value - ival.lower_value
    return ret

# returns the node imbalance: the ratio between the node with the
# highest load to the node with the lowest load
def shard_imbalance(ring, shards):
    loads = shard_loads(ring, shards).values()
    return max(loads) / min(loads)


argp = argparse.ArgumentParser('Simulate Scylla cluster load imbalance')
argp.add_argument('--nodes', '-n', metavar='N', type=int, default=5, dest='nodes',
                  help='Number of nodes in the cluster')
argp.add_argument('--vnodes', '-v', metavar='N', type=int, default=32, dest='vnodes',
                  help='Number of vnodes per node')
argp.add_argument('--shards', '-s', metavar='N', type=int, default=12, dest='shards',
                  help='Number of shards per node')
opts = argp.parse_args()

nodes = opts.nodes 
vnodes = opts.vnodes
shards = opts.shards

print('{nodes} nodes, {vnodes} vnodes, {shards} shards'.format(**globals()))

ring = make_ring(nodes, vnodes)

print('maximum node imbalance:  {}'.format(node_imbalance(ring)))
print('maximum shard imbalance: {}'.format(shard_imbalance(ring, shards)))

