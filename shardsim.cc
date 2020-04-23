
#include <boost/icl/interval.hpp>
#include <boost/icl/interval_set.hpp>
#include <random>
#include <iostream>
#include <tuple>
#include <set>
#include <unordered_map>
#include <boost/range.hpp>
#include <boost/range/irange.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/combine.hpp>
#include <boost/program_options.hpp>
#include <boost/format.hpp>

using namespace std;
using namespace boost;
using namespace boost::icl;
using namespace boost::adaptors;

unsigned shards;
unsigned ignorebits;

struct node {
    unsigned id;
    bool operator<(node x) const {
        return id < x.id;
    }
    bool operator==(node x) const {
        return id == x.id;
    }
};

namespace std {
template <>
struct hash<node> {
    size_t operator()(node x) const { return hash<unsigned>()(x.id); }
};
}

struct vnode {
    double token;
    node owner;
    std::tuple<double, node> as_tuple() const {
        return std::make_tuple(token, owner);
    }
    bool operator<(vnode x) const {
        return as_tuple() < x.as_tuple();
    }
    bool operator==(vnode x) const {
        return as_tuple() == x.as_tuple();
    }
};

double
random_token() {
    static auto dist = std::uniform_real_distribution<double>(0, 1);
    static auto re = std::default_random_engine(std::random_device()());
    return dist(re);
}

set<vnode>
make_node(node node, unsigned vnodes) {
    return copy_range<set<vnode>>(
            irange(0u, vnodes)
            | transformed([&] (unsigned n) { return vnode{random_token(), node}; }));
}

set<vnode>
make_ring(unsigned nodes, unsigned vnodes) {
    auto ret = set<vnode>();
    for (auto n : irange(0u, nodes)) {
        auto nd = make_node(node{n}, vnodes);
        for (auto vn : nd) {
            ret.insert(vn);
        }
    }
    return ret;
}

set<vnode>
pad_ring(set<vnode> ring) {
    // pads a ring with fake Vnode objects at token 0 and token 1
    // used to avoid wraparound
    ring.insert(vnode{0, ring.rbegin()->owner});
    ring.insert(vnode{1, ring.begin()->owner});
    return ring;
}

// converts a ring to a map node->interval_set
unordered_map<node, interval_set<double>>
make_node_intervals(set<vnode> ring) {
    auto ret = unordered_map<node, interval_set<double>>();
    ring = pad_ring(ring);
    auto left_bounds = make_iterator_range(ring.begin(), std::prev(ring.end()));
    auto right_bounds = make_iterator_range(std::next(ring.begin()), ring.end());
    for (auto pair : combine(left_bounds, right_bounds)) {
        auto ival = interval<double>::left_open(get<0>(pair).token, get<1>(pair).token);
        ret[get<1>(pair).owner].insert(ival);
    }
    return ret;
}

// returns a map node->load for a given ring
unordered_map<node, double>
ring_loads(set<vnode> ring) {
    auto ret = unordered_map<node, double>();
    for (auto node_iset : make_node_intervals(ring)) {
        auto&& node = node_iset.first;
        auto&& iset = node_iset.second;
        ret[node] = 0;
        for (auto ival : iset) {
            ret[node] += ival.upper()- ival.lower();
        }
    }
    return ret;
}

// returns the node overcommit: the ratio between the node with the
// highest load to the "average node"
double
node_overcommit(set<vnode> ring) {
    auto loads = ring_loads(ring);
    return *max_element(loads | map_values) * loads.size();
}

// converts a ring to a map (node, shard)->IntervalSet
std::map<std::tuple<node, unsigned>, interval_set<double>>
make_shard_intervals_static(set<vnode> ring) {
    auto node_intervals = make_node_intervals(ring);
    auto shard_ranges = std::map<unsigned, interval_set<double>>();
    auto delta = 1. / (shards * (1<<ignorebits));
    auto pos = 0.;
    for (auto rep : irange(0, 1<<ignorebits)) {
        for (auto x : irange(0u, shards)) {
            shard_ranges[x].add(interval<double>::left_open(pos, pos + delta));
            pos += delta;
        }
    }
    auto ret = std::map<std::tuple<node, unsigned>, interval_set<double>>();
    for (auto node_node_ivals : node_intervals) {
        auto&& node = node_node_ivals.first;
        auto&& node_ivals = node_node_ivals.second;
        for (auto shard_shard_iset : shard_ranges) {
            auto&& shard = shard_shard_iset.first;
            auto&& shard_iset = shard_shard_iset.second;
            ret[std::make_tuple(node, shard)] = node_ivals & shard_iset;
        }
    }
    return ret;
}

using make_shard_interval_fn = std::function<map<std::tuple<node, unsigned>, interval_set<double>> (set<vnode>)>;

// returns a map (node, shard)->load for a given ring
// make_shard_intervals: lambda: ring return ((node, shard)->IntervalSet)
map<std::tuple<node, unsigned>, double>
shard_loads(set<vnode> ring, unsigned shards, make_shard_interval_fn make_shard_intervals) {
    auto ret = map<std::tuple<node, unsigned>, double>();
    for (auto node_shard_iset : make_shard_intervals(ring)) {
        auto&& node_shard = node_shard_iset.first;
        auto&& iset = node_shard_iset.second;
        ret[node_shard] = 0;
        for (auto ival : iset) {
            ret[node_shard] += ival.upper() - ival.lower();
        }
    }
    return ret;
}

// returns the shard overcommit: the ratio between the shard with the
// highest load to the "average" shard
// make_shard_intervals: lambda: ring return ((node, shard)->IntervalSet)
double
shard_overcommit(set<vnode> ring, unsigned shards, make_shard_interval_fn make_shard_intervals) {
    auto loads = shard_loads(ring, shards, make_shard_intervals);
    return *max_element(loads | map_values) * loads.size();
}

auto make_shard_intervals = make_shard_intervals_static;

auto algorithms = std::unordered_map<std::string, make_shard_interval_fn>({
    {"static", make_shard_intervals_static},
});

namespace bpo = boost::program_options;

int main(int ac, char** av) {
    try {
        auto desc = bpo::options_description("Simulate Scylla cluster load imbalance");
        desc.add_options()
                ("nodes,n", bpo::value<unsigned>()->default_value(5u), "Number of nodes in the cluster")
                ("vnodes,v", bpo::value<unsigned>()->default_value(32u), "Number of vnodes per node")
                ("shards,s", bpo::value<unsigned>()->default_value(12u), "Number of shards per node")
                ("ignore-msb-bits,b", bpo::value<unsigned>()->default_value(8), "Number of token MSB bits to ignore for sharding")
                ("algorithm,a", bpo::value<string>()->default_value("static"),
                        "select sharding algorithm ({})" /*'.format(algorithms.keys()) */)
                ("help,h", "show this help")
                ;
        auto vm = bpo::variables_map();
        bpo::store(bpo::parse_command_line(ac, av, desc), vm);
        notify(vm);
        if (vm.count("help")) {
            std::cout << desc << "\n";
            return 1;
        }
        auto nodes = vm["nodes"].as<unsigned>();
        auto vnodes = vm["vnodes"].as<unsigned>();
        shards = vm["shards"].as<unsigned>();
        ignorebits = vm["ignore-msb-bits"].as<unsigned>();
        auto make_shard_intervals = algorithms[vm["algorithm"].as<string>()];
        std::cout << boost::format("%d nodes, %d vnodes, %d shards\n") % nodes % vnodes % shards;

        auto ring = make_ring(nodes, vnodes);

        std::cout << boost::format("maximum node overcommit:  %g\n") % node_overcommit(ring);
        std::cout << boost::format("maximum shard overcommit: %f\n") % shard_overcommit(ring, shards, make_shard_intervals);
        return 0;
    } catch (bpo::error& e) {
        std::cout << "Bad parameters, try --help (" << e.what() << ")\n";
        return 1;
    } catch (...) {
        std::cout << "something bad happened\n";
        return 1;
    }
}

