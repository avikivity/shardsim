#!/usr/bin/env python3

import random
import sys
from collections import defaultdict
import argparse


def analyze_vnodes(num_nodes: int, tokens_per_node: int = 256) -> dict:
    # Assign tokens to nodes
    all_tokens = []  # list of (token_value, node_id)

    for node_id in range(num_nodes):
        for _ in range(tokens_per_node):
            token = random.random()
            all_tokens.append((token, node_id))

    # Sort by token value
    all_tokens.sort(key=lambda x: x[0])

    # Find vnodes: gaps between adjacent tokens belonging to different nodes
    vnodes = []
    n = len(all_tokens)

    for i in range(n):
        current_node = all_tokens[i][1]
        next_idx = (i + 1) % n
        next_node = all_tokens[next_idx][1]

        if current_node != next_node:
            current_token = all_tokens[i][0]
            next_token = all_tokens[next_idx][0]

            # Handle wrap-around
            if next_idx == 0:
                gap = (1.0 - current_token) + next_token
            else:
                gap = next_token - current_token

            vnodes.append(gap)

    if not vnodes:
        return None

    avg = sum(vnodes) / len(vnodes)
    return {
        "num_nodes": num_nodes,
        "tokens_per_node": tokens_per_node,
        "total_tokens": num_nodes * tokens_per_node,
        "num_vnodes": len(vnodes),
        "smallest": min(vnodes),
        "average": avg,
        "largest": max(vnodes),
        "largest_to_smallest_ratio": max(vnodes) / min(vnodes),
    }


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Vnode analysis tool")
    parser.add_argument('--nodes', type=int, default=6, help='Number of nodes (default: 6)')
    parser.add_argument('--tokens-per-vnode', type=int, default=256, help='Tokens per vnode (default: 256)')
    args = parser.parse_args()

    num_nodes = args.nodes
    tokens_per_node = args.tokens_per_vnode

    print(f"Analyzing vnode distribution for {num_nodes} nodes, {tokens_per_node} tokens/node\n")

    results = analyze_vnodes(num_nodes, tokens_per_node)

    if results:
        total = results["total_tokens"]
        expected_vnode = 1.0 / results["num_vnodes"]
        print(f"  Total tokens:             {total}")
        print(f"  Number of vnodes:         {results['num_vnodes']}")
        print(f"  Expected avg vnode size:  {expected_vnode:.8f}")
        print(f"  Smallest vnode:           {results['smallest']:.8f}")
        print(f"  Average vnode:            {results['average']:.8f}")
        print(f"  Largest vnode:            {results['largest']:.8f}")
        print(f"  Largest/smallest ratio:   {results['largest_to_smallest_ratio']:.2f}x")


if __name__ == "__main__":
    main()
