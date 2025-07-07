from memory_profiler import memory_usage
import networkx as nx
import subprocess
import time
import sys
import ray

def main():
    # Load from HDFS
    cat = subprocess.Popen(
        ["hadoop", "fs", "-cat", "/data/data_reddit_10M.csv"],
        stdout=subprocess.PIPE
    )

    lines = []
    skip_headers = True

    for raw_line in cat.stdout:
        line = raw_line.decode().strip()
        if not line or line.startswith("#"):
            continue
        if skip_headers:
            skip_headers = False
            continue
        parts = line.split(",")
        if len(parts) < 2:
            continue
        lines.append(f"{parts[0]} {parts[1]}")

    G = nx.parse_edgelist(lines, nodetype=str, create_using=nx.DiGraph())
    G = G.to_undirected()

    if len(sys.argv) > 1:
        num_chunks = int(sys.argv[1])
    else:
        num_chunks = 8  # Default

    node_list = list(G.nodes())
    node_chunk_size = len(node_list) // num_chunks

    node_chunks = [
        node_list[node_chunk_size * i : node_chunk_size * (i + 1)] if i < num_chunks - 1
        else node_list[node_chunk_size * i :]
        for i in range(num_chunks)
    ]

    ray.init()

    @ray.remote
    def compute_triangles(G, nodes):
        return nx.triangles(G, nodes=nodes)

    A = time.time()
    G_ref = ray.put(G)

    results = []
    for chunk in node_chunks:
        result = ray.get(compute_triangles.remote(G_ref, chunk))
        results.append(result)

    for idx, result in enumerate(results):
        print(f"Triangles in chunk {idx}: {result}")

    print("Total Time:", time.time() - A)

# Run main with peak memory measurement
mem_usage = memory_usage(main, interval=0.1, max_usage=True)
print(f"Peak memory usage: {mem_usage} MB")
