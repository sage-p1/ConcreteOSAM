import math
import argparse
import random
import pandas as pd
from oblivious_graph import build_from_networkx, benchmark
from smart_pointer import SmartPointer
from networkx import erdos_renyi_graph
import networkx as nx
from single_access_machine import sam, get_global_counter, print_stats_by_structure, print_max_write_batches

def benchmark_graph_from_csv(   
        graph_name: str,
        directed: bool,
        file: str,
        bs: int, 
        trials: int,
        pointer: str, 
        algorithm: str, 
        move_semantics: bool, 
        make_copies: bool, 
        prime_graph: bool, 
        static_insertion: bool,
        walk_length: int = 50,
        damping_factor: float = 0.9
    ) -> None:
    """Build graph from csv and benchmark one traversal algorithm"""
    csv = pd.read_csv(
        file,
        sep=",",
        names=["start_node", "end_node"],
    )
    if directed:
        graph = nx.from_pandas_edgelist(csv, source="start_node", target="end_node", create_using=nx.DiGraph())
    else:
        graph = nx.from_pandas_edgelist(csv, source="start_node", target="end_node", create_using=nx.Graph())

    SmartPointer.set_smart_pointer(pointer)
    name = f"{graph_name}_bs-{bs}_trials-{trials}_pt-{pointer}_alg-{algorithm}_move-{move_semantics}_copies-{make_copies}_prime-{prime_graph}_staticinsertion-{static_insertion}"
    
    if algorithm == "rw":
        name += f"_wl-{walk_length}"
    elif algorithm == "pr":
        name += f"_wl-{walk_length}_df-{damping_factor}"
    print(f"----- Running test: {name} -----")
    bf = math.floor((bs - 2 * 8) / 8)

    # build oblivious graph
    ograph = build_from_networkx(    
        function=None,
        name=name, 
        graph=graph, 
        args=None, 
        branching_factor=bf,
        static_insertion=static_insertion, 
        move_semantics=move_semantics, 
        make_copies=make_copies, 
        prime_graph=prime_graph, 
        seed=1,
        connect_all=False, 
        queue_osam=True, 
        stack_osam=True, 
        avl_tree_osam=True, 
        random_weight_min=None, 
        random_weight_max=None, 
        sort_names=False, 
        ordered_dynamic_vertices=False
    )

    print("-"*(len(name)+26))

    # benchmark graph on specified algorithm
    vertex_names = list(graph.nodes)
    benchmark(  
        ograph=ograph, 
        name=name, 
        trials=trials,
        algorithm=algorithm,
        entry_point=None, 
        next_entry_point=None, 
        pick_random_entry_points=True,
        move_semantics=move_semantics,
        make_copies=make_copies, 
        walk_length=walk_length,
        damping_factor=damping_factor, 
        queue_osam=True, 
        stack_osam=True, 
        avl_tree_osam=True,
        vertex_names=vertex_names,
        print_trace=False, 
        print_each_trial=False
    )
    
    print(f"\n----- Finished test: {name} -----\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--graph_name", type=str, required=True)

    group0 = parser.add_mutually_exclusive_group(required=True)
    group0.add_argument("--directed", action="store_true")
    group0.add_argument("--undirected", action="store_false")

    parser.add_argument("--file", type=str, required=True)
    parser.add_argument("--bs", type=int, required=True)
    parser.add_argument("--trials", type=int, required=True)
    parser.add_argument("--pt", type=str, required=True)
    parser.add_argument("--alg", type=str, required=True)
    
    group1 = parser.add_mutually_exclusive_group(required=True)
    group1.add_argument("--move", action="store_true")
    group1.add_argument("--no_move", action="store_false")

    group2 = parser.add_mutually_exclusive_group(required=True)
    group2.add_argument("--copies", action="store_true")
    group2.add_argument("--no_copies", action="store_false")

    group3 = parser.add_mutually_exclusive_group(required=True)
    group3.add_argument("--prime", action="store_true")
    group3.add_argument("--no_prime", action="store_false")

    group4 = parser.add_mutually_exclusive_group(required=True)
    group4.add_argument("--staticinsertion", action="store_true")
    group4.add_argument("--no_staticinsertion", action="store_false")

    parser.add_argument("--wl", type=int, required=False, default=50)
    parser.add_argument("--df", type=float, required=False, default=0.9)

    args = parser.parse_args()

    benchmark_graph_from_csv(args.graph_name, args.directed, args.file, args.bs, args.trials, args.pt, args.alg, args.move, args.copies, args.prime, args.staticinsertion, args.wl, args.df)
    