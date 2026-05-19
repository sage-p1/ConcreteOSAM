import math
import argparse
import random
from oblivious_graph import build_from_networkx, benchmark
from smart_pointer import SmartPointer
from networkx import erdos_renyi_graph
from single_access_machine import sam, get_global_counter, print_stats_by_structure, print_max_write_batches
from typing import Set

def benchmark_single_alg(   
        n: int,
        d: int, 
        bs: int, 
        trials: int,
        pointer: str, 
        algorithm: str, 
        move_semantics: bool, 
        make_copies: bool, 
        prime_graph: bool, 
        static_insertion: bool,
        walk_length: int = 50,
        damping_factor: float = 0.9,
        dynamic_tests: bool = False
    ) -> None:
    """Build Erdős-Rényi graph and benchmark one traversal algorithm"""
    p = d / n
    if p > 1.0:
        print(f"Skipping test: p={p} > 1.0")
        return

    SmartPointer.set_smart_pointer(pointer)
    name = f"ER_n-{n}_d-{d}_bs-{bs}_trials-{trials}_pt-{pointer}_alg-{algorithm}_move-{move_semantics}_copies-{make_copies}_prime-{prime_graph}_staticinsertion-{static_insertion}"

    if algorithm == "rw":
        name += f"_wl-{walk_length}"
    elif algorithm == "pr":
        name += f"_wl-{walk_length}_df-{damping_factor}"
    print(f"----- Running test: {name} -----")
    bf = math.floor((bs - 2 * 8) / 8)

    # build oblivious graph
    ograph = build_from_networkx(    
        function=erdos_renyi_graph,
        name=name, 
        graph=None, 
        args=(n, p, 1), 
        branching_factor=bf,
        static_insertion=static_insertion, 
        move_semantics=move_semantics, 
        make_copies=make_copies, 
        prime_graph=prime_graph, 
        seed=1,
        connect_all=True, 
        queue_osam=True, 
        stack_osam=True, 
        avl_tree_osam=True, 
        random_weight_min=None, 
        random_weight_max=None, 
        sort_ids=False, 
        ordered_dynamic_vertices=False
    )

    print("-"*(len(name)+26))

    if not dynamic_tests:
        # benchmark graph on specified algorithm
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
            print_trace=False, 
            print_each_trial=False
        )
    
    else:
        # optionally perform a series of 
        # 1) vertex insertions
        # 2) edge insertions
        # 3) vertex deletions
        # 4) edge deletions
        # calculate the number of times a function will be called
        add_vertices_percentage = 0.05
        add_edges_percentage = 0.2
        delete_vertices_percentage = 0.05
        delete_edges_percentage = 0.2

        add_vertices_number = int(add_vertices_percentage*n)
        add_edges_number = int(add_edges_percentage*n*d)
        delete_vertices_number = int(delete_vertices_percentage*n)
        delete_edges_number = int(delete_edges_percentage*n*d)

        operations_by_function_list = [
            [add_vertices_number, "add_vertex"], 
            [add_edges_number, "add_edge"], 
            [delete_vertices_number, "delete_vertex"], 
            [delete_edges_number, "delete_edge"]
        ]
        
        indices = list(range(4)) # indices of operations to choose from
        present_nodes = list(range(n)) # list of all existing nodes

        # randomly iterate over each function until all are complete
        while operations_by_function_list:
            i = random.choice(indices)
            operations_by_function = operations_by_function_list[i]

            # match the operation
            match operations_by_function[1]:
                case "add_vertex":
                    # get list of unique neighbors
                    neighbors: Set[int] = set()
                    while len(neighbors) < d:
                        neighbors.add(random.choice(present_nodes))

                    # add vertex with d connections
                    ograph.add_vertex(
                        v_name=n, 
                        v_ids=ograph.v_ids, 
                        entry_points=ograph.entry_points,
                        out_degree=d, 
                        out_children=None, 
                        label=None,
                    )

                    present_nodes.append(n)
                    n += 1
                    
                case "add_edge":
                    # repeat until an insertion is successfully completed
                    is_inserted = False
                    alert_exists = False
                    while not is_inserted:
                        src = dst = random.choice(present_nodes)
                        while dst == src:
                            dst = random.choice(present_nodes)
                        if move_semantics:
                            is_inserted = ograph.add_edge(src, dst, 0, alert_exists, make_copies)
                        else:
                            is_inserted = ograph.add_edge_full_copy(src, dst, 0, alert_exists)

                case "delete_vertex":
                    # delete vertex
                    target = random.choice(present_nodes)
                    present_nodes.remove(target)
                    if move_semantics:
                        ograph.delete_vertex(target, make_copies)
                    else:
                        ograph.delete_vertex_full_copy(target)

                case "delete_edge":
                    # repeat until an insertion is successfully completed
                    is_deleted = False
                    alert_missing = False
                    while not is_deleted:
                        src = dst = random.choice(present_nodes)
                        while dst == src:
                            dst = random.choice(present_nodes)

                        if move_semantics:
                            is_deleted = ograph.delete_edge(src, dst, alert_missing, make_copies)
                        else:
                            is_deleted = ograph.add_edge_full_copy(src, dst, alert_missing)

            assert isinstance(operations_by_function[0], int)
            operations_by_function[0] -= 1
            if operations_by_function[0] <= 0:
                operations_by_function_list.pop(i)
                indices.pop()

        # print stats and headers
        print(f"Post dynamic addition and deletion stats {get_global_counter()}")
        print("Post dynamic addition and deletion structure stats:")
        add_vertices_tense = "vertices" if add_vertices_number != 1 else "vertex"
        add_edges_tense = "edges" if add_edges_number != 1 else "edge"
        delete_vertices_tense = "vertices" if delete_vertices_number != 1 else "vertex"
        delete_edge_tense = "edges" if delete_edges_number != 1 else "edge"
        print(f"Added {add_vertices_number} {add_vertices_tense} (with degree {d})")
        print(f"Added {add_edges_number} {add_edges_tense}")
        print(f"Deleted {delete_vertices_number} {delete_vertices_tense}")
        print(f"Deleted {delete_edges_number} {delete_edge_tense}")
        print_stats_by_structure()
        print_max_write_batches()
        print("-"*(len(name)+26))

        # choose random entry points here because ograph does not
        # track which of its indices have been deleted
        # that responsibility is left to the user 
        entry_point = next_entry_point = random.choice(present_nodes)
        while entry_point == next_entry_point:
            next_entry_point = random.choice(present_nodes)

        benchmark(  
            ograph=ograph, 
            name=name, 
            trials=trials,
            algorithm=algorithm,
            entry_point=entry_point, 
            next_entry_point=next_entry_point, 
            pick_random_entry_points=False,
            move_semantics=move_semantics,
            make_copies=make_copies, 
            walk_length=walk_length,
            damping_factor=damping_factor, 
            queue_osam=True, 
            stack_osam=True, 
            avl_tree_osam=True,
            print_trace=False, 
            print_each_trial=False
        )

    print(f"\n----- Finished test: {name} -----\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, required=True)
    parser.add_argument("--d", type=int, required=True)
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

    group5 = parser.add_mutually_exclusive_group(required=False)
    group5.add_argument("--dynamic_tests", action="store_true")

    args = parser.parse_args()

    benchmark_single_alg(args.n, args.d, args.bs, args.trials, args.pt, args.alg, args.move, args.copies, args.prime, args.staticinsertion, args.wl, args.df, args.dynamic_tests)
