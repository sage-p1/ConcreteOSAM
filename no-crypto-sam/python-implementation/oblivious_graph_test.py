from smart_pointer import SmartPointer
from single_access_machine import get_global_counter, print_stats_by_structure, print_max_write_batches, reset_write_batches
from networkx import karate_club_graph, erdos_renyi_graph, DiGraph, Graph, connected_components, get_edge_attributes, write_edgelist
from typing import Dict, Set
from oblivious_graph import OGraph, build_from_networkx, benchmark
import random

"""
Test algorithms for ensuring the OGraph algorithms correctly
Uses Karate Club graph: https://networkx.org/documentation/stable/auto_examples/graph/plot_karate_club.html 
"""

def test_ograph(trials: int = 10, seed: int = 1) -> None:
    """Test building and traversing an Oblivious Graph"""
    # parameters to test on
    graph = karate_club_graph()
    name = "Karate Club"
    queue_osam = True
    stack_osam = True
    avl_tree_osam = True
    prime_graph = True
    branching_factor = 3
    pointers = {"original": [False, True], "multiwrite": [True], "recursive": [True]}
    algorithms = ["bfs", "dfs", "prim", "dijkstra", "random walk", "contact discovery", "directed triangle count", "pagerank"]
    make_copies_plural = [True, False]
    static_insertion_plural = [True, False]

    # iterate over every SmartPointer setting
    for pointer, move_semantics_plural in pointers.items():
        SmartPointer.set_smart_pointer(pointer)
        for move_semantics in move_semantics_plural:
            # toggle eager smart-copying
            for make_copies in make_copies_plural:
                # toggle static and dynamic insertion
                for static_insertion in static_insertion_plural:

                    # initialize ograph
                    ograph = build_from_networkx(    
                        function=karate_club_graph,
                        name=name, 
                        graph=None, 
                        args=None, 
                        branching_factor=branching_factor,
                        static_insertion=static_insertion, 
                        move_semantics=move_semantics, 
                        make_copies=make_copies,
                        prime_graph=prime_graph,
                        seed=seed, 
                        connect_all=True, 
                        queue_osam=queue_osam,
                        stack_osam=stack_osam,
                        avl_tree_osam=avl_tree_osam,
                        random_weight_min=None, 
                        random_weight_max=None, 
                        sort_names=False, 
                        ordered_dynamic_vertices=True
                    )
                    
                    # call graph algorithm
                    for alg in algorithms:
                        traces = benchmark(  
                            ograph=ograph, 
                            name=name, 
                            trials=trials, 
                            algorithm=alg,
                            entry_point=0, 
                            next_entry_point=0, 
                            pick_random_entry_points=True,
                            move_semantics=move_semantics, 
                            make_copies=make_copies, 
                            walk_length=50,
                            damping_factor=0.9, 
                            queue_osam=queue_osam,
                            stack_osam=stack_osam,
                            avl_tree_osam=avl_tree_osam,
                            print_trace=True, 
                            print_each_trial=False
                        )
                        
                        # ensure the number of expected vertices appears
                        for i in range(34):
                            # print(i, ograph.ogV_counts[i], len(list(graph.neighbors(i))) + 1)
                            assert ograph.ogV_counts[i] == len(list(graph.neighbors(i))) + 1

def test_ograph_edge_deletion(fraction: float = 0.1, seed: int = 1) -> None:
    """Test deleting edges in an Oblivious Graph"""
    # parameters to test on
    graph = karate_club_graph()
    name = "Karate Club"
    queue_osam = True
    stack_osam = True
    avl_tree_osam = True
    prime_graph = True
    branching_factor = 3
    pointers = {"original": [False, True], "multiwrite": [True], "recursive": [True]}
    make_copies_plural = [True, False]
    static_insertion_plural = [True, False]

    # pick random edges to delete
    deleted_edges = []
    present_edges = list(graph.edges)
    num_edges_to_remove = int(len(present_edges)*fraction)
    for i in range(num_edges_to_remove):
        index = random.randint(0, len(present_edges)-1-i)
        deleted_edges.append(present_edges.pop(index))

    # create dictionary of deleted edges 
    deleted_edges_dict: Dict[int, Set[int]] = dict()
    for edge in deleted_edges:
        src_name, dst_name = edge
        if src_name in deleted_edges_dict:
            deleted_edges_dict[src_name].add(dst_name)
        else:
            deleted_edges_dict[src_name] = {dst_name}

        if dst_name in deleted_edges_dict:
            deleted_edges_dict[dst_name].add(src_name)
        else:
            deleted_edges_dict[dst_name] = {src_name}

    # iterate over every SmartPointer setting
    for pointer, move_semantics_plural in pointers.items():
        SmartPointer.set_smart_pointer(pointer)
        for move_semantics in move_semantics_plural:
            # toggle eager smart-copying
            for static_insertion in static_insertion_plural:
                # toggle static and dynamic insertion
                for make_copies in make_copies_plural:
                    # initialize ograph
                    ograph = build_from_networkx(    
                        function=karate_club_graph,
                        name=name, 
                        graph=None, 
                        args=None, 
                        branching_factor=branching_factor,
                        static_insertion=static_insertion, 
                        move_semantics=move_semantics, 
                        make_copies=make_copies,
                        prime_graph=prime_graph,
                        seed=seed, 
                        connect_all=True, 
                        queue_osam=queue_osam,
                        stack_osam=stack_osam,
                        avl_tree_osam=avl_tree_osam,
                        random_weight_min=None, 
                        random_weight_max=None, 
                        sort_names=False, 
                        ordered_dynamic_vertices=True
                    )
                    
                    reset_write_batches()

                    # delete every edge selected for deletion
                    for edge in deleted_edges:
                        src_name, dst_name = edge
                        print(f"Deleting edge {src_name}->{dst_name}")
                        print(f"Deleting edge {dst_name}->{src_name}")
                        if move_semantics:
                            ograph.delete_edge(src_name, dst_name)     
                            ograph.delete_edge(dst_name, src_name)        
                        else:
                            ograph.delete_edge_full_copy(src_name, dst_name)     
                            ograph.delete_edge_full_copy(dst_name, src_name)    
                    
                    print(f"\nPost delete edge stats {get_global_counter()}", flush=True)
                    print_stats_by_structure(flush=True)
                    print_max_write_batches(flush=True)
                    reset_write_batches()

                    # call add_neighbors to search OutEdge tree and ensure deletions occurred
                    for src_name in deleted_edges_dict:
                        dst_names = deleted_edges_dict[src_name]
                        present_neighbors: Set[int] = set()
                        p_src = ograph.entry_points[ograph.v_ids[src_name]]
                        if move_semantics:
                            ograph.add_neighbors(p_src, "SET", present_neighbors, "DST_ID", None, make_copies)
                        else:
                            ogV = SmartPointer.get_and_copy(p_src)
                            ograph.add_neighbors_full_copy(p_src, ogV, "SET", present_neighbors, "DST_ID", None)
                        assert not present_neighbors & dst_names

                        print(f"Neighbors of vertex {src_name}: {present_neighbors}")

                    print(f"\nPost OutEdge tree search stats {get_global_counter()}", flush=True)
                    print_stats_by_structure(flush=True)
                    print_max_write_batches(flush=True)

                    # ensure the number of expected vertices appears
                    for i in range(34):
                        if i in deleted_edges_dict:
                            # print(i, ograph.ogV_counts[i], len(list(graph.neighbors(i))) + 1 - len(deleted_edges_dict[i]))
                            assert ograph.ogV_counts[i] == len(list(graph.neighbors(i))) + 1 - len(deleted_edges_dict[i])
                        else:
                            # print(i, ograph.ogV_counts[i], len(list(graph.neighbors(i))) + 1)
                            assert ograph.ogV_counts[i] == len(list(graph.neighbors(i))) + 1

def test_ograph_vertex_deletion(fraction: float = 0.5, seed: int = 1) -> None:
    """Test deleting vertices in an Oblivious Graph"""
    # parameters to test on
    graph = karate_club_graph()
    name = "Karate Club"
    queue_osam = True
    stack_osam = True
    avl_tree_osam = True
    prime_graph = True
    branching_factor = 3
    pointers = {"original": [False, True], "multiwrite": [True], "recursive": [True]}
    make_copies_plural = [True, False]
    static_insertion_plural = [True, False]

    # pick random vertices to delete
    deleted_vertices = set()
    present_vertices = list(graph.nodes)
    num_vertices_to_remove = int(len(present_vertices)*fraction)
    for i in range(num_vertices_to_remove):
        index = random.randint(0, len(present_vertices)-1-i)
        deleted_vertices.add(present_vertices.pop(index))
    
    # iterate over every SmartPointer setting
    for pointer, move_semantics_plural in pointers.items():
        SmartPointer.set_smart_pointer(pointer)
        for move_semantics in move_semantics_plural:
            # toggle eager smart-copying
            for make_copies in [False]:
                # toggle static and dynamic insertion
                for static_insertion in [False]:
                    # initialize ograph
                    ograph = build_from_networkx(    
                        function=karate_club_graph,
                        name=name, 
                        graph=None, 
                        args=None, 
                        branching_factor=branching_factor,
                        static_insertion=static_insertion, 
                        move_semantics=move_semantics, 
                        make_copies=make_copies,
                        prime_graph=prime_graph,
                        seed=seed, 
                        connect_all=True, 
                        queue_osam=queue_osam,
                        stack_osam=stack_osam,
                        avl_tree_osam=avl_tree_osam,
                        random_weight_min=None, 
                        random_weight_max=None, 
                        sort_names=False, 
                        ordered_dynamic_vertices=True
                    )

                    reset_write_batches()

                    # delete vertex
                    for src_name in deleted_vertices:
                        print(f"Deleting vertex {src_name}")
                        if move_semantics:
                            ograph.delete_vertex(src_name)
                        else:
                            ograph.delete_vertex_full_copy(src_name)

                    print(f"\nNumber of deleted objects: {ograph.num_deleted_objs}")

                    print(f"\nPost delete vertex stats {get_global_counter()}", flush=True)
                    print_stats_by_structure(flush=True)
                    print_max_write_batches(flush=True)
                    reset_write_batches()

                    # check OutEdge tree to ensure all deleted verices are gone
                    remaining_neighbors = dict()
                    for src_name in present_vertices:
                        present_neighbors: Set[int] = set()
                        p_src = ograph.entry_points[ograph.v_ids[src_name]]
                        if move_semantics:
                            ograph.add_neighbors(p_src, "SET", present_neighbors, "DST_ID", None, make_copies)
                        else:
                            ogV = SmartPointer.get_and_copy(p_src)
                            ograph.add_neighbors_full_copy(p_src, ogV, "SET", present_neighbors, "DST_ID", None)
                        assert not present_neighbors & deleted_vertices
                        remaining_neighbors[src_name] = present_neighbors
                        print(f"Neighbors of vertex {src_name}: {present_neighbors}")

                    print(f"\nPost OutEdge tree search stats {get_global_counter()}", flush=True)
                    print_stats_by_structure(flush=True)
                    print_max_write_batches(flush=True)
                        
                    # all references to deleted objects should have been encountered and removed
                    assert ograph.num_deleted_objs == 0

                    # ensure the number of expected vertices appears
                    for i in range(34):
                        if i in deleted_vertices:
                            assert ograph.ogV_counts[i] == 0
                        else:
                            assert ograph.ogV_counts[i] == len((set(graph.neighbors(i)) & remaining_neighbors[i])) + 1
                            
if __name__ == "__main__":
    seed = 10
    test_ograph(seed=seed)
    test_ograph_edge_deletion(seed=seed)
    test_ograph_vertex_deletion(seed=seed)
