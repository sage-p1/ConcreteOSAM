import math
import argparse
from oblivious_graph import build_from_networkx, benchmark
from smart_pointer import SmartPointer
from networkx import erdos_renyi_graph
import os

def run_er_test_case(n, d, bs, pt_name, move_semantics, function, prime, make_copies):
    p = d / n
    if p > 1.0:
        print(f"Skipping test: p={p} > 1.0")
        return

    move_semantics = True if "True" == move_semantics else False
    make_copies = True if "True" == make_copies else False
    prime = True if prime == "True" else False

    SmartPointer.set_smart_pointer(pt_name)
    name = f"ER_n{n}_d{d}_{pt_name}_semantics{move_semantics}_bs{bs}_{function}_prime{prime}_copies{make_copies}"
    print(f"----- Running test: {name} -----")
    # bf = math.floor((bs - 2 * 8) / 8)
    bf = 6
    ograph = build_from_networkx(
        graph_function=erdos_renyi_graph,
        name=name,
        args=(n, p, 1),
        branching_factor=bf,
        static_insertion=True,
        move_semantics=move_semantics,
        show_edges=False,
        connect_all=True, 
        prime_graph=prime,
        random_seed=1
    )

    benchmark(
        ograph,
        name,
        trials=10,
        function=function,
        walk_length=10,
        move_semantics=move_semantics,
        damping_factor=0.9,
        pick_random_entry_points=True,
        make_copies=make_copies
    )

    print(f"\n----- Finished test: {name} -----\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, required=True)
    parser.add_argument("--d", type=int, required=True)
    parser.add_argument("--bs", type=int, required=True)
    parser.add_argument("--pt_name", type=str, required=True)
    parser.add_argument("--move_semantics", type=str, required=True)
    parser.add_argument("--fn", type=str, required=True)
    parser.add_argument("--prime", type=str, required=True)
    parser.add_argument("--make_copies", type=str, required=True)
    args = parser.parse_args()

    # python3 run_single_test.py --n  --d  --bs  --pt_name  --move_semantics  --fn  --prime   --make_copies
    # python3 run_single_test.py --n  --d  --bs  --pt_name  --move_semantics  --fn  > "logs/ER_n_d__semantics_bs.log" 2>&1 &

    run_er_test_case(args.n, args.d, args.bs, args.pt_name, args.move_semantics, args.fn, args.prime, args.make_copies)
