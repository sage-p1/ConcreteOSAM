# Concrete Oblivious Single Access Machines

This is a simulator that implements and improves upon the Oblivious Single Access Machine interface provided in [Appan et al., CCS'24](https://dl.acm.org/doi/10.1145/3658644.3690352).

## High Level Overview

Suppose a space-constrained client wants to store data on an untrusted server. Even with a secure encryption scheme, the memory access pattern of requests to the server can reveal sensitive information. We want our requests to be *oblivious* such that an eavesdropper does not learn anything from the memory access pattern.  

An Oblivious SAM (Single Access Machine) is a compiler that a client uses to make oblivious accesses to the server. At a high level, OSAM handles accesses in such a way that any that are two requests to the server appear indistinguishable from one another. In other words, an eavesdropper does not learn any useful information from observing the memory access pattern through OSAM.

![OSAM overview](/images/OSAM_overview.jpg)

The only leakage that occurs is the number of accesses made to the server.

## Oblivious Models

### Oblivious RAM 

Oblivious RAM (Random Access Machines) are constructions designed to obscure the memory access pattern. These are compilers that process client requests in such a way that the server cannot gain information about the memory access pattern. ORAM is the original concept OSAM is derived from. They support the following three operations:

* `a` &larr; `Alloc()`: Provides a fresh memory address `a`. 
* `Write(a, v)`: Writes the value `v` to the server location associated with address `a`.
* `v` &larr; `Read(a)`: Retrieves value `v` from the server associated with address `a`. 

If `a` was not allocated, then `Write()` and `Read()` both throw errors. If `a` was allocated but not written to, `Read(a)` returns `None`. ORAM allow the same address to be read from and written to multiple times.

ORAM are measured in three categories: bandwidth blowup, client storage, roundtrips to the server. For any ORAM, it is impossible to simultaneously achieve small bandwidth blowup, client storage, and a single roundtrip. 

### Oblivious SAM

Appan et al. consider the weaker SAM (Single Access Machine) model. SAM supports the same operations (`Alloc()`, `Read()`, `Write()`) as RAM with the additional limit that addresses can be written to and read from at most once. Despite being less powerful, this restriction allows Oblivious SAM to enjoy small bandwidth blowup, small client storage, and few roundtrips. Additionally, this framework allow Appan et al. to provide an oblivious and efficient library of common data structures and algorithms.
  
### Oblivious SAM+

To facilitate our improvements, we consider a model called SAM+ that is stronger than SAM but weaker than RAM. SAM+ is also composed of the three algorithms (`Alloc()`, `Read()`, `Write()`). The key difference is that while an address can still be read from at most once, SAM+ allows writing to the same address multiple times.

## Pointers

Under the different frameworks of RAM, SAM, and SAM+, we create multiple pointer classes that all use the following interface:

* `p` &larr; `new(v)`: Create and return a pointer `p` that points to value `v` in memory.
* `v` &larr; `get(p)`: Dereference a pointer `p` to retrieve its pointee `v`. This can be repeated several times.
* `put(p, val')`: Update the pointer `p` so that is points to the new `val'`. This can be repeated several times.
* `q` &larr; `p`: Creates `q` as a copy of `p` such that they share a single pointee.
* `delete(p)`: Deletes a pointer.

### RAM-based Pointers

We easily implement pointers under RAM due to its multiple read and write capabilites. At a high level, we can simply associate one allocated address per pointer to perform all relevant operations. Pointee sharing is possible by assigning the same address to multiple pointers. The ORAM architecture is treated as a blackbox and does not particularly matter here.

### SAM-based Pointers

Under SAM's single read and write restriction, pointee sharing requires additional care, as it is not enough to share addresses. If pointers `p` and `q` share a pointee `v` through one allocated address, a dereference of `p` will burn the shared address and make any dereferences of `q` violate the single read policy. 

To remedy this, Appan et al. augment every pointer with a SAM-based queue to manage sharing. Each queue has two addresses: `head` and `tail`. Reading `head` dequeues items in a FIFO order, and writing to `tail` enqueues later items. We use queues to store every address `v` has been written to since the last dereference.

Let pointers `p` and `q` both maintain `head`s that point to `v`. `v` tracks both incoming `tail`s. If `p` is repeatedly dereferenced four times, a new address to write back `v` is allocated and added to `q`'s queue four times. Dereferencing `q` requires reading all four addresses in the queue until the final one can be used to retrieve `v`. 

![SAM-based sharing](/images/SAM_sharing.png)

A pointee can support arbitrary references by creating an inverted pointer tree Each pointer is a leaf in this inverted tree and maintains a `head` of an address queue. Each non-root node manages two `tail`s and contains its own `head`. All pointers have a path to the root node, which contains the pointee.

When adding new pointers, Appan et al. traverse the inverted tree first to the root and then rightmost leaf to maintain balance. This process is known as *smart copying* as to not confuse with bitwise copying.

![SAM-based trees](/images/SAM_tree.png)

### SAM+-based Pointers

The multiple write relaxation under SAM+ allows us to achieve a simpler, more efficient method of pointer sharing. 

Again consider a pointee `v` that is shared by `p` and `q`. Both pointers contain `v`'s address and the address of the other pointer. If `p` is derefenced, the address to `p` is read to access `v`'s address. Two new addresses are allocated for `v` and `p` to be written back to. To ensure `q` can be dereferenced, `q`'s address is written to with the new addresses of `v` and `p`. We can do this thanks to (1) `p` storing `q`'s address and (2) the multiple write relaxation. 

![SAM-based sharing](/images/SAM+_sharing.png)

Arbitrary references can also be managed by creating an inverted pointer tree. For smart copying, the pointer to copy is a leaf. We simply extend the path of the tree by creating a new leaf that points to the old leaf. If we create a new pointer alongside our original pointer, we can set both to point to the new leaf. 

Note this means we may have unbalanced inverted trees if we copy the same leaf over and over. These are rebalanced when traversing the path from leaf to root during a dereference.

![SAM-based sharing](/images/SAM+_tree.png)

## Smart Copies and Move

In Appan et al., whenever a pointer is dereferenced, the pointee itself is smart copied. Since a pointee might contain other pointers, creating smart copies avoids the possibility of having two identical instances of a pointer at once, which would violate the single read limitation.

Smart copying per every dereference can get expensive, so we introduce a notion called `move`. The `move` property indicates that after a dereference to pointer `p`, the client receives the original pointee `v` and an allocated address `a`. If `v` is a pointer, the client can dereference it at most once. Then, the client is obliged to write it back to address `a` to ensure the single read rule is not violated. Under careful management on the client's side, it is possible to significantly reduce the number of smart copies made.

## Graph Simulation

Given an arbitrary degree graph which we call the *original graph*, we create an *emulating graph*. This is an oblivious emulation of the original graph built on top of the pointer interface as a directed graph of constant out degree `bf` (for branching factor). Emulating graphs consist of three objects: *vertices*, *edges*, and *fanout vertices*. 

* A vertex in the emulating graph is a vertex in the original graph.
* An edge in the emulating graph is a pointer to a destination vertex. This represents the destination `v` in an edge `u` &rarr; `v` belonging to the original graph.
* A fanout vertex stores the edge pointer. Each vertex in the emulating graph points to its fanout vertices, which represents the source `u` in an original edge `u` &rarr; `v`. If `u` in the original graph has an outdegree exceeding `bf`, then a tree of fanout vertices is built below vertex `u` in the emulating graph.

Fanout vertices have in degree 1, and vertices have in degree `d`. Note that undirected edges in an original graph become two directed edges in the emulating graph.

![SAM-based sharing](/images/emulated_graph.png)

In the above picture, we have an original graph (left) and the resulting emulating graph (right). In the right, black vertices and edges correspond to their couner parts in the original graph. Green edges and vertices at the fanout vertices we introduce to have a constant degree graph. Note that `bf=2` in this example.

## Implementation 

This repo provides a Python simulator of the ORAM, OSAM, and OSAM+ interfaces. It does not perform any cryptography and simply serves as a benchmarking tool. 

* We use `single_access_machine.py` as a blackbox compiler that can perform `Alloc()`, `Write()`, and `Read()`. The settings are configured for SAM+ by default, but can be changed.
* The three separate pointer interfaces based on SAM+, SAM, and RAM are respectively available in `smart_pointer_multi_write.py` (our contribution), `smart_pointer_original.py` (Appan et al.'s work), and `recursive_pointer.py` (assumes a recursive Path ORAM implementation).
* So that our graph interface can easily operate on all pointer types, `smart_pointer.py` is a wrapper class that enables pointer setting and switching.
* Our graph interface is availble in `oblivious_graph.py`. We allow building graphs on any pointer architecture and support a suite of helper procedures and algorithms:
    * Algorithms:
        * `bfs()` traverses the graph by queueing nodes in a FIFO manner.
        * `dfs()` traverses the graph with a stack following a LIFO principle.
        * `dijkstra()` computes the single source shortest path for each node.
        * `prim()` computes a minimum spanning tree.
        * `random_walk()` randomly traverses a path through the graph until either `wl` (walk length) steps have been completed or there are no further vertices to access.
        * `contact_discovery()` computes the intersection of neighbors shared by two vertices.
        * `directed_triangle_count()` outputs all directed triangles associated with a vertex. A directed triangle is a cycle of three vertices `(u, v, w)` that are connected by the edges `u` &rarr; `v`, `v` &rarr; `w`, and `w` &rarr; `u`. 
        * `pagerank()` measures importance of vertices by performing several random walks of total length `wl`. A fresh vertex is randomly selected with a probability of `1 - df` (damping factor) or if there are no further neighbors to select. 
    * Helper Procedures:
        * `addNeighbors()` conducts a bfs on vertex's tree of fanout vertices and traverses down to each edge pointer. Pointers or other vertex metadata are added to some data structure.
        * `getRandomNeighbor()` randomly chooses a neighbor, traverses down the tree of fanout vertices, and fetches the edge pointer. Returns `None` if the vertex has no neighbors.
    * Miscellaneous:
        * Nearly all algorithms and procedures have a second implementation where the tag `full_copy` is appended to its name. All `full_copy` algorithms do not make use of `move`. 
* We introduce the SAM-based AVLTree in `smart_avl_tree.py` as an oblivious map. This AVLTree is used to obliviously track entry points to the emulating graph.
* The Appan et al. implementations of SAM-based queues and stacks are given in `address_queue.py` and `smart_stack.py`. 
* We provide a slightly modified queue structure in `smart_queue.py` that allows us to save one roundtrip to the server. We maintain Appan et al.'s address queues in `smart_pointer_original.py` and use the new queues everywhere else.
* To implement `move`, `sublist.py` allows us to retrieve specified list indices only.
* The files `launch_all_er_tests.sh`, `detect_failed_er_tests.py`, 
* The files `cloudlab.sh`, `detect_failed_er_tests.py`, `Makefile`

### Miscellaneous

* `cloudlab.sh` and `run_er_tests_gnu.sh` were alternate scripts for running tests.
* `Makefile` and `sam.cc` are a simplified version of `oblivious_graph.py` build in C++. 
* `smart_pointer_splay.py` is an alternate version of `smart_pointer_multi_write.py` we briefly tested. 

## Usage

We benchmark our emulating graphs with the following steps and settings:

* Procedure: Build a graph and run one algorithm. We construct a fixed, fresh graph for each algorithm to ensure fairness across testing.
* Graph size: 
    * degree `d=20` and number of nodes `n=` 2^2^ to 2^20
    * degree `d=100` and number of nodes `n=` 2^2^ to 2^18
* Graph type: Use the Erdos–Renyi model to construct random graphs. These graphs could be disconnected, so we randomly insert one edge between components until all vertices are connected. 
* Graph algorithms: We test four phases - Build, Random Walk, PageRank, and Contact Discovery. For algorithms that require them, `wl=50` and `df=0.9`. Random entry points are selected for each run. For all algorithms besides build, we conduct 50 trials to compute an average number of SAM operations.
* Graph priming: In between building the graph and testing some algorithm, we prime the graph, or conduct `0.10n` random walks, to rebalance the underlying inverted pointer trees in the SAM+-based pointers. Priming stats are included in the building phase.
* Pointer types: We perform these tests across several pointer types:
    * Multi-Write: SAM+-based pointers with `move` enabled
    * Original: SAM-based pointers as described by Appan et al.
    * Original w/ Move: SAM-based pointers as described by Appan et al. with `move`
    * Recursive: A pointer built on recursive Path ORAM
* Block size and branching factor: We set the block size `bs` to 64 and 4096 (which simulates how much space we would use if `single_access_machine.py` had a real backend). We set the branching factor `bf=bs/8-2` to simulate how much data we could hope to fix in a single vertex or fanout vertex. 

To run our experiments, the file `run_single_test.py` accepts the graph stats (`n`, `d`, algorithm to run, etc.), builds a graph, and conducts 50 runs of the specified algorithm. The script `launch_all_er_tests.sh` makes many calls to `run_single_test.py` to test every possible permutation of a given set of parameters. Results containing the number of SAM operations are saved as log files with the format: `ER_n[n]_d[d]_[pt_name]_semantics[move_semantics]_bs[bs]_[fn]_prime[p]_copies[mc].log`. Note that `semantics` and `move_semantics` are synonymous with the `move` procedure. Meanwhile, `copies` dictates if smart copies are made every dereference or not. 

`launch_all_er_tests.sh` can spawn a large number of tests at once, so please adjust the file to run fewer tests together. 

Run `detect_failed_er_test.py` to determine which tests have not finished. Note that is will also report experiments that are still running.

Of the SAM stats, we are most interested in reads because they can be used as proxy for the number of roundtrips. As such, when running our parser `parse_all_er_tests.py`, it generates graphs of the number of reads by the number of nodes for a given `d` and algorithm. To provide a clearer visual of the change in performance as `n` increases, we take log~2~ of box axes. The parser also generates tables showing a breakdown of reads per SAM-based object. To know what parameters to use for creating plots and tables, the parser looks inside `parameters.log`.
