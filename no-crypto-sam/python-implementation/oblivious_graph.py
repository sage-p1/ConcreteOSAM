from smart_pointer import SmartPointer
from attribute_at_positions import AttributeAtPositions
from single_access_machine import get_global_counter, print_stats_by_structure, print_max_write_batches, reset_write_batches, sam
from smart_queue import SmartQueue as queue
from smart_stack import SmartStack as stack
from smart_avl_tree import SmartAVLTree
from math import log, ceil, sqrt
from collections import deque
from networkx import karate_club_graph, erdos_renyi_graph, DiGraph, Graph, connected_components, get_edge_attributes
import random
import copy
import heapq
from typing import Dict, Deque, Tuple, List, Set, Any
from collections.abc import Callable

class PriorityQueue:
    """
    Priority Queue (Max first) for dijkstra / prim algorithms
    A priority queue built on SAM requires manipulating
    tree-based ORAM/OSAM techniques
    This is just a simulator, so we did not build a 
    priority queue that incurs SAM accesses
    """
    def __init__(self) -> None:
        self.storage: Dict[int, Deque[Any]] = dict()
        self.keys: List[int] = []
        self.length = 0

    def is_empty(self) -> bool:
        """Checks if the PQ is empty"""
        return self.length == 0

    def insert(self, key: int, value: Any) -> None:
        """Insert a value associated with a priority / key"""
        heapq.heappush(self.keys, key)
        if key not in self.storage:
            self.storage[key] = deque([value])
        else:
            self.storage[key].append(value)
        self.length += 1

    def pop(self) -> Tuple[int, Any]:
        """Pop the value with the largest priority / key"""
        key = heapq.heappop(self.keys)
        value = self.storage[key].popleft()
        if not self.storage[key]:
            del self.storage[key]
        self.length -= 1
        return key, value

    def peek(self) -> int:
        """Observe the largest priority / key in the PQ"""
        key = heapq.heappop(self.keys)
        heapq.heappush(self.keys, key)
        return key

    def __len__(self) -> int:
        """Get length of PQ"""
        return self.length

PQ = PriorityQueue()




"""
Vertex object classes for oblivious graphs nodes
"""

class Vertex:
    """Corresponds to a vertex in the original graph"""
    def __init__(
            self, 
            id: int, 
            out_children: List[SmartPointer | None] = [], 
            out_degree: int = 0, 
            height: int = 0, 
            visited: bool = False,
            label: int | None = None
        ) -> None:
        """Initialize a Vertex"""
        self.id = id
        self.type = "VERTEX"
        self.out_children = out_children
        self.out_degree = out_degree
        self.height = height
        self.visited = visited
        self.label = label

    def __repr__(self) -> str:
        """String representation of a Vertex"""
        return f"Vertex(id={self.id}, type={self.type}, out_children={self.out_children}, out_degree={self.out_degree}, height={self.height}, visited={self.visited}, label={self.label})"

    def __eq__(self, other: object) -> bool:
        """Decide equality by comparing attributes"""
        if isinstance(other, Vertex):
            return self.id == other.id and self.out_degree == other.out_degree and self.height == other.height and all(self.out_children[i] == other.out_children[i] for i in range(len(self.out_children)))
        return False
        
    def smart_delete(self) -> None:
        """Delete Vertex"""
        SmartPointer.delete(self.out_children)

    @staticmethod
    def smart_copy(vertex: "Vertex", temp_copy: bool) -> "Vertex":
        """Copy a Vertex"""
        copy_v = Vertex(id=vertex.id, out_degree=vertex.out_degree, height=vertex.height, visited=vertex.visited, label=copy.deepcopy(vertex.label))
        copy_v.out_children = SmartPointer.copy(vertex.out_children, temp_copy)
        return copy_v

class FanOut:
    """
    Corresponds to an edge (ogE) in the original graph
    May also be used to create an edge tree of internal FanOuts
    """
    def __init__(
            self, 
            children: List[SmartPointer | None] = [], 
            ogE: SmartPointer | None = None,
            weight: int = 0, 
            dst_id: int | None = None, 
            is_leaf: bool = False,
            label: int | None = None
        ) -> None:
        """Intialize an FanOut"""
        self.type = "FAN_OUT"
        self.children = children
        self.ogE = ogE
        self.weight = weight
        self.dst_id = dst_id
        self.is_leaf = is_leaf
        self.label = label

    def __repr__(self) -> str:
        """String representation of an FanOut"""
        return f"FanOut(type={self.type}, children={self.children}, ogE={self.ogE}, weight={self.weight}, dst_id={self.dst_id}, is_leaf={self.is_leaf}, label={self.label})"

    def smart_delete(self) -> None:
        """Delete FanOut"""
        SmartPointer.delete(self.children)
        SmartPointer.delete(self.ogE)

    @staticmethod
    def smart_copy(fan_out: "FanOut", temp_copy: bool = False) -> "FanOut":
        """Copy an FanOut"""
        copy_v = FanOut(weight=fan_out.weight, dst_id=fan_out.dst_id, is_leaf=fan_out.is_leaf, label=copy.deepcopy(fan_out.label))
        copy_v.children = SmartPointer.copy(fan_out.children, temp_copy)
        copy_v.ogE = SmartPointer.copy(fan_out.ogE, temp_copy)
        return copy_v

class DeletedObject:
    """DeletedObject to place at pointees when Vertices are deleted"""
    def __init__(self, id: int | None = None) -> None:
        """Initialize a DeletedObject"""
        self.id = id
        self.type = "DELETED_OBJECT"

    def __repr__(self) -> str:
        """String representation of a DeletedObject"""
        return f"DeletedObject(id={self.id}, type={self.type})"

    def smart_delete(self) -> None:
        """
        DeleteObject function
        No need to actually delete anything
        """
        pass

    @staticmethod
    def smart_copy(deleted_object: "DeletedObject", temp_copy: bool = False) -> "DeletedObject":
        """Copy a DeletedObject"""
        copy_v = DeletedObject(id=deleted_object.id)
        return copy_v




"""
OGraph - Oblivious Graph algorithms built on SmartPointer architecture
All standard functions incur fewer SAM accesses by:
    - using get_attr/set_attr, two functions that only interact with 
    requested attributes 
    - toggling the make_copies flag that dictates whether the original
    object or a copy is returned
There are versions of most functions that end in _full_copy, which 
means that attributes are always returned and copies are always made 
"""

class OGraph:
    def __init__(self, branching_factor: int = 2, avl_tree_osam: bool = True) -> None:
        """Initialize an Oblivious Graph"""
        assert branching_factor > 1
        self.nodes = 0
        self.branching_factor = branching_factor
        self.v_ids = SmartAVLTree(avl_tree_osam=avl_tree_osam) # v_name -> v_id
        self.entry_points = SmartAVLTree(avl_tree_osam=avl_tree_osam) # v_id -> SmartPointer to Vertex
        self.can_emulate = True
        self.num_deleted_objs = 0
        
        # additional access stats
        self.ogV_counts: Dict[int, int] = dict() # v_id -> number of shared pointers
        self.ogV_accesses = 0
        self.out_tree_accesses = 0
        self.ogV_degree = 0
        self.out_tree_degree = 0
        self.ogV_depth: List[int] = [] # list of ogV depths to average

    def reset_access_stats(self) -> None:
        """Reset all OGraph stats"""
        self.ogV_accesses = 0
        self.out_tree_accesses = 0
        self.ogV_degree = 0
        self.out_tree_degree = 0
        self.ogV_depth.clear()

    def print_access_stats(self) -> None:
        """Print regular access stats"""
        print("\nAccess stats: ogV accesses, out tree accesses, avg ogV degree, avg out tree degree, avg ogV depth")
        avg_ogV_degree = round(self.ogV_degree/self.ogV_accesses) if self.ogV_accesses > 0 else 0
        avg_out_tree_degree = round(self.out_tree_degree/self.out_tree_accesses) if self.out_tree_accesses > 0 else 0
        avg_ogV_depth = sum(self.ogV_depth)/len(self.ogV_depth) if len(self.ogV_depth) > 0 else 0
        print(f"({self.ogV_accesses}, {self.out_tree_accesses}, {avg_ogV_degree}, {avg_out_tree_degree}, {avg_ogV_depth})")
            
    def get_pointer(self, src_name: int | str) -> Tuple[int, SmartPointer]:
        """Lookup v_id and pointer associated with v_name"""
        src_id = self.v_ids[src_name]
        p_src = self.entry_points[src_id]
        if p_src is None:
            raise KeyError(f"Error: Source name {src_name} not found.")
        return src_id, p_src

    def get_ogV_full_copy(self, p_src: SmartPointer) -> Tuple[SmartPointer, Vertex]:
        """Get a copy of the Vertex stored at p_src"""
        pre_alloc_count = get_global_counter()[0]
        ogV = SmartPointer.get_and_copy(p_src)
        assert ogV.type == "VERTEX"
        self.ogV_depth.append(get_global_counter()[0] - pre_alloc_count)
        self.ogV_degree += self.ogV_counts[ogV.id]
        self.ogV_accesses += 1
        return p_src, ogV
    
    def get_ogV_attr(
            self, 
            p_src: SmartPointer, 
            req_attr: List[str] = ["id", "type"], 
            make_copies: bool = True
        ) -> Tuple[SmartPointer, Dict[str, Any]]:
        """Get specified attributes of a Vertex"""
        pre_alloc_count = get_global_counter()[0]
        v_attr = SmartPointer.get_attr(p_src, req_attr, make_copies)
        assert v_attr["type"] == "VERTEX"
        self.ogV_depth.append(get_global_counter()[0] - pre_alloc_count)
        self.ogV_degree += self.ogV_counts[v_attr["id"]]
        self.ogV_accesses += 1
        return p_src, v_attr

    def add_vertex( 
            self, 
            v_name: int | str, 
            v_ids: SmartAVLTree | Dict[int | str, int], 
            entry_points: SmartAVLTree | Dict[int, SmartPointer],
            out_degree: int = 0, 
            out_children: List[SmartPointer | None] | None = None, 
            label: int | None = None
        ) -> Tuple[int, SmartPointer]:
        """Create and assign a new vertex in the OGraph"""
        # see if v_name already exists in graph
        try:
            v_id = v_ids[v_name]
        except:
            v_id = None

        # return smart pointer if it does
        if v_id is not None:
            p = entry_points[v_id]
            return v_id, p

        # assign new vertex id
        v_id = self.nodes
        self.nodes += 1

        # assign children if they exist 
        if out_children is None:
            out_children = []

        for i in range(self.branching_factor - len(out_children)):
            out_children.append(None)

        assert self.branching_factor == len(out_children)

        match out_degree:
            case 0:
                height = 0
            case 1:
                height = 1
            case _:
                height = ceil(log(out_degree, self.branching_factor))

        # create original vertex
        v = Vertex(
            id=v_id,
            out_degree=out_degree,
            out_children=out_children,
            height=height,
            label=label
        )

        # store entry point and v_id in AVL trees
        p = SmartPointer(SmartPointer.new(v))
        entry_points[v_id] = p
        v_ids[v_name] = v_id
        self.ogV_counts[v_id] = 1

        return v_id, p

    def add_edge_full_copy(self, src_name: int | str, dst_name: int | str, weight: int = 0, alert_exists: bool = True) -> bool:
        """Dynamic insertion of a single edge between src and dst"""
        # manually adding an edge means the graph can no longer be statically emulated
        self.can_emulate = False

        # create src/dst SmartPointer/vertices
        src_id, p_src = self.add_vertex(v_name=src_name, v_ids=self.v_ids, entry_points=self.entry_points)
        dst_id, p_dst = self.add_vertex(v_name=dst_name, v_ids=self.v_ids, entry_points=self.entry_points)

        # ensure edge does not exist yet
        if not self.edge_exists_full_copy(p_src, dst_id):
            # add FanOut 
            self.__add_fan_out_full_copy(src_id, p_src, dst_id, p_dst, weight)     
            return True
        if alert_exists: print(f"Edge {src_name}->{dst_name} already exists!")       
        return False     

    def add_edge(   
            self, 
            src_name: int | str, 
            dst_name: int | str, 
            weight: int = 0,
            alert_exists: bool = True,
            make_copies: bool = True
        ) -> bool:
        """Dynamic insertion of a single edge between src and dst"""
        # manually adding an edge means the graph can no longer be statically emulated
        self.can_emulate = False

        # create src/dst SmartPointer/vertices
        src_id, p_src = self.add_vertex(v_name=src_name, v_ids=self.v_ids, entry_points=self.entry_points)
        dst_id, p_dst = self.add_vertex(v_name=dst_name, v_ids=self.v_ids, entry_points=self.entry_points)

        # ensure edge does not exist yet
        if not self.edge_exists(p_src, dst_id, make_copies):
            # add FanOut 
            self.__add_fan_out(src_id, p_src, dst_id, p_dst, weight, make_copies)
            return True
        if alert_exists: print(f"Edge {src_name}->{dst_name} already exists!")       
        return False

    def edge_exists_full_copy(self, p_src: SmartPointer, dst_id: int) -> bool:
        """Checks if src has a path to dst"""
        found = False

        # load ogV
        p_src, ogV = self.get_ogV_full_copy(p_src)

        # queue children of src to visit
        sq = queue.init_queue()
        for i in range(self.branching_factor):
            if ogV.out_children[i] is not None:
                queue.enqueue(sq, ogV.out_children[i])
                ogV.out_children[i] = None
            else:
                break

        # delete ogV    
        SmartPointer.delete(ogV)

        # traverse child fan_outs
        while sq.head is not None:
            p = queue.dequeue(sq)
            if p is None:
                continue

            # no need to traverse if dst_id was found
            if found:
                SmartPointer.delete_temp_copy(p)
                continue
                
            # load v
            v = SmartPointer.get_and_copy(p)
            assert v.type == "FAN_OUT"

            # traverse until reaching FanOut leaves
            if not v.is_leaf:
                for i in range(self.branching_factor):
                    if v.children[i] is not None:
                        queue.enqueue(sq, v.children[i])
                        v.children[i] = None
                    else:
                        break
                        
            # reached leaf
            elif v.dst_id == dst_id:
                found = True

            # no longer need to access v or p
            SmartPointer.delete(v)
            SmartPointer.delete_temp_copy(p)
        
        return found

    def edge_exists(self, p_src: SmartPointer, dst_id: int, make_copies=True) -> bool:
        """Checks if src has a path to dst"""
        # determine if src has path to dst
        found = False

        # load ogV
        req_attr = ["out_children", "type", "out_degree", "id"]
        p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)

        # queue children of src to visit
        sq = queue.init_queue()
        for i in range(self.branching_factor):
            if v_attr["out_children"][i] is not None:
                queue.enqueue(sq, v_attr["out_children"][i])
            else:
                break

        # traverse child fan_outs
        req_attr = ["type", "is_leaf", "children", "dst_id"]
        while sq.head is not None:
            p = queue.dequeue(sq)
            if p is None:
                continue

            # no need to traverse if dst_id was found
            if found:
                SmartPointer.delete_temp_copy(p)
                continue

            # load v attributes
            v_attr = SmartPointer.get_attr(p, req_attr, make_copies)
            assert v_attr["type"] == "FAN_OUT"
            self.out_tree_accesses += 1
            degree = 2 if SmartPointer.is_temp_copy(p) else 1
            self.out_tree_degree += degree

            # traverse until reaching FanOut leaves
            if not v_attr["is_leaf"]:
                for i in range(self.branching_factor):
                    if v_attr["children"][i] is not None:
                        queue.enqueue(sq, v_attr["children"][i])
                    else:
                        break

            # reached leaf
            elif v_attr["dst_id"] == dst_id:
                found = True
        
            # no longer need to access p
            SmartPointer.delete_temp_copy(p)

        return found

    def __add_fan_out_full_copy(   
            self, 
            src_id: int, 
            p_src: SmartPointer, 
            dst_id: int, 
            p_dst: SmartPointer, 
            weight: int
        ) -> None:
        """ 
        Traverse src's FanOut tree and inserts a leaf FanOut that points to p_dst
        FanOut leaves a filled out left to right
        If necessary, expand the tree by one level
        """
        # new FanOut leaf
        v_out_leaf = FanOut(
            children=[None]*self.branching_factor,
            ogE=SmartPointer.copy(p_dst),
            weight=weight,
            dst_id=dst_id,
            is_leaf=True,
            label=None
        )

        self.ogV_counts[dst_id] += 1
        p_out_leaf = SmartPointer(SmartPointer.new(v_out_leaf))

        # get ogV's first layer of children and out degree
        p_src, ogV = self.get_ogV_full_copy(p_src)
        out_degree = ogV.out_degree
        ogV.out_degree += 1

        # height calculations
        height = ogV.height
        new_height = ceil(log(ogV.out_degree, self.branching_factor)) if ogV.out_degree > 1 else 1
        
        # insert leaf at ogV's first level
        if out_degree < self.branching_factor:
            ogV.out_children[out_degree] = p_out_leaf
            ogV.height = new_height
            SmartPointer.put(p_src, ogV)
            SmartPointer.delete(ogV)

        # insert at current tree height
        elif height == new_height:
            # update ogV
            SmartPointer.put(p_src, ogV)

            # slicing parameters
            all_leaves = [i for i in range(int(self.branching_factor**height))]
            target = out_degree
            step = len(all_leaves) // self.branching_factor
            start = 0
            i = 0

            # partition list of child numbers to know which direct to traverse
            sq = queue.init_queue()
            while start < len(all_leaves):
                if target in all_leaves[start:start+step]:
                    queue.enqueue(sq, ogV.out_children[i])
                    ogV.out_children[i] = None
                    all_leaves = all_leaves[start:start+step]
                    break
                else:
                    start += step
                    i += 1

            SmartPointer.delete(ogV)

            # traverse to bottom of tree
            found = False
            while sq.head is not None:
                p = queue.dequeue(sq)
                if p is None:
                    continue

                if found:
                    SmartPointer.delete_temp_copy(p)
                    continue

                # load v
                v = SmartPointer.get_and_copy(p)
                assert v.type == "FAN_OUT"

                # slicing parameters
                step = len(all_leaves)//self.branching_factor
                start = 0
                i = 0

                # partition list of child numbers to know which direction to traverse
                while start < len(all_leaves):
                    # computes which child should be accessed
                    if target not in all_leaves[start:start+step]:
                        start += step
                        i += 1
                        continue

                    elif len(all_leaves) == self.branching_factor:
                        # indicates we have reached the penultimate level
                        # this is also the last level of FanOuts
                        v.children[i] = p_out_leaf
                        SmartPointer.put(p, v)
                        found = True

                    else:
                        # still at internal FanOuts
                        queue.enqueue(sq, v.children[i])
                        v.children[i] = None
                        all_leaves = all_leaves[start:start+step]
                    
                    break

                SmartPointer.delete(v)
                SmartPointer.delete_temp_copy(p)

        # create and insert at a new level
        else:
            # set current children to grandchildren of new left most child
            leftmost_child = FanOut(
                children=ogV.out_children,
                ogE=None,
                weight=0,
                dst_id=None,
                is_leaf=False,
                label=None
            )

            # collect new children pointers for ogV
            new_out_children: List[SmartPointer | None] = [SmartPointer(SmartPointer.new(leftmost_child))]
            
            for i in range(1, self.branching_factor):
                # p_out_leaf is the only new FanOut to add at the tree's bottom 
                if i == 1:
                    sq = queue.init_queue()
                    queue.enqueue(sq, p_out_leaf)
                else:
                    sq = queue(head=None, tail=None)

                # build FanOut tree
                sq = self.__build_edge_tree(sq=sq, height=height, src_id=src_id, offset=1)
                
                # empty queue
                while sq.head is not None:
                    p_child = queue.dequeue(sq)
                    if p_child is not None:
                        new_out_children.append(p_child)

            ogV.out_children = new_out_children
            ogV.height = new_height
            SmartPointer.put(p_src, ogV)
            SmartPointer.delete(ogV)

    def __add_fan_out( 
            self, 
            src_id: int, 
            p_src: SmartPointer, 
            dst_id: int, 
            p_dst: SmartPointer, 
            weight: int,
            make_copies: bool = True
        ) -> None:
        """ 
        Traverse src's FanOut tree and inserts a leaf FanOut that points to p_dst
        FanOut leaves a filled out left to right
        If necessary, expand the tree by one level
        """

        # new FanOut leaf
        v_out_leaf = FanOut(
            children=[None]*self.branching_factor,
            ogE=SmartPointer.copy(p_dst),
            weight=weight,
            dst_id=dst_id,
            is_leaf=True,
            label=None
        )

        self.ogV_counts[dst_id] += 1
        p_out_leaf = SmartPointer(SmartPointer.new(v_out_leaf))

        # get ogV's first layer of children and out degree
        req_attr = ["type", "out_degree", "id", "height"]
        p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)
        out_degree = v_attr["out_degree"]

        # prepare setter dict
        set_attr = {"out_degree": out_degree + 1}

        # height calculations
        # due to deletions, height may be one level higher than 
        # the calculation found from out_degree 
        height = v_attr["height"]
        match out_degree:
            case 0:
                expected_height = 0
                expected_new_height = 1
            case 1:
                expected_height = 1
                expected_new_height = 1
            case _:
                expected_height = ceil(log(out_degree, self.branching_factor))
                expected_new_height = ceil(log(out_degree+1, self.branching_factor))

        new_height = expected_new_height if height == expected_height else height
        
        # insert ogE at ogV's first level
        if out_degree < self.branching_factor and height <= 1:
            # get out_children
            req_attr = ["type", "out_children", "id"]
            p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)
            out_children = v_attr["out_children"]

            # update children
            out_children[out_degree] = p_out_leaf
            set_attr["out_children"] = out_children 
            set_attr["height"] = new_height 
            SmartPointer.put_attr(p_src, set_attr, delete_old=make_copies)

        # insert ogE without creating a new tree level
        elif height == new_height:
            # update ogV 
            SmartPointer.put_attr(p_src, set_attr, delete_old=make_copies)

            # precompute path: sequence of child indices to follow at each tree level (most-significant first)
            target = out_degree
            subtree_height = expected_height
            path = []

            for i in range(height-subtree_height):
                path.append(0)

            # handle the special case where adding the next
            # child would have increased the tree size
            # (but the tree is already at this size due to deletions)
            if expected_height < height and expected_new_height == height:
                path[0] = 1
                for level in range(subtree_height):
                    path.append(0)
                
            else:
                rem = target
                for level in range(subtree_height):
                    exp = subtree_height - level - 1
                    if exp < 0:
                        exp = 0  
                    div = self.branching_factor ** exp
                    idx = rem // div
                    path.append(int(idx))
                    rem = rem % div

            leaf_idx = path.pop()
            p = p_src
            i = 0

            pre_alloc_count = get_global_counter()[0]
            for idx in path:
                child_term = "out_children" if i == 0 else "children"
                sl = AttributeAtPositions(child_term, [idx])
                next_child = SmartPointer.get_attr(p, sl, make_copies)[child_term][0]

                if i == 0:
                    self.ogV_depth.append(get_global_counter()[0] - pre_alloc_count)
                    self.ogV_degree += self.ogV_counts[src_id]
                    self.ogV_accesses += 1

                else:
                    SmartPointer.delete_temp_copy(p)
                    self.out_tree_accesses += 1
                    degree = 2 if SmartPointer.is_temp_copy(p) else 1
                    self.out_tree_degree += degree

                i += 1
                p = next_child

            # get children 
            children = SmartPointer.get_attr(p, "children", make_copies)
            self.out_tree_accesses += 1
            degree = 2 if SmartPointer.is_temp_copy(p) else 1
            self.out_tree_degree += degree

            # put FanOut with ogE in tree
            children[leaf_idx] = p_out_leaf
            set_attr_intermediate = {"children": children}
            SmartPointer.put_attr(p, set_attr_intermediate, delete_old=make_copies)
            SmartPointer.delete_temp_copy(p)
                            
        # create and insert at a new level
        else:
            # get out_children
            req_attr = ["type", "out_children", "id"]
            p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)
            out_children = v_attr["out_children"]

            for i in range(self.branching_factor):
                out_children[i].temp_copy = False

            # set current children to grandchildren of new left most child
            leftmost_child = FanOut(
                children=out_children,
                ogE=None,
                weight=0,
                dst_id=None,
                is_leaf=False,
                label=None
            )

            # collect new children pointers for ogV
            new_out_children = [SmartPointer(SmartPointer.new(leftmost_child))]
            
            for i in range(1, self.branching_factor):
                # p_out_leaf is the only new FanOut to add at the tree's bottom 
                if i == 1:
                    sq = queue.init_queue()
                    queue.enqueue(sq, p_out_leaf)
                else:
                    sq = queue(head=None, tail=None)

                # build FanOut tree
                sq = self.__build_edge_tree(sq=sq, height=height, src_id=src_id, offset=1)

                # empty queue
                while sq.head is not None:
                    p_child = queue.dequeue(sq)
                    if p_child is not None:
                        new_out_children.append(p_child)

            set_attr["out_children"] = new_out_children
            set_attr["height"] = new_height 
            SmartPointer.put_attr(p_src, set_attr, delete_old=make_copies)

    def __build_edge_tree( self, sq: queue, height: int, src_id: int, offset: int = 0) -> queue:
        """Recursively build FanOut tree from bottom to top"""
        if height > 0:
            num_vertices = int(self.branching_factor**(height-offset))
            new_sq = queue.init_queue()

            # create one level of internal fan_outs
            for i in range(num_vertices):
                v = FanOut(
                    children=[None]*self.branching_factor,
                    ogE=None,
                    weight=0,
                    dst_id=None,
                    is_leaf=False,
                    label=None
                )
                
                # populate children
                for j in range(self.branching_factor):
                    if sq.head is not None:
                        p_child = queue.dequeue(sq)
                        if p_child is not None:
                            v.children[j] = p_child
                    else:
                        break
                
                p = SmartPointer(SmartPointer.new(v))
                queue.enqueue(new_sq, p)

            # empty queue
            while sq is not None and sq.head is not None:
                queue.dequeue(sq)

            return self.__build_edge_tree(sq=new_sq, height=height-1, src_id=src_id, offset=offset)
        
        else: 
            return sq

    def emulate_graph(  
            self, 
            edges: List[Tuple[int | str, int | str, int]] | List[Tuple[int | str, int | str]] | None = None, 
            graph: DiGraph | Graph | None = None, 
            move_semantics: bool = True, 
            sort_ids: bool = False
        ) -> None:
        """
        Creates an emulating graph based on a given original graph
        Can be initialized through a list of edges or a networkx graph
        Referred to as 'static building'
        Adds Vertex objects to the emulating graph
        """
        assert edges is not None or graph is not None

        # allow emulate graph to be called at most once
        if not self.can_emulate:
            return
        self.can_emulate = False

        # prepare networkx graph 
        match graph:
            case DiGraph():
                DG = graph   
            case Graph():
                DG = DiGraph(graph)
            case _:
                DG = DiGraph()

        # add edges to DiGraph
        if isinstance(edges, list):
            weighted_edges: List[Tuple[int | str, int | str, int]] = []
            weight = 0
            for edge in edges:
                src = edge[0]
                dst = edge[1]
                if len(edge) == 3:
                    weight = edge[2]
            DG.add_weighted_edges_from(weighted_edges)
           
        # local storage for building
        v_ids: Dict[int | str, int] = dict()
        entry_points: Dict[int, SmartPointer] = dict()

        if sort_ids:
            nodes = sorted(list(DG.nodes))
        else:
            nodes = list(DG.nodes)

        if move_semantics:
            self.__emulate_graph(DG, v_ids, entry_points, nodes)
        else:
            self.__emulate_graph_full_copy(DG, v_ids, entry_points, nodes)

        # transfer local storage to smart AVL tree
        for v_name, v_id in v_ids.items():
            self.v_ids[v_name] = v_id

        for v_id, p in entry_points.items():
            self.entry_points[v_id] = p
    
    def __emulate_graph_full_copy(  
            self, 
            DG: DiGraph, 
            v_ids: Dict[int | str, int] | SmartAVLTree, 
            entry_points: Dict[int, SmartPointer] | SmartAVLTree,
            nodes: List[str] | List[int]
        ) -> None:
        """Connects each Vertex through pointers and FanOuts"""
        # create entry points
        for v_name in nodes:
            out_degree = len(list(DG.neighbors(v_name)))
            self.add_vertex(v_name=v_name, v_ids=v_ids, entry_points=entry_points, out_degree=out_degree)

        # build edge tree
        for src_name in DG.nodes:
            out_degree = len(list(DG.neighbors(src_name)))
            if out_degree <= 0:
                continue

            height = ceil(log(out_degree, self.branching_factor)) - 1
            sq = queue.init_queue()
            src_id = v_ids[src_name]

            total_allocs = 0
            for dst_name in DG.neighbors(src_name):
                dst_id = v_ids[dst_name]
                p_dst = entry_points[dst_id]

                try:
                    weight = DG.edges[src_name, dst_name]["weight"]
                except:
                    weight = 0

                pre_alloc_count = get_global_counter()[0]
                v_out_leaf = FanOut(
                    children=[None]*self.branching_factor,
                    ogE=SmartPointer.copy(p_dst),
                    weight=weight,
                    dst_id=dst_id,
                    is_leaf=True,
                    label=None
                )

                self.ogV_counts[dst_id] += 1

                total_allocs += get_global_counter()[0] - pre_alloc_count
                p_out_leaf = SmartPointer(SmartPointer.new(v_out_leaf))
                queue.enqueue(sq, p_out_leaf)

            sq = self.__build_edge_tree(sq=sq, height=height, src_id=src_id)

            # assign FanOut children
            out_children = []
            for i in range(self.branching_factor):
                p_child = queue.dequeue(sq)
                out_children.append(p_child)
            
            while sq.head is not None:
                queue.dequeue(sq)

            # update ogV entry
            p_src = entry_points[src_id]
            p_src, ogV = self.get_ogV_full_copy(p_src)
            ogV.out_children = out_children
            SmartPointer.put(p_src, ogV)
            SmartPointer.delete(ogV)

    def __emulate_graph(    
            self, 
            DG: DiGraph, 
            v_ids: Dict[int | str, int] | SmartAVLTree, 
            entry_points: Dict[int, SmartPointer] | SmartAVLTree,
            nodes: List[str] | List[int]
        ) -> None:
        """Connects each Vertex through pointers and FanOuts"""
        # create entry points
        for v_name in nodes:
            out_degree = len(list(DG.neighbors(v_name)))
            self.add_vertex(v_name=v_name, v_ids=v_ids, entry_points=entry_points, out_degree=out_degree)

        # build edge tree
        for src_name in DG.nodes:
            out_degree = len(list(DG.neighbors(src_name)))
            if out_degree <= 0:
                continue

            height = ceil(log(out_degree, self.branching_factor)) - 1
            sq = queue.init_queue()
            src_id = v_ids[src_name]

            for dst_name in DG.neighbors(src_name):
                dst_id = v_ids[dst_name]
                p_dst = entry_points[dst_id]

                try:
                    weight = DG.edges[src_name, dst_name]["weight"]
                except:
                    weight = 0

                v_out_leaf = FanOut(
                    children=[None]*self.branching_factor,
                    ogE=SmartPointer.copy(p_dst),
                    weight=weight,
                    dst_id=dst_id,
                    is_leaf=True,
                    label=None
                )

                self.ogV_counts[dst_id] += 1
                p_out_leaf = SmartPointer(SmartPointer.new(v_out_leaf))
                queue.enqueue(sq, p_out_leaf)

            sq = self.__build_edge_tree(sq=sq, height=height, src_id=src_id)

            # assign FanOut children
            out_children = []
            for i in range(self.branching_factor):
                p_child = queue.dequeue(sq)
                out_children.append(p_child)
            
            while sq.head is not None:
                queue.dequeue(sq)

            # update ogV entry
            set_attr = {"out_children": out_children}
            p_src = entry_points[src_id]
            SmartPointer.put_attr(p_src, set_attr)

    def delete_vertex_full_copy(self, src_name: int | str) -> None:
        """
        Delete the Vertex pointed to by p_src (src_name's 
        pointer) and its FanOut tree
        Replace with a DeletedObject so other pointers
        that share a reference with p_src know it was deleted
        """
        src_id = self.v_ids[src_name]
        p_src = self.entry_points.delete(src_id)
        if p_src is None:
            return

        # load ogV
        p_src, ogV = self.get_ogV_full_copy(p_src)

        # queue children of src to visit
        sq = queue.init_queue()
        for i in range(self.branching_factor):
            if ogV.out_children[i] is not None:
                queue.enqueue(sq, ogV.out_children[i])
                ogV.out_children[i] = None
            else:
                break

        SmartPointer.delete(ogV)

        # delete FanOut tree
        while sq.head is not None:
            ogE = None
            p = queue.dequeue(sq)
            if p is None:
                continue 
            
            # load v 
            v = SmartPointer.get_and_copy(p)
            assert v.type == "FAN_OUT"
            self.out_tree_accesses += 1
            self.out_tree_degree += 2

            # at internal FanOut
            if not v.is_leaf:
                assert v.ogE is None and v.dst_id is None
                for i in range(self.branching_factor):
                    if v.children[i] is not None:
                        queue.enqueue(sq, v.children[i])
                        v.children[i] = None
                    else:
                        break
        
            # ogE is reached
            else:
                self.ogV_counts[v.dst_id] -= 1
                ogE = v.ogE
                v.ogE = None
                v.dst_id = None
            
            # delete pointers at original copy
            SmartPointer.put(p, v)
            SmartPointer.delete(v)
            SmartPointer.delete(p)

            # need to delete ogE at original FanOut first
            # otherwise is_single_reference always fails
            if isinstance(ogE, SmartPointer):
                if SmartPointer.is_single_reference(ogE): 
                    self.num_deleted_objs -= 1
                SmartPointer.delete(ogE)

        # wait until tree has been deleted so that putting a deleted object
        # as the pointee; original smart pointer always deletes with put()
        self.ogV_counts[src_id] -= 1
        deleted_obj = DeletedObject(id=src_id)
        SmartPointer.put(p_src, deleted_obj, delete_old=True)
        self.num_deleted_objs += 1
        if SmartPointer.is_single_reference(p_src): 
            self.num_deleted_objs -= 1

        SmartPointer.delete(p_src)

    def delete_vertex(self, src_name: int | str, make_copies: bool = True) -> None:
        """
        Delete the Vertex pointed to by p_src (src_name's pointer) and its FanOut tree
        Replace with a DeletedObject so other pointers
        that share a reference with p_src know it was deleted
        """
        src_id = self.v_ids[src_name]
        p_src = self.entry_points.delete(src_id)
        if p_src is None:
            return

        # load ogV attrs
        req_attr = ["out_children", "type", "id"]
        p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)
        out_children = v_attr["out_children"]

        # queue children of src to visit
        sq = queue.init_queue()
        for i in range(self.branching_factor):
            if out_children[i] is not None:
                queue.enqueue(sq, out_children[i])
            else:
                break

        # delete FanOut tree
        req_attr = ["type", "is_leaf", "children", "dst_id", "ogE"]
        while sq.head is not None:
            p = queue.dequeue(sq)
            if p is None:
                continue 
            
            # load v attributes
            v_attr = SmartPointer.get_attr(p, req_attr, make_copies)
            assert v_attr["type"] == "FAN_OUT"
            self.out_tree_accesses += 1
            degree = 2 if SmartPointer.is_temp_copy(p) else 1
            self.out_tree_degree += degree
            children = v_attr["children"]
            ogE = None

            # at internal node
            if not v_attr["is_leaf"]:
                assert v_attr["ogE"] is None and v_attr["dst_id"] is None
                for i in range(self.branching_factor):
                    if children[i] is not None:
                        queue.enqueue(sq, children[i])
                        children[i] = None
                    else:
                        break
                        
            # ogE is reached
            else:
                self.ogV_counts[v_attr["dst_id"]] -= 1
                ogE = v_attr["ogE"]
            
            # update pointer with deleted pointers
            set_attr = {"children": children, "ogE": None, "dst_id": None, "weight": 0}
            SmartPointer.put_attr(p, set_attr, delete_old=make_copies)
            SmartPointer.delete(p)

            # wait until underlying tree and the original ogE are deleted
            # so that make_copies doesn't prevent us from using
            # is_single_reference correctly
            if isinstance(ogE, SmartPointer):
                if SmartPointer.is_single_reference(ogE): 
                    self.num_deleted_objs -= 1
                SmartPointer.delete(ogE)

        # place deleted object in place of ogV
        self.ogV_counts[src_id] -= 1
        deleted_obj = DeletedObject(id=src_id)
        SmartPointer.put(p_src, deleted_obj, delete_old=make_copies)
        self.num_deleted_objs += 1
        if SmartPointer.is_single_reference(p_src): 
            self.num_deleted_objs -= 1
        SmartPointer.delete(p_src)

    def delete_edge_full_copy(self, src_name: int | str, dst_name: int | str, alert_missing: bool = True) -> bool:
        """
        Deletes the edge between src and dst
        Downsizes the tree by one level if out degree gets too small
        """
        # get src_id/pointer and dst_id
        src_id, p_src = self.get_pointer(src_name) 
        dst_id = self.v_ids[dst_name]

        # rearrange and resize
        new_out_degree, found = self.reorder_edge_tree_full_copy(p_src, None, dst_id)
        self.downsize_edge_tree_full_copy(p_src, new_out_degree)

        if not found:
            if alert_missing: print(f"Could not find edge: {src_name}->{dst_name}")
            return False
        else:
            return True
            
    def delete_edge(self, src_name: int | str, dst_name: int | str, alert_missing: bool = True, make_copies: bool = True) -> bool:
        """
        Deletes the edge between src and dst
        Downsizes the tree by one level if out degree gets too small
        """

        # get src_id/pointer and dst_id
        src_id, p_src = self.get_pointer(src_name) 
        dst_id = self.v_ids[dst_name]
    
        # rearrange and resize
        new_out_degree, found = self.reorder_edge_tree(p_src, None, dst_id, make_copies)
        self.downsize_edge_tree(p_src, new_out_degree, make_copies)

        if not found:
            if alert_missing: print(f"Could not find edge: {src_name}->{dst_name}")
            return False
        else:
            return True

    def process_deleted_objects_full_copy(self, p: SmartPointer, v: FanOut, out_degree: int) -> Tuple[SmartPointer, Vertex | FanOut, int]:
        """
        If a Vertex was deleted somewhere in the graph, we need
        to check every ogE, as it could point to a DeletedObject
        """
        
        # check if ogE points to vertex or deleted object
        pre_alloc_count = get_global_counter()[0]
        assert isinstance(v.ogE, SmartPointer)
        obj = SmartPointer.get_and_copy(v.ogE)

        if isinstance(obj, Vertex):
            self.ogV_depth.append(get_global_counter()[0] - pre_alloc_count)
            self.ogV_degree += self.ogV_counts[obj.id]
            self.ogV_accesses += 1
            SmartPointer.delete(obj)
            return p, v, out_degree
            
        assert isinstance(obj, DeletedObject)

        # save ogE    
        ogE = v.ogE
        dst_id = v.dst_id

        # delete ogE at parent pointer
        v.ogE = None
        v.dst_id = None
        v.weight = 0
        SmartPointer.put(p, v)
        
        # delete ogE temp copy and check single reference
        assert isinstance(dst_id, int)
        if SmartPointer.is_single_reference(ogE):
            self.num_deleted_objs -= 1
            del self.v_ids[dst_id]
        SmartPointer.delete(ogE)
        self.ogV_counts[dst_id] -= 1
        out_degree -= 1

        return p, v, out_degree

    def process_deleted_objects(    
            self, 
            p: SmartPointer, 
            ogE: SmartPointer, 
            dst_id: int, 
            out_degree: int, 
            make_copies: bool
        ) -> Tuple[SmartPointer, SmartPointer | None, int | None, int]:
        """
        If a Vertex was deleted somewhere in the graph, we need
        to check every ogE, as it could point to a DeletedObject
        """
        
        # a pointer with exactly one reference must point to a deleted 
        # object otherwise it would have at least two pointers between 
        # itself and its entry point in the AVL Tree
        if make_copies or not SmartPointer.is_single_reference(ogE):
            # check if ogE points to vertex or deleted object
            req_attr = ["type", "id"]
            pre_alloc_count = get_global_counter()[0]
            obj_attr = SmartPointer.get_attr(ogE, req_attr, make_copies=False)
            
            if obj_attr["type"] == "VERTEX":
                self.ogV_depth.append(get_global_counter()[0] - pre_alloc_count)
                self.ogV_degree += self.ogV_counts[obj_attr["id"]]
                self.ogV_accesses += 1
                return p, ogE, dst_id, out_degree

            assert obj_attr["type"] == "DELETED_OBJECT"
            
        # update parent
        set_attr = {"ogE": None, "dst_id": None, "weight": 0}
        SmartPointer.put_attr(p, set_attr, delete_old=make_copies)

        # delete ogE
        if SmartPointer.is_single_reference(ogE): 
            self.num_deleted_objs -= 1
            del self.v_ids[dst_id]
        SmartPointer.delete(ogE)
        self.ogV_counts[dst_id] -= 1
        out_degree -= 1

        return p, None, None, out_degree

    def visit_full_copy(self, p_src: SmartPointer, ogV: Vertex, label: int | None) -> Tuple[Vertex, int]:
        """Update Vertex as visited and with new label"""
        # update label at ogV
        ogV.label = label
        ogV.visited = True
        src_id = ogV.id  
        SmartPointer.put(p_src, ogV)
        return ogV, src_id

    def visit(self, p_src: SmartPointer, label: int | None, make_copies: bool = True) -> Tuple[SmartPointer, int]:
        """Update Vertex as visited and with new label"""
        # update label at ogV
        req_attr = ["type", "id"]
        p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)
        src_id = v_attr["id"]
        set_attr = {"label": label, "visited": True}
        SmartPointer.put_attr(p_src, set_attr)
        return p_src, src_id

    def unvisit_full_copy(self, visited: queue) -> None:
        """Reset visited status for every Vertex"""
        # reset all visited vertices 
        while visited.head is not None:
            p = queue.dequeue(visited)
            if p is not None:
                p, v = self.get_ogV_full_copy(p)
                v.visited = False
                SmartPointer.put(p, v)
                SmartPointer.delete(v)
                if SmartPointer.is_temp_copy(p): self.ogV_counts[v.id] -= 1
                SmartPointer.delete_temp_copy(p)

    def unvisit(self, visited: queue, make_copies: bool = True) -> None:
        """Reset visited status for every Vertex"""
        # reset all visited vertices 
        set_attr = {"visited": False}
        while visited.head is not None:
            po = queue.dequeue(visited)
            if po is not None:
                p, og_id = po
                SmartPointer.put_attr(p, set_attr)
                if SmartPointer.is_temp_copy(p): self.ogV_counts[og_id] -= 1 
                SmartPointer.delete_temp_copy(p)

    def add_neighbors_full_copy(    
            self, 
            p_src: SmartPointer, 
            ogV: Vertex, 
            ds: str, 
            pDS: queue | int | Set[int] | List[int] | None, 
            getL: str | None = None, 
            getP: str | None = None
        ) -> int | None:
        """Perform BFS on an FanOut tree and collect neighbors in some data structure"""
        # load ogV
        og_id = ogV.id
        og_label = ogV.label
        out_degree = new_out_degree = ogV.out_degree
        height = ogV.height

        # queue first level of vertices for visiting
        sq = queue.init_queue()
        for i in range(self.branching_factor):
            if ogV.out_children[i] is not None:
                # queue child
                queue.enqueue(sq, ogV.out_children[i])
                ogV.out_children[i] = None
            else:
                break

        # delete ogV
        SmartPointer.delete(ogV)

        # traverse down until ogEs are reached
        while sq.head is not None:
            p = queue.dequeue(sq)
            if p is None:
                continue 
            
            # load v
            v = SmartPointer.get_and_copy(p)
            assert v.type == "FAN_OUT"
            self.out_tree_accesses += 1
            self.out_tree_degree += 2

            # FanOut children contains ogEs or more fan_outs
            if not v.is_leaf:
                assert v.ogE is None
                for i in range(self.branching_factor):
                    if v.children[i] is not None:
                        # queue child
                        queue.enqueue(sq, v.children[i])
                        v.children[i] = None
                    else:
                        break
            
            # check if ogE was deleted
            if v.ogE is not None and self.num_deleted_objs > 0:
                p, v, new_out_degree = self.process_deleted_objects_full_copy(p, v, new_out_degree)

            # process ogEs
            if v.ogE is not None:
                # get proper label
                label: int | None
                match getL:
                    case "ID":
                        label = og_id
                    case "LW":
                        label = og_label + v.weight
                    case "DST_ID":
                        label = v.dst_id
                    case _:
                        label = None

                # get proper priority
                match getP:
                    case "LW":
                        pty = og_label + v.weight
                    case "W":
                        pty = v.weight
                    case _:
                        pty = None

                # put ogE in appropriate data structure
                match ds:
                    case "QUEUE":
                        assert isinstance(pDS, queue)
                        queue.enqueue(pDS, [label, v.ogE])
                    case "STACK":
                        assert isinstance(pDS, int) or pDS is None
                        pDS = stack.push(pDS, [label, v.ogE])
                    case "PQ":
                        PQ.insert(pty, [label, v.ogE, og_id])
                    case "SET":
                        assert isinstance(pDS, set)
                        pDS.add(v.dst_id)
                        SmartPointer.delete_temp_copy(v.ogE)
                    case "LIST":
                        assert isinstance(pDS, list)
                        pDS.append(v.dst_id)
                        SmartPointer.delete_temp_copy(v.ogE)
                
                if ds not in ["SET", "LIST"]: self.ogV_counts[v.dst_id] += 1
                v.ogE = None
            
            SmartPointer.delete(v)
            SmartPointer.delete_temp_copy(p)

        # rearrange and resize FanOut tree
        if new_out_degree < out_degree:
            new_out_degree, found = self.reorder_edge_tree_full_copy(p_src, new_out_degree)
            self.downsize_edge_tree_full_copy(p_src, new_out_degree)

        if ds == "STACK" and (isinstance(pDS, int) or pDS is None):
            return pDS
        return None

    def add_neighbors(  
            self, 
            p_src: SmartPointer,
            ds: str, 
            pDS: queue | int | Set[int] | List[int] | None, 
            getL: str | None = None, 
            getP: str | None =None, 
            make_copies: bool = True
        ) -> int | None:
        """Perform BFS on an FanOut tree and collect neighbors in some data structure"""
        # load ogV
        req_attr = ["type", "out_children", "id", "label", "out_degree", "height"]
        p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)
        og_id = v_attr["id"]
        og_label = v_attr["label"]
        out_degree = new_out_degree = v_attr["out_degree"]
        height = v_attr["height"]
        out_children = v_attr["out_children"]

        # queue first level of vertices for visiting
        sq = queue.init_queue()
        for i in range(self.branching_factor):
            if out_children[i] is not None:
                # queue child
                queue.enqueue(sq, out_children[i])
            else:
                break

        # exclude ogE if possible to avoid useless copies
        req_attr = ["type", "is_leaf", "children", "weight", "ogE", "dst_id"]
        if ds in ["LIST", "SET"] and self.num_deleted_objs <= 0: req_attr.remove("ogE")

        # traverse down until ogEs are reached 
        while sq.head is not None:
            p = queue.dequeue(sq)
            if p is None:
                continue 

            # load v attributes
            v_attr = SmartPointer.get_attr(p, req_attr, make_copies)
            assert v_attr["type"] == "FAN_OUT"
            self.out_tree_accesses += 1
            degree = 2 if SmartPointer.is_temp_copy(p) else 1
            self.out_tree_degree += degree

            # important variables
            ogE: SmartPointer | None = v_attr["ogE"] if "ogE" in v_attr else None
            dst_id = v_attr["dst_id"]
            is_leaf = v_attr["is_leaf"]
            weight = v_attr["weight"]
            children = v_attr["children"]
                    
            # FanOut children contains ogEs or more fan_outs
            if not is_leaf:
                assert ogE is None
                for i in range(self.branching_factor):
                    if children[i] is not None:
                        # queue child
                        queue.enqueue(sq, children[i])
                    else:
                        break

            # check if ogE was deleted
            if ogE is not None and self.num_deleted_objs > 0:
                p, ogE, dst_id, new_out_degree = self.process_deleted_objects(p, ogE, dst_id, new_out_degree, make_copies)

            # process ogEs
            if ogE is not None or (dst_id is not None and ds in ["LIST", "SET"]):
                # get proper label
                label: int | None
                match getL:
                    case "ID":
                        label = og_id
                    case "LW":
                        label = og_label + weight
                    case "DST_ID":
                        label = dst_id
                    case _:
                        label = None

                # get proper priority
                match getP:
                    case "LW":
                        pty = og_label + weight
                    case "W":
                        pty = weight
                    case _:
                        pty = None
            
                # put ogE in appropriate data structure
                match ds:
                    case "QUEUE":
                        assert isinstance(pDS, queue)
                        queue.enqueue(pDS, [label, ogE])
                    case "STACK":
                        assert isinstance(pDS, int) or pDS is None
                        pDS = stack.push(pDS, [label, ogE])
                    case "PQ":
                        PQ.insert(pty, [label, ogE, og_id])
                    case "SET":
                        assert isinstance(pDS, set)
                        pDS.add(dst_id)
                        SmartPointer.delete_temp_copy(ogE)
                        ogE = None
                    case "LIST":
                        assert isinstance(pDS, list)
                        pDS.append(dst_id)
                        SmartPointer.delete_temp_copy(ogE)
                        ogE = None
                        
                if SmartPointer.is_temp_copy(ogE): 
                    self.ogV_counts[dst_id] += 1
            
            SmartPointer.delete_temp_copy(p)

        # rearrange and resize FanOut tree
        if new_out_degree < out_degree:
            new_out_degree, found = self.reorder_edge_tree(p_src, new_out_degree, None, make_copies)
            self.downsize_edge_tree(p_src, new_out_degree, make_copies)

        # only need to return stack addresses
        # all other data structures are changed in place
        if ds == "STACK" and (isinstance(pDS, int) or pDS is None):
            return pDS
        return None

    def reorder_edge_tree_full_copy(   
            self, 
            p_src: SmartPointer, 
            new_out_degree: int | None = None, 
            dst_id_to_delete: int | None = None
        ) -> Tuple[int, bool]:
        """
        Rearrange FanOut tree so that valid ogEs to ogVs
        populate the FanOut tree leaves from left to right
        Occurs when DeletedObjects are encountered 
        """

        # load ogV
        p_src, ogV = self.get_ogV_full_copy(p_src)
        height = ogV.height
        if new_out_degree is None: new_out_degree = ogV.out_degree
        found = False

        # assign each ogV/FanOut an id
        parent_id = 0
        child_id = 1
        parent_map = dict() # child ids to parent ids
        child_map = dict() # parent ids to list of children ids
        child_list = []

        # storage devices for preserving left to right order
        # if a deleted object is found
        deleted_leaf_sq = queue.init_queue() 
        leaf_top = stack.init_stack() # stack pointers to leaf FanOuts
        internal_top = stack.init_stack() # stack pointers to internal FanOuts
        internal_top = stack.push(internal_top, (0, None))

        # queue first level of vertices for visiting
        sq = queue.init_queue()
        for i in range(self.branching_factor):
            if ogV.out_children[i] is not None:
                # queue child
                queue.enqueue(sq, ogV.out_children[i])
                ogV.out_children[i] = None

                # update maps
                child_list.append(child_id)
                parent_map[child_id] = parent_id
                child_id += 1
            else:
                break

        SmartPointer.delete(ogV)

        # update maps
        child_map[parent_id] = child_list
        parent_id += 1
        child_list = []

        # traverse down until ogEs are reached 
        while sq.head is not None:
            p = queue.dequeue(sq)
            if p is None:
                continue 

            # load v attributes
            v = SmartPointer.get_and_copy(p)
            assert v.type == "FAN_OUT"
            self.out_tree_accesses += 1
            self.out_tree_degree += 2

            # FanOut children contains ogEs or more fan_outs
            if not v.is_leaf:
                assert v.ogE is None
                for i in range(self.branching_factor):
                    if v.children[i] is not None:
                        # queue child
                        queue.enqueue(sq, v.children[i])
                        v.children[i] = None

                        # update maps
                        child_list.append(child_id)
                        parent_map[child_id] = parent_id
                        child_id += 1
                    else:
                        break

            # update maps
            child_map[parent_id] = child_list
            child_list = []

            # check if ogE is the edge we want to delete
            if dst_id_to_delete is not None and dst_id_to_delete == v.dst_id:
                found = True
                SmartPointer.delete(v.ogE)
                v.ogE = v.dst_id = None
                v.weight = 0
                SmartPointer.put(p, v, delete_old=True)
                self.ogV_counts[dst_id_to_delete] -= 1
                new_out_degree -= 1

            # check if ogE was deleted
            if v.ogE is not None and self.num_deleted_objs > 0:
                p, v, new_out_degree = self.process_deleted_objects_full_copy(p, v, new_out_degree)

            # queue internal pointers
            if not v.is_leaf:
                internal_top = stack.push(internal_top, (parent_id, p))

            # stack FanOut leaves with an ogE
            elif v.ogE is not None:
                leaf_top = stack.push(leaf_top, (parent_id, p, v))

            # track id of leaf FanOuts with no ogE
            else:
                queue.enqueue(deleted_leaf_sq, (parent_id, p))   

            # increase parent_id
            parent_id += 1

        # shift ogEs in right of tree to replace deleted
        # ogEs in the left side of the tree
        parent_indices_to_update: Dict[int, List[int]] = dict()
        while deleted_leaf_sq.head is not None:
            # get pointer to FanOut with deleted ogE
            ip1 = queue.dequeue(deleted_leaf_sq)
            if ip1 is None:
                deleted_id = p_deleted = None
            else:           
                deleted_id, p_deleted = ip1
            
            # get pointer to FanOut with intact ogE
            ip2, leaf_top = stack.pop(leaf_top)   
            if ip2 is None:
                child_id = -1
                p_child = v_child = None 
            else: 
                child_id, p_child, v_child = ip2

            # only swap if the child/leaf is to the right 
            # of the pointer with the deleted ogE
            if deleted_id is not None:
                if child_id >= 0 and deleted_id < child_id:
                    assert isinstance(p_deleted, SmartPointer) 
                    assert isinstance(v_child, FanOut)
                    assert isinstance(p_child, SmartPointer)

                    # collect index of parent to update
                    parent_id = parent_map[child_id]
                    i = child_map[parent_id].index(child_id)

                    # transfer rightmost attrs left
                    SmartPointer.put(p_deleted, v_child, delete_old=True)
                    SmartPointer.delete(v_child)
                    v_child.ogE = None
                    v_child.dst_id = None
                    v_child.weight = 0
                    SmartPointer.put(p_child, v_child, delete_old=True)
                    
                else:
                    # collect index of parent to update
                    parent_id = parent_map[deleted_id]
                    i = child_map[parent_id].index(deleted_id)

                # delete v_child
                SmartPointer.delete(v_child)

                # dictionary of indices to update
                if parent_id in parent_indices_to_update:
                    parent_indices_to_update[parent_id].append(i)
                else:
                    parent_indices_to_update[parent_id] = [i]

            SmartPointer.delete_temp_copy(p_child)
            SmartPointer.delete_temp_copy(p_deleted)

        # delete leftover pointers
        while leaf_top is not None:
            ip2, leaf_top = stack.pop(leaf_top)   
            if ip2 is None:
                continue

            child_id, p_child, v_child = ip2
            SmartPointer.delete(v_child)
            SmartPointer.delete_temp_copy(p_child)

        # delete leftover pointers
        while internal_top is not None:
            ip3, internal_top = stack.pop(internal_top)   
            if ip3 is None:
                continue
            parent_id, p_parent = ip3

            if parent_id in parent_indices_to_update:
                # get children of internal parent
                if parent_id == 0:
                    p_src, v = self.get_ogV_full_copy(p_src)
                    children = v.out_children

                else: 
                    v = SmartPointer.get_and_copy(p_parent)
                    self.out_tree_accesses += 1
                    self.out_tree_degree += 2
                    children = v.children

                # update deleted children
                for i in parent_indices_to_update[parent_id]:
                    children[i] = None

                # put changes back
                if parent_id == 0:
                    SmartPointer.put(p_src, v, delete_old=True)
                else: 
                    SmartPointer.put(p_parent, v, delete_old=True)

            SmartPointer.delete_temp_copy(p_parent)

        return new_out_degree, found

    def reorder_edge_tree( 
            self, 
            p_src: SmartPointer,
            new_out_degree: int | None = None, 
            dst_id_to_delete: int | None = None,
            make_copies: bool = True
        ) -> Tuple[int, bool]:
        """
        Rearrange FanOut tree so that valid ogEs to ogVs
        populate the FanOut tree leaves from left to right
        Occurs when DeletedObjects are encountered 
        """

        # load ogV
        req_attr = ["type", "out_children", "id", "height", "out_degree"]
        p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)
        height = v_attr["height"]
        out_children = v_attr["out_children"]
        if new_out_degree is None: new_out_degree = v_attr["out_degree"]
        found = False

        # assign each ogV/FanOut an id
        parent_id = 0
        child_id = 1
        parent_map = dict() # child ids to parent ids
        child_map = dict() # parent ids to list of children ids
        child_list = []

        # storage devices for preserving left to right order
        # if a deleted object is found
        deleted_leaf_sq = queue.init_queue() 
        leaf_top = stack.init_stack() # stack pointers to leaf FanOuts
        internal_top = stack.init_stack() # stack pointers to internal FanOuts
        internal_top = stack.push(internal_top, (0, None))

        # queue first level of vertices for visiting
        sq = queue.init_queue()
        for i in range(self.branching_factor):
            if out_children[i] is not None:
                # queue child
                queue.enqueue(sq, out_children[i])

                # update maps
                child_list.append(child_id)
                parent_map[child_id] = parent_id
                child_id += 1
            else:
                break

        # update maps
        child_map[parent_id] = child_list
        parent_id += 1
        child_list = []

        # traverse down until ogEs are reached 
        req_attr = ["type", "is_leaf", "children", "ogE", "dst_id", "weight"]
        while sq.head is not None:
            p = queue.dequeue(sq)
            if p is None:
                continue 

            # load v attributes
            v_attr = SmartPointer.get_attr(p, req_attr, make_copies)
            assert v_attr["type"] == "FAN_OUT"
            self.out_tree_accesses += 1
            degree = 2 if SmartPointer.is_temp_copy(p) else 1
            self.out_tree_degree += degree

            # important variables
            ogE = v_attr["ogE"] if "ogE" in v_attr else None
            dst_id = v_attr["dst_id"]
            is_leaf = v_attr["is_leaf"]
            children = v_attr["children"]
            weight = v_attr["weight"]
                    
            # FanOut children contains ogEs or more fan_outs
            if not is_leaf:
                assert ogE is None
                for i in range(self.branching_factor):
                    if children[i] is not None:
                        # queue child
                        queue.enqueue(sq, children[i])

                        # update maps
                        child_list.append(child_id)
                        parent_map[child_id] = parent_id
                        child_id += 1
                    else:
                        break

            # update maps
            child_map[parent_id] = child_list
            child_list = []

            # check if ogE is the edge we want to delete
            if dst_id_to_delete is not None and dst_id_to_delete == dst_id:
                found = True
                set_attr = {"ogE": None, "dst_id": None, "weight": 0}
                SmartPointer.put_attr(p, set_attr, delete_old=make_copies)
                SmartPointer.delete(ogE)
                self.ogV_counts[dst_id] -= 1
                new_out_degree -= 1
                ogE = dst_id = None

            # check if ogE points to a deleted object
            if ogE is not None and self.num_deleted_objs > 0:
                p, ogE, dst_id, new_out_degree = self.process_deleted_objects(p, ogE, dst_id, new_out_degree, make_copies)

            # queue internal pointers
            if not is_leaf:
                internal_top = stack.push(internal_top, (parent_id, p))

            # stack FanOut leaves with an ogE
            elif ogE is not None:
                leaf_top = stack.push(leaf_top, (parent_id, p, ogE, dst_id, weight))

            # track id of leaf FanOuts with no ogE
            else:
                queue.enqueue(deleted_leaf_sq, (parent_id, p))   

            # increase parent_id
            parent_id += 1
        
        # shift ogEs in right of tree to replace deleted
        # ogEs in the left side of the tree
        parent_indices_to_update: Dict[int, List[int]] = dict()
        while deleted_leaf_sq.head is not None:
            # get pointer to FanOut with deleted ogE
            ip1 = queue.dequeue(deleted_leaf_sq)
            if ip1 is None:
                deleted_id = p_deleted = None
            else:           
                deleted_id, p_deleted = ip1
            
            # get pointer to FanOut with intact ogE
            ip2, leaf_top = stack.pop(leaf_top)   
            if ip2 is None:
                child_id = -1
                p_child = ogE = dst_id = weight = None
            else: 
                child_id, p_child, ogE, dst_id, weight = ip2

            # only swap if the child/leaf is to the right 
            # of the pointer with the deleted ogE
            if deleted_id is not None:
                if child_id >= 0 and deleted_id < child_id:
                    assert isinstance(p_deleted, SmartPointer)
                    assert isinstance(p_child, SmartPointer)

                    # collect index of parent to update
                    parent_id = parent_map[child_id]
                    i = child_map[parent_id].index(child_id)

                    # transfer rightmost attrs left
                    set_attr = {"ogE": ogE, "dst_id": dst_id, "weight": weight}
                    SmartPointer.put_attr(p_deleted, set_attr, delete_old=make_copies)

                    # update rightmost pointer with deleted items
                    set_attr = {"ogE": None, "dst_id": None, "weight": None}
                    SmartPointer.put_attr(p_child, set_attr, delete_old=make_copies)

                else:
                    # collect index of parent to update
                    parent_id = parent_map[deleted_id]
                    i = child_map[parent_id].index(deleted_id)
                    SmartPointer.delete_temp_copy(ogE)

                # dictionary of indices to update
                if parent_id in parent_indices_to_update:
                    parent_indices_to_update[parent_id].append(i)
                else:
                    parent_indices_to_update[parent_id] = [i]

            SmartPointer.delete_temp_copy(p_child)
            SmartPointer.delete_temp_copy(p_deleted)

        # delete leftover pointers
        while leaf_top is not None:
            ip2, leaf_top = stack.pop(leaf_top)   
            if ip2 is None:
                continue

            child_id, p_child, ogE, dst_id, weight = ip2
            SmartPointer.delete_temp_copy(p_child)
            SmartPointer.delete_temp_copy(ogE)

        # delete leftover pointers
        while internal_top is not None:
            ip3, internal_top = stack.pop(internal_top)   
            if ip3 is None:
                continue
            parent_id, p_parent = ip3

            if parent_id in parent_indices_to_update:
                # get children of internal parent
                if parent_id == 0:
                    req_attr = ["id", "type", "out_children"]
                    p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)
                    children = v_attr["out_children"]

                else: 
                    children = SmartPointer.get_attr(p_parent, "children", make_copies)
                    self.out_tree_accesses += 1
                    degree = 2 if SmartPointer.is_temp_copy(p_parent) else 1
                    self.out_tree_degree += degree

                # update deleted children
                for i in parent_indices_to_update[parent_id]:
                    children[i] = None

                # put changes back
                if parent_id == 0:
                    SmartPointer.put_attr(p_src, {"out_children": children}, delete_old=make_copies)
                else: 
                    SmartPointer.put_attr(p_parent, {"children": children}, delete_old=make_copies)

            SmartPointer.delete_temp_copy(p_parent)

        return new_out_degree, found

    def downsize_edge_tree_full_copy(self, p_src: SmartPointer, new_out_degree: int) -> None:
        """Downsize FanOut tree by one level"""
        # load ogV
        p_src, ogV = self.get_ogV_full_copy(p_src)
        ogV.out_degree = new_out_degree

        # calculate number of levels to remove
        if new_out_degree == 0:
            start = 0
            new_height = 0
        else:
            start = 1
            new_height = ogV.height
            while new_out_degree / (self.branching_factor ** new_height) <= 1 / (self.branching_factor ** 2):
                new_height -= 1

        if new_height < ogV.height:
            # queue children to delete
            sq = queue.init_queue()
            for i in range(start, self.branching_factor):
                if ogV.out_children[i] is not None:
                    queue.enqueue(sq, ogV.out_children[i])
                    ogV.out_children[i] = None
                else:
                    break

            # transfer children of left subtree to immediate children of ogV
            if start:
                p = ogV.out_children[0]
                ogV.out_children[0] = None
                for i in range(ogV.height-new_height):
                    # get next level of children
                    assert isinstance(p, SmartPointer)
                    v = SmartPointer.get_and_copy(p)
                    assert v.type == "FAN_OUT" and not v.is_leaf
                    self.out_tree_accesses += 1
                    self.out_tree_degree += 2

                    # queue right children
                    if i < ogV.height - new_height - 1:
                        for j in range(start, self.branching_factor):
                            if v.children[j] is not None:
                                queue.enqueue(sq, v.children[j])
                                v.children[j] = None
                            else:
                                break

                        # get leftmost child
                        p_child = v.children[0]
                        v.children[0] = None
                        SmartPointer.put(p, v)
                        SmartPointer.delete(v)
                        SmartPointer.delete(p)
                        p = p_child

                    else:
                        # set leftmost children as new ogV children
                        for j in range(self.branching_factor):
                            ogV.out_children[j] = v.children[j]
                            v.children[j] = None

                        # delete leftmost child and its child pointers
                        SmartPointer.put(p, v)
                        SmartPointer.delete(v)
                        SmartPointer.delete(p)

            # delete subtree
            while sq.head is not None:
                p = queue.dequeue(sq)
                if p is None:
                    continue 
                
                # load v attributes
                v = SmartPointer.get_and_copy(p)
                assert v.type == "FAN_OUT"
                self.out_tree_accesses += 1
                self.out_tree_degree += 2

                # at internal FanOut
                if not v.is_leaf:
                    assert v.ogE is None and v.dst_id is None
                    for i in range(self.branching_factor):
                        if v.children[i] is not None:
                            queue.enqueue(sq, v.children[i])
                            v.children[i] = None
                        else:
                            break

                SmartPointer.put(p, v)
                SmartPointer.delete(v)
                SmartPointer.delete(p)
            
        # update ogV
        ogV.height = new_height
        SmartPointer.put(p_src, ogV)
        SmartPointer.delete(ogV)

    def downsize_edge_tree(self, p_src: SmartPointer, new_out_degree: int, make_copies: bool = True) -> None:
        """Downsize FanOut tree by one level"""
        # load ogV attrs
        req_attr = ["id", "type", "out_children", "out_degree", "height"]
        p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)
        out_children = v_attr["out_children"]
        height = v_attr["height"]

        # calculate number of levels to remove
        if new_out_degree == 0:
            start = 0
            new_height = 0
        else:
            start = 1
            new_height = height
            while new_out_degree / (self.branching_factor ** new_height) <= 1 / (self.branching_factor ** 2):
                new_height -= 1
        
        if new_height < height:
            # queue children to delete
            sq = queue.init_queue()
            for i in range(start, self.branching_factor):
                if out_children[i] is not None:
                    queue.enqueue(sq, out_children[i])
                    out_children[i] = None
                else:
                    break

            # transfer children of left subtree to immediate children of ogV
            if start:
                p = out_children[0]
                out_children[0] = None
                req_attr = ["children", "type", "is_leaf"]
                for i in range(height-new_height):
                    # get next level of children
                    v_attr = SmartPointer.get_attr(p, req_attr, make_copies)
                    assert v_attr["type"] == "FAN_OUT" and not v_attr["is_leaf"]
                    self.out_tree_accesses += 1
                    self.out_tree_degree += 1
                    children = v_attr["children"]

                    # queue right children
                    if i < height - new_height - 1:
                        for j in range(start, self.branching_factor):
                            if children[j] is not None:
                                queue.enqueue(sq, children[j])
                                children[j] = None
                            else:
                                break

                        # get leftmost child
                        p_child = children[0]
                        children[0] = None
                        SmartPointer.put_attr(p, {"children": children}, delete_old=make_copies)
                        SmartPointer.delete(p)
                        p = p_child

                    else:
                        # set leftmost children as new ogV children
                        for j in range(self.branching_factor):
                            out_children[j] = children[j]
                            children[j] = None

                        # delete leftmost child and its child pointers
                        SmartPointer.put_attr(p, {"children": children}, delete_old=make_copies)
                        SmartPointer.delete(p)

            # delete subtree
            req_attr = ["children", "type", "is_leaf", "ogE", "dst_id"]
            while sq.head is not None:
                p = queue.dequeue(sq)
                if p is None:
                    continue 
                
                # load v attributes
                v_attr = SmartPointer.get_attr(p, req_attr)
                assert v_attr["type"] == "FAN_OUT"
                self.out_tree_accesses += 1
                degree = 2 if SmartPointer.is_temp_copy(p) else 1
                self.out_tree_degree += degree
                children = v_attr["children"]

                # at internal FanOut
                if not v_attr["is_leaf"]:
                    assert v_attr["ogE"] is None and v_attr["dst_id"] is None
                    for i in range(self.branching_factor):
                        if children[i] is not None:
                            queue.enqueue(sq, children[i])
                            children[i] = None
                        else:
                            break

                set_attr = {"children": children, "ogE": None, "dst_id": None, "weight": 0}
                SmartPointer.put_attr(p, set_attr, delete_old=make_copies)
                SmartPointer.delete(p)

        # update ogV
        set_attr = {"out_children": out_children, "out_degree": new_out_degree, "height": new_height}
        SmartPointer.put_attr(p_src, set_attr, delete_old=make_copies)

    def get_random_neighbor_full_copy(self, p_src: SmartPointer | None) -> Tuple[int | None, SmartPointer | None]: 
        """Randomly pick and output a dst id and ogE from a given Vertex's FanOut tree"""
        if p_src is None:
            return None, None

        # return random dst_id if possible
        dst_id = ogE = None

        # load ogV and compute stats
        p_src, ogV = self.get_ogV_full_copy(p_src)
        out_degree = new_out_degree = ogV.out_degree

        # ensure src Vertex has edges
        if ogV.out_degree:
            # use a numbers list that corresponds to children leaves for traversal
            # perform a binary search-like slicing to choose traverse toward children 
            # slicing parameters 
            possible_leaves = [i for i in range(ogV.out_degree)]
            while possible_leaves and dst_id is None and ogE is None:
                target = random.choice(possible_leaves)
                possible_leaves.remove(target)
                all_leaves = [i for i in range(self.branching_factor**ogV.height)]
                step = len(all_leaves)//self.branching_factor if len(all_leaves) >= self.branching_factor else 1
                start = 0
                i = 0

                # partition list of child numbers to know which direct to traverse
                sq = queue.init_queue()
                while start < len(all_leaves):
                    if target in all_leaves[start:start+step]:
                        queue.enqueue(sq, ogV.out_children[i])
                        ogV.out_children[i] = None
                        all_leaves = all_leaves[start:start+step]
                        break
                    else:
                        start += step
                        i += 1
            
                # traverse to ogEs
                trials = 0
                while sq.head is not None:
                    p = queue.dequeue(sq)
                    if p is None:
                        continue

                    if dst_id is not None:
                        SmartPointer.delete_temp_copy(p)
                        continue
                    
                    # load v
                    v = SmartPointer.get_and_copy(p)
                    assert v.type == "FAN_OUT"
                    self.out_tree_accesses += 1
                    self.out_tree_degree += 2

                    if not v.is_leaf:
                        assert v.ogE is None
                        # slicing parameters
                        step = len(all_leaves)//self.branching_factor if len(all_leaves) >= self.branching_factor else 1
                        start = 0
                        i = 0

                        # partition list for further traversal
                        while start < len(all_leaves):
                            if target in all_leaves[start:start+step]:
                                queue.enqueue(sq, v.children[i])
                                v.children[i] = None
                                all_leaves = all_leaves[start:start+step]
                                break
                            else:
                                start += step
                                i += 1
                                
                    # check if ogE was deleted
                    if v.ogE is not None and self.num_deleted_objs > 0:
                        p, v, new_out_degree = self.process_deleted_objects_full_copy(p, v, new_out_degree)

                    # process ogEs
                    if v.ogE is not None:
                        # queue ogE
                        dst_id = v.dst_id
                        ogE = v.ogE
                        v.ogE = None
                        self.ogV_counts[dst_id] += 1
                            
                    SmartPointer.delete(v)
                    # do not delete first pointer since we might need to 
                    # traverse from the ogV out_children again
                    if trials == 0: 
                        SmartPointer.delete_temp_copy(p)

                    trials += 1

        SmartPointer.delete(ogV)

        # rearrange and resize FanOut tree
        if new_out_degree < out_degree:
            new_out_degree, found = self.reorder_edge_tree_full_copy(p_src, new_out_degree)
            self.downsize_edge_tree_full_copy(p_src, new_out_degree)

        # delete entry point if desired
        if SmartPointer.is_temp_copy(p_src): self.ogV_counts[ogV.id] -= 1
        SmartPointer.delete_temp_copy(p_src)
    
        return dst_id, ogE

    def get_random_neighbor(self, p_src: SmartPointer | None, make_copies: bool = True) -> Tuple[int | None, SmartPointer | None]: 
        """Randomly pick and output a dst id and ogE from a given Vertex's FanOut tree"""
        if p_src is None:
            return None, None

        # return random dst_id if possible
        dst_id = ogE = None
        
        # load ogV attributes
        req_attr = ["out_degree", "type", "id", "height"]
        p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies) 
        src_id = v_attr["id"]
        out_degree = new_out_degree = v_attr["out_degree"]
        height = v_attr["height"]

        # ensure src Vertex has edges
        if out_degree:
            # use a numbers list that corresponds to children leaves for traversal
            # perform a binary search-like slicing to choose traverse toward children 
            # slicing parameters 
            possible_leaves = [i for i in range(out_degree)]
            subtree_height = ceil(log(out_degree, self.branching_factor)) if out_degree > 1 else 1
            while possible_leaves and dst_id is None and ogE is None:
                target = random.choice(possible_leaves)
                possible_leaves.remove(target)
                i = 0

                # precompute path: sequence of child indices to follow at each tree level (most-significant first)
                path = []
                for i in range(height-subtree_height):
                    path.append(0)
                    
                rem = target
                for level in range(subtree_height):
                    exp = subtree_height - level - 1
                    if exp < 0:
                        exp = 0  
                    div = self.branching_factor ** exp
                    idx = rem // div
                    path.append(int(idx))
                    rem = rem % div

                p = p_src
                i = 0

                pre_alloc_count = get_global_counter()[0]
                for idx in path:
                    child_term = "out_children" if i == 0 else "children"
                    sl = AttributeAtPositions(child_term, [idx])
                    next_child = SmartPointer.get_attr(p, sl, make_copies)[child_term][0]

                    if i == 0:
                        self.ogV_depth.append(get_global_counter()[0] - pre_alloc_count)
                        self.ogV_degree += self.ogV_counts[src_id]
                        self.ogV_accesses += 1

                    else:
                        SmartPointer.delete_temp_copy(p)
                        self.out_tree_accesses += 1
                        degree = 2 if SmartPointer.is_temp_copy(p) else 1
                        self.out_tree_degree += degree

                    i += 1
                    p = next_child

                # get ogE
                req_attr = ["type", "is_leaf", "dst_id", "ogE"]
                v_attr = SmartPointer.get_attr(p, req_attr, make_copies)
                assert v_attr["is_leaf"] and v_attr["type"] == "FAN_OUT"
                dst_id = v_attr["dst_id"]
                ogE = v_attr["ogE"]

                # compute FanOut tree accesses
                self.out_tree_accesses += 1
                degree = 2 if SmartPointer.is_temp_copy(p) else 1
                self.out_tree_degree += degree

                # ensure item is not deleted
                if ogE is not None and self.num_deleted_objs > 0:
                    p, ogE, dst_id, new_out_degree = self.process_deleted_objects(p, ogE, dst_id, new_out_degree, make_copies)

                # compute FanOut tree accesses
                SmartPointer.delete_temp_copy(p)
                if isinstance(dst_id, int) and SmartPointer.is_temp_copy(ogE): self.ogV_counts[dst_id] += 1

        # rearrange and resize FanOut tree
        if new_out_degree < out_degree:
            new_out_degree, found = self.reorder_edge_tree(p_src, new_out_degree, None, make_copies)
            self.downsize_edge_tree(p_src, new_out_degree, make_copies)

        if SmartPointer.is_temp_copy(p_src): self.ogV_counts[src_id] -= 1
        SmartPointer.delete_temp_copy(p_src)

        return dst_id, ogE
  
    def bfs_full_copy(self, src_name: int | str) -> List[int]:
        """Explore Vertices in the graph with a queue structure"""
        src_id, p_src = self.get_pointer(src_name)
        path = [src_id]

        # initialize queue
        sq = queue.init_queue()

        # initialize visited queue
        visited = queue.init_queue()
        
        # mark ogV as visited
        p_src, ogV = self.get_ogV_full_copy(p_src)
        ogV, src_id = self.visit_full_copy(p_src, ogV, None)
        queue.enqueue(visited, p_src)
        
        # queue neighbors for visiting
        self.add_neighbors_full_copy(p_src, ogV, "QUEUE", sq, "ID", None)

        # process ogVs for visiting
        while sq.head is not None:
            # get label and pointer
            lp = queue.dequeue(sq)
            if lp is None:
                continue

            label, p = lp
            if p is None:
                continue
                
            # traverse to new node only if it has not been visited
            p, v = self.get_ogV_full_copy(p)
            if v is not None and not v.visited:
                v, src_id = self.visit_full_copy(p, v, label)
                queue.enqueue(visited, p)
                path.append(src_id)
                self.add_neighbors_full_copy(p, v, "QUEUE", sq, "ID", None)
            else:
                SmartPointer.delete(v)
                SmartPointer.delete_temp_copy(p)
                self.ogV_counts[v.id] -= 1

        self.unvisit_full_copy(visited)
        return path

    def bfs(self, src_name: int | str, make_copies: bool = True) -> List[int]:
        """Explore Vertices in the graph with a queue structure"""
        src_id, p_src = self.get_pointer(src_name)
        path = [src_id]
        
        # initialize queue
        sq = queue.init_queue()

        # initialize queue to collect visited ogVs
        visited = queue.init_queue()
        
        # mark ogV as visited
        p_src, src_id = self.visit(p_src, None)
        
        # queue neighbors for visiting
        self.add_neighbors(p_src, "QUEUE", sq, "ID", None, make_copies)
        queue.enqueue(visited, (p_src, src_id))

        # process ogVs for visiting
        while sq.head is not None:
            # get label and pointer
            lp = queue.dequeue(sq)
            if lp is None:
                continue

            label, p = lp
            if p is None:
                continue
                
            # traverse to new node only if it has not been visited
            req_attr = ["visited", "type", "id"]
            p, v_attr = self.get_ogV_attr(p, req_attr, make_copies)
            if p is not None and not v_attr["visited"]:
                p, src_id = self.visit(p, label)
                path.append(src_id)
                self.add_neighbors(p, "QUEUE", sq, "ID", None, make_copies)
                queue.enqueue(visited, (p, src_id))
            else:
                SmartPointer.delete_temp_copy(p)
                if SmartPointer.is_temp_copy(p): 
                    self.ogV_counts[v_attr["id"]] -= 1

        self.unvisit(visited, make_copies)
        return path

    def dfs_full_copy(self, src_name: int | str) -> List[int]:
        """Explore Vertices in the graph with a stack structure"""
        src_id, p_src = self.get_pointer(src_name)
        path = [src_id]

        # initialize stack
        top = stack.init_stack()

        # initialize visited queue
        visited = queue.init_queue()
        
        # mark ogV as visited
        p_src, ogV = self.get_ogV_full_copy(p_src)
        ogV, src_id = self.visit_full_copy(p_src, ogV, None)
        queue.enqueue(visited, p_src)

        # stack neighbors for visiting
        top = self.add_neighbors_full_copy(p_src, ogV, "STACK", top, "ID", None)

        # process FanOuts for visiting
        while top is not None:
            # get label and pointer
            lp, top = stack.pop(top)
            if lp is None:
                continue

            label, p = lp
            if p is None:
                continue

            # traverse to new node only if it has not been visited
            p, v = self.get_ogV_full_copy(p)
            if v is not None and not v.visited:
                v, src_id = self.visit_full_copy(p, v, label)
                queue.enqueue(visited, p)
                path.append(src_id)
                top = self.add_neighbors_full_copy(p, v, "STACK", top, "ID", None)
            else:
                SmartPointer.delete(v)
                self.ogV_counts[v.id] -= 1
                SmartPointer.delete_temp_copy(p)

        self.unvisit_full_copy(visited)
        return path

    def dfs(self, src_name: int | str, make_copies: bool = True) -> List[int]:
        """Explore Vertices in the graph with a stack structure"""
        src_id, p_src = self.get_pointer(src_name)
        path = [src_id]

        # initialize stack
        top = stack.init_stack()

        # initialize queue to collect visited ogVs
        visited = queue.init_queue()
        
        # mark ogV as visited
        p_src, src_id = self.visit(p_src, None)

        # stack neighbors for visiting
        top = self.add_neighbors(p_src, "STACK", top, "ID", None, make_copies)
        queue.enqueue(visited, (p_src, src_id))

        # process ogVs for visiting
        while top is not None:
            # get label and pointer
            lp, top = stack.pop(top)
            if lp is None:
                continue

            label, p = lp
            if p is None:
                continue

            # traverse to new node only if it has not been visited
            req_attr = ["visited", "type", "id"]
            p, v_attr = self.get_ogV_attr(p, req_attr, make_copies)
            if p is not None and not v_attr["visited"]:
                p, src_id = self.visit(p, label)
                path.append(src_id)
                top = self.add_neighbors(p, "STACK", top, "ID", None, make_copies)
                queue.enqueue(visited, (p, src_id))
            else:
                SmartPointer.delete_temp_copy(p)
                if SmartPointer.is_temp_copy(p): self.ogV_counts[v_attr["id"]] -= 1

        self.unvisit(visited, make_copies)
        return path

    def dijkstra_full_copy(self, src_name: int | str) -> Tuple[int, Dict[int, int], List[Tuple[int, int, int]]]:
        """Compute Single Source Shortest Paths"""
        src_id, p_src = self.get_pointer(src_name)
        costs = {src_id: 0}
        edges = []

        # initialize vistited queue
        visited = queue.init_queue()
        
        # mark ogV as visited
        p_src, ogV = self.get_ogV_full_copy(p_src)
        ogV, src_id = self.visit_full_copy(p_src, ogV, 0)
        queue.enqueue(visited, p_src)

        # heap neighbors for visiting
        self.add_neighbors_full_copy(p_src, ogV, "PQ", None, "LW", "LW")
        
        # process ogVs for visiting
        while not PQ.is_empty():
            # get weight, label, and pointer
            weight, lpo = PQ.pop()
    
            if lpo is None:
                continue

            label, p, old_src_id = lpo
            if p is None:
                continue
            
            # traverse to new node only if it has not been visited
            p, v = self.get_ogV_full_copy(p)
            if v is not None and not v.visited:
                v, src_id = self.visit_full_copy(p, v, label)
                queue.enqueue(visited, p)
                costs[src_id] = weight
                edges.append((old_src_id, src_id, weight))
                self.add_neighbors_full_copy(p, v, "PQ", None, "LW", "LW")
            else:
                SmartPointer.delete(v)
                SmartPointer.delete_temp_copy(p)
                self.ogV_counts[v.id] -= 1

        total = 0
        for cost in costs.values():
            total += cost

        self.unvisit_full_copy(visited)
        return total, costs, edges

    def dijkstra(self, src_name: int | str, make_copies: bool = True) -> Tuple[int, Dict[int, int], List[Tuple[int, int, int]]]:
        """Compute Single Source Shortest Paths"""
        src_id, p_src = self.get_pointer(src_name)
        costs = {src_id: 0}
        edges = []

        # initialize vistited queue
        visited = queue.init_queue()
        
        # mark ogVs as visited
        p_src, src_id = self.visit(p_src, 0)

        # heap neighbors for visiting
        self.add_neighbors(p_src, "PQ", None, "LW", "LW", make_copies)
        queue.enqueue(visited, (p_src, src_id))
        
        # process ogVs for visiting
        while not PQ.is_empty():
            # get weight, label, and pointer
            weight, lpo = PQ.pop()
    
            if lpo is None:
                continue

            label, p, old_src_id = lpo
            if p is None:
                continue
                
            # traverse to new node only if it has not been visited
            req_attr = ["visited", "type", "id"]
            p, v_attr = self.get_ogV_attr(p, req_attr, make_copies)
            if p is not None and not v_attr["visited"]:
                p, src_id = self.visit(p, label)
                queue.enqueue(visited, (p, src_id))
                costs[src_id] = weight
                edges.append((old_src_id, src_id, weight))
                self.add_neighbors(p, "PQ", None, "LW", "LW", make_copies)
            else:
                SmartPointer.delete_temp_copy(p)
                if SmartPointer.is_temp_copy(p): self.ogV_counts[v_attr["id"]] -= 1

        total = 0
        for cost in costs.values():
            total += cost
        
        self.unvisit(visited, make_copies)
        return total, costs, edges

    def prim_full_copy(self, src_name: int | str) -> Tuple[int, List[Tuple[int, int, int]]]:
        """Output Minimum Spanning Tree"""
        src_id, p_src = self.get_pointer(src_name)
        cost = 0
        edges = []

        # initialize vistited queue
        visited = queue.init_queue()
        
        # mark ogVs as visited
        p_src, ogV = self.get_ogV_full_copy(p_src)
        ogV, src_id = self.visit_full_copy(p_src, ogV, None)
        queue.enqueue(visited, p_src)

        # heap neighbors for visiting
        self.add_neighbors_full_copy(p_src, ogV, "PQ", None, "ID", "W")
        
        # process ogVs for visiting
        while not PQ.is_empty():
            # get weight, label, and pointer
            weight, lpo = PQ.pop()
    
            if lpo is None:
                continue

            label, p, old_src_id = lpo
            if p is None:
                continue

            # traverse to new node only if it has not been visited
            p, v = self.get_ogV_full_copy(p)
            if v is not None and not v.visited:
                v, src_id = self.visit_full_copy(p, v, label)
                queue.enqueue(visited, p)
                cost += weight
                edges.append((old_src_id, src_id, weight))
                self.add_neighbors_full_copy(p, v, "PQ", None, "ID", "W")
            else:
                SmartPointer.delete(v)
                SmartPointer.delete_temp_copy(p)
                self.ogV_counts[v.id] -= 1

        self.unvisit_full_copy(visited)
        return cost, edges

    def prim(self, src_name: int | str, make_copies: bool = True) -> Tuple[int, List[Tuple[int, int, int]]]:
        """Output Minimum Spanning Tree"""
        src_id, p_src = self.get_pointer(src_name)
        cost = 0
        edges = []

        # initialize vistited queue
        visited = queue.init_queue()
        
        # mark ogVs as visited
        p_src, src_id = self.visit(p_src, None)

        # heap neighbors for visiting
        self.add_neighbors(p_src, "PQ", None, "ID", "W", make_copies)
        queue.enqueue(visited, (p_src, src_id))
        
        # process ogVs for visiting
        while not PQ.is_empty():
            # get weight, label, and pointer
            weight, lpo = PQ.pop()
    
            if lpo is None:
                continue

            label, p, old_src_id = lpo
            if p is None:
                continue

            # traverse to new node only if it has not been visited
            req_attr = ["visited", "type", "id"]
            p, v_attr = self.get_ogV_attr(p, req_attr, make_copies)
            if p is not None and not v_attr["visited"]:
                p, src_id = self.visit(p, label)
                queue.enqueue(visited, (p, src_id))
                cost += weight
                edges.append((old_src_id, src_id, weight))
                self.add_neighbors(p, "PQ", None, "ID", "W", make_copies)
            else:
                SmartPointer.delete_temp_copy(p)
                if SmartPointer.is_temp_copy(p): self.ogV_counts[v_attr["id"]] -= 1
            
        self.unvisit(visited, make_copies)
        return cost, edges

    def random_walk_full_copy(self, src_name: int | str, walk_length: int) -> List[int]:
        """Perform a walk of specified length or until reaching a stopping point"""
        src_id : int | None
        p_src : SmartPointer | None
        src_id, p_src = self.get_pointer(src_name)
        path = [src_id]
        
        # iterate for walk_length trials
        for i in range(walk_length):
            src_id, p_src = self.get_random_neighbor_full_copy(p_src)

            # terminate if there are no more steps to walk
            if src_id is None:
                print(f"Could not find new nodes to traverse. Walk terminated early at step {i}.")
                break

            path.append(src_id)

        # delete ending pointer
        if SmartPointer.is_temp_copy(p_src): self.ogV_counts[path[-1]] -= 1
        SmartPointer.delete_temp_copy(p_src)
                
        return path

    def random_walk(self, src_name: int | str, walk_length: int, make_copies: bool = True) -> List[int]:
        """Perform a walk of specified length or until reaching a stopping point"""
        src_id : int | None
        p_src : SmartPointer | None
        src_id, p_src = self.get_pointer(src_name)
        path = [src_id]
        
        # iterate for walk_length trials
        for i in range(walk_length):
            src_id, p_src = self.get_random_neighbor(p_src, make_copies)

            # terminate if there are no more steps to walk
            if src_id is None:
                print(f"Could not find new nodes to traverse. Walk terminated early at step {i}.")
                break

            path.append(src_id)

        # delete ending pointer
        if SmartPointer.is_temp_copy(p_src): self.ogV_counts[path[-1]] -= 1
        SmartPointer.delete_temp_copy(p_src)
                
        return path

    def contact_discovery_full_copy(self, src_name1: int | str, src_name2: int | str) -> Set[int]:
        """Outputs intersection of neighbors shared by src 1 and src 2"""
        src_id1, p_src1 = self.get_pointer(src_name1)
        p_src1, ogV1 = self.get_ogV_full_copy(p_src1)

        src_id2, p_src2 = self.get_pointer(src_name2)
        p_src2, ogV2 = self.get_ogV_full_copy(p_src2)

        # get neighbors of each
        src_id1_out : Set[int] = set()
        self.add_neighbors_full_copy(p_src1, ogV1, "SET", src_id1_out, None, None)
        src_id2_out : Set[int] = set()
        self.add_neighbors_full_copy(p_src2, ogV2, "SET", src_id2_out, None, None)

        # compute intersection
        shared = src_id1_out.intersection(src_id2_out)
        return shared

    def contact_discovery(self, src_name1: int | str, src_name2: int | str, make_copies : bool = True) -> Set[int]:
        """Outputs intersection of neighbors shared by src 1 and src 2"""
        src_id1, p_src1 = self.get_pointer(src_name1)
        src_id2, p_src2 = self.get_pointer(src_name2)

        # get neighbors of each
        src_id1_out : Set[int] = set()
        self.add_neighbors(p_src1, "SET", src_id1_out, None, None, make_copies)
        src_id2_out : Set[int] = set()
        self.add_neighbors(p_src2, "SET", src_id2_out, None, None, make_copies)

        # compute intersection
        shared = src_id1_out.intersection(src_id2_out)
        return shared

    def directed_triangle_count_full_copy(self, src_name : int | str) -> Tuple[int, List[Tuple[int, int, int]]]:
        """
        Output the number of directed triangles a src is associated with
        A triangle is defined as a cycle of three vertices (u, v, w)
        with the edges u -> v, v -> w, and w -> u
        """
        src_id, p_src = self.get_pointer(src_name)
        triangles = []

        # retrieve FanOuts entry points
        p_src, ogV = self.get_ogV_full_copy(p_src)
        out1 = queue.init_queue()
        self.add_neighbors_full_copy(p_src, ogV, "QUEUE", out1, "DST_ID", None)

        # process each FanOut pointer to an ogV
        while out1.head is not None:
            # get out_id and p
            op1 = queue.dequeue(out1)
            if op1 is None:
                continue

            out_id1, p1 = op1
            if p1 is None:
                continue

            # retrieve FanOuts entry points
            p1, ogV1 = self.get_ogV_full_copy(p1)
            out2 = queue.init_queue()
            self.add_neighbors_full_copy(p1, ogV1, "QUEUE", out2, "DST_ID", None)
            self.ogV_counts[out_id1] -= 1
            SmartPointer.delete_temp_copy(p1)

            # process each FanOut pointer to an ogV
            while out2.head is not None:
                # get out_id and p
                op2 = queue.dequeue(out2)
                if op2 is None:
                    continue

                out_id2, p2 = op2
                if p2 is None:
                    continue

                # retrieve FanOut ids
                p2, ogV2 = self.get_ogV_full_copy(p2)
                out3 : Set[int] = set()
                self.add_neighbors_full_copy(p2, ogV2, "SET", out3, None, None)
                self.ogV_counts[out_id2] -= 1
                SmartPointer.delete_temp_copy(p2)

                # triangle is complete if src_id is found
                if src_id in out3:
                    triangles.append((src_id, out_id1, out_id2))

        count = len(triangles)
        return count, triangles

    def directed_triangle_count(self, src_name : int | str, make_copies : bool = True) -> Tuple[int, List[Tuple[int, int, int]]]:
        """
        Output the number of directed triangles a src is associated with
        A triangle is defined as a cycle of three vertices (u, v, w)
        with the edges u -> v, v -> w, and w -> u
        """
        src_id, p_src = self.get_pointer(src_name)
        triangles = []

        # retrieve FanOut entry points
        out1 = queue.init_queue()
        self.add_neighbors(p_src, "QUEUE", out1, "DST_ID", None, make_copies)
        
        # process each FanOut pointer to an ogV
        while out1.head is not None:
            # get out_id and p
            op1 = queue.dequeue(out1)
            if op1 is None:
                continue

            out_id1, p1 = op1
            if p1 is None:
                continue

            # retrieve FanOuts entry points
            out2 = queue.init_queue()
            self.add_neighbors(p1, "QUEUE", out2, "DST_ID", None, make_copies)
            if SmartPointer.is_temp_copy(p1): self.ogV_counts[out_id1] -= 1
            SmartPointer.delete_temp_copy(p1)

            # process each FanOut pointer to an ogV
            while out2.head is not None:
                # get out_id and p
                op2 = queue.dequeue(out2)
                if op2 is None:
                    continue

                out_id2, p2 = op2
                if p2 is None:
                    continue

                # retrieve FanOut ids
                out3 : Set[int] = set()
                self.add_neighbors(p2, "SET", out3, None, None, make_copies)
                if SmartPointer.is_temp_copy(p2): self.ogV_counts[out_id2] -= 1
                SmartPointer.delete_temp_copy(p2)
                
                # triangle is complete if src_id is found
                if src_id in out3:
                    triangles.append((src_id, out_id1, out_id2))

        count = len(triangles)
        return count, triangles

    def pagerank_full_copy(self, walk_length: int, damping_factor: float) -> Tuple[Dict[int, int], Dict[int, float]]:
        """
        Measures popularity of Vertices by doing smaller random walks of 
        some total length through the graph and sporadically picking a new 
        starting Vertex with a probability of 1-df, or if a stopping point 
        is reached
        """
        visits: Dict[int, int] = dict()
        p_src = src_id = None

        for i in range(walk_length):
            probability = random.random()

            # go to random neighbor of current node
            if probability <= damping_factor:
                src_id, p_src = self.get_random_neighbor_full_copy(p_src)

            # get new starting node if damping_factor permits or
            # get_random_neighbor could not traverse further
            if (src_id is None and p_src is None) or probability > damping_factor:
                if isinstance(src_id, int) and SmartPointer.is_temp_copy(p_src): self.ogV_counts[src_id] -= 1
                SmartPointer.delete_temp_copy(p_src)
                p_src = src_id = None

                # loop until a valid pointer is found
                # with deletions, a number may be gone from the graph
                while p_src is None:
                    src_id = random.randint(0, self.nodes-1)
                    p_src = self.entry_points[src_id]

            assert isinstance(src_id, int)
            try:
                visits[src_id] += 1
            except:
                visits[src_id] = 1

        
        if isinstance(src_id, int) and SmartPointer.is_temp_copy(p_src): self.ogV_counts[src_id] -= 1
        SmartPointer.delete_temp_copy(p_src)

        ratios = dict()

        for v_id, count in visits.items():
            ratios[v_id] = count / walk_length

        # assert we traversed the entire walk_length
        count = 0
        for visit in visits.values():
            count += visit
        assert count == walk_length
                
        return visits, ratios

    def pagerank(self, walk_length: int, damping_factor: float, make_copies : bool = True) -> Tuple[Dict[int, int], Dict[int, float]]:
        """
        Measures popularity of Vertices by doing smaller random walks of 
        some total length through the graph and sporadically picking a new 
        starting Vertex with a probability of 1-df, or if a stopping point 
        is reached
        """
        visits: Dict[int, int] = dict()
        p_src = src_id = None

        for i in range(walk_length):
            probability = random.random()

            # go to random neighbor of current node
            if probability <= damping_factor:
                src_id, p_src = self.get_random_neighbor(p_src, make_copies)

            # get new starting node if damping_factor permits or
            # get_random_neighbor could not traverse further
            if (src_id is None and p_src is None) or probability > damping_factor:
                if isinstance(src_id, int) and SmartPointer.is_temp_copy(p_src): self.ogV_counts[src_id] -= 1
                SmartPointer.delete_temp_copy(p_src)
                p_src = src_id = None

                # loop until a valid pointer is found
                # with deletions, a number may be gone from the graph
                while p_src is None:
                    src_id = random.randint(0, self.nodes-1)
                    p_src = self.entry_points[src_id]

            assert isinstance(src_id, int)   
            try:
                visits[src_id] += 1
            except:
                visits[src_id] = 1

        # delete ending pointer
        if isinstance(src_id, int) and SmartPointer.is_temp_copy(p_src): self.ogV_counts[src_id] -= 1
        SmartPointer.delete_temp_copy(p_src)

        ratios = dict()

        for v_id, count in visits.items():
            ratios[v_id] = count / walk_length

        # assert we traversed the entire walk_length
        count = 0
        for visit in visits.values():
            count += visit
        assert count == walk_length
                
        return visits, ratios

def build_from_edges(   
        edges: List[Tuple[int | str, int | str, int]] | List[Tuple[int | str, int | str]], 
        name: str, 
        branching_factor: int = 2, 
        static_insertion: bool = True, 
        move_semantics: bool = True, 
        make_copies: bool = True, 
        seed: int | None = None,
        queue_osam: bool = True, 
        stack_osam: bool = True, 
        avl_tree_osam: bool = True, 
        sort_ids: bool = True, 
        ordered_dynamic_vertices: bool = False
    ) -> OGraph:
    """Build Oblvious Graph from a list of edge tuples (with optional weights)"""
    # number of writes made during a graph phase
    reset_write_batches() 

    # control randomness
    if isinstance(seed, int):
        random.seed(seed)
    
    print(f"\nGraph from Edges: {name}", flush=True)

    # initialize graph with branching factor and osam usages
    ograph = OGraph(branching_factor=branching_factor, avl_tree_osam=avl_tree_osam)
    queue.queue_osam = queue_osam
    stack.stack_osam = stack_osam

    print("\nAllocs, Reads, Writes", flush=True)
    print(f"Pre build stats {get_global_counter()}", flush=True) 

    if static_insertion: 
        # build graph knowing the entire graph structure
        ograph.emulate_graph(edges=edges, move_semantics=move_semantics, sort_ids=sort_ids)
    else:
        # optionally sort vertex names so that vertex IDs are assigned in order
        if ordered_dynamic_vertices:
            nodes = set()
            for edge in edges:
                nodes.add(edge[0])
                nodes.add(edge[1])

            for src_name in sorted(list(nodes)):
                ograph.add_vertex(src_name, ograph.v_ids, ograph.entry_points)
        
        # dynamically insert edges 
        alert_exists = True
        for edge in edges:
            weight = 0
            src_name = edge[0]
            dst_name = edge[1]
            if len(edge) == 3:
                weight = edge[2]

            if move_semantics:
                ograph.add_edge(src_name, dst_name, weight, alert_exists, make_copies)
            else:
                ograph.add_edge_full_copy(src_name, dst_name, weight, alert_exists)

    print(f"Post build stats {get_global_counter()}", flush=True)
    print("Post build structure stats:", flush=True)
    print_stats_by_structure(flush=True)
    print_max_write_batches(flush=True)

    return ograph

def build_from_networkx(    
        function: Callable | None,
        name: str, 
        graph: Graph | DiGraph | None = None, 
        args: Any = None, 
        branching_factor: int = 2,
        static_insertion: bool = True, 
        move_semantics: bool = True, 
        make_copies: bool = True, 
        prime_graph: bool = False, 
        seed: int | None = None, 
        connect_all: bool = True, 
        queue_osam: bool = True, 
        stack_osam: bool = True, 
        avl_tree_osam: bool = True, 
        random_weight_min: int | None = None, 
        random_weight_max: int | None = None, 
        sort_ids: bool = False, 
        ordered_dynamic_vertices: bool = False, 
    ) -> OGraph:
    """Build Oblvious Graph from networkx graph functions"""
    # number of writes made during a graph phase
    reset_write_batches() 

    # control randomness
    if isinstance(seed, int):
        random.seed(seed)
    
    print(f"\nGraph from NetworkX: {name}", flush=True)
        
    # graph can be passed as a DiGraph or Graph object from NetworkX
    if not (isinstance(graph, Graph) or isinstance(graph, DiGraph)):
        # load graph from NetworkX function
        assert callable(function)
        if args:
            graph = function(*args)
        else:
            graph = function()

    # randomly connect all disconnected components of a graph
    # only works on Graph objects (undirected)
    if connect_all and isinstance(graph, Graph) and not isinstance(graph, DiGraph):
        components = list(connected_components(graph))
        for i in range(len(components)-1):
            u = random.choice(list(components[i]))
            v = random.choice(list(components[i+1]))
            graph.add_edge(u, v)
    
    # assign random weights
    if isinstance(random_weight_min, int) and isinstance(random_weight_max, int):
        for u, v in graph.edges:
            graph.add_edge(u, v, weight=random.randint(random_weight_min, random_weight_max))

    # initialize graph with branching factor and osam usages
    ograph = OGraph(branching_factor=branching_factor, avl_tree_osam=avl_tree_osam)
    queue.queue_osam = queue_osam
    stack.stack_osam = stack_osam

    print("\nAllocs, Reads, Writes", flush=True)
    print(f"Pre build stats {get_global_counter()}", flush=True) 
    
    if static_insertion:
        # build graph knowing the entire graph structure
        ograph.emulate_graph(graph=graph, move_semantics=move_semantics, sort_ids=sort_ids) 
    else:
        # optionally sort vertex names so that vertex IDs are assigned in order
        if ordered_dynamic_vertices:
            for src_name in graph.nodes:
                ograph.add_vertex(src_name, ograph.v_ids, ograph.entry_points)

        # insert edges dynamically
        alert_exists = True
        for src_name in graph.nodes:
            for dst_name in graph.neighbors(src_name):
                try:
                    weight = graph.edges[src_name, dst_name]["weight"]
                except:
                    weight = 0

                if move_semantics:
                    ograph.add_edge(src_name, dst_name, weight, alert_exists, make_copies)
                else:
                    ograph.add_edge_full_copy(src_name, dst_name, weight, alert_exists)

    # in the multiwrite setting, prime the graph with random walks so 
    # that the underlying splay trees are rebalanced
    if prime_graph:
        for count in range(len(graph.nodes)//10):
            src = random.choice(list(graph.nodes))
            if move_semantics:
                ograph.random_walk(src, 50, make_copies)
            else: 
                ograph.random_walk_full_copy(src, 50)

    print(f"Post build stats {get_global_counter()}", flush=True)
    print("Post build structure stats:", flush=True)
    print_stats_by_structure(flush=True)
    print_max_write_batches(flush=True)

    return ograph
        
def benchmark(  
        ograph: OGraph, 
        name: str, 
        trials: int = 1, 
        algorithm: str = "rw",
        entry_point: int | str | None = None, 
        next_entry_point: int | str | None = None, 
        pick_random_entry_points: bool = False,
        move_semantics: bool = True, 
        make_copies: bool = True, 
        walk_length: int = 0,
        damping_factor: float = 0.9, 
        queue_osam: bool = True, 
        stack_osam: bool = True, 
        avl_tree_osam: bool = True,
        vertex_names: List[int | str] | None = None,
        print_trace: bool = False, 
        print_each_trial: bool = False
    ) -> List[Any]:
    """Benchmark Oblivious Graph for a specific algorithm"""
    # number of writes made during a graph phase
    reset_write_batches()
    
    assert trials > 0
    
    # enable/disable OSAM usage
    queue.queue_osam = queue_osam
    stack.stack_osam = stack_osam
    ograph.v_ids.avl_tree_osam = avl_tree_osam
    ograph.entry_points.avl_tree_osam = avl_tree_osam

    ograph.reset_access_stats()
    
    print(f"Benchmarking Graph {name}", flush=True) 

    # print(f"Pre algorithm {algorithm} stats {get_global_counter()}", flush=True) 
    # print_stats_by_structure(flush=True)

    # match algorithm to name
    alg = algorithm.lower()
    match alg:
        case "dfs" | "bfs" | "dijkstra" | "prim":
            tagline = f"Running {alg} for {trials} trial(s)"
        case "random walk" | "randomwalk" | "rw" | "random_walk" | "random-walk":
            alg = "random walk"
            tagline = f"Running {alg} for {trials} trial(s) with walk length {walk_length}"
        case "contact discovery" | "contactdiscovery" | "cd" | "contact_discovery" | "contact-discovery":
            alg = "contact discovery"
            tagline = f"Running {alg} for {trials} trial(s)"
        case "directed triangle count" | "directedtrianglecount" | "dtc" | "directed_triangle_count" | "directed-triangle-count":
            alg = "directed triangle count"
            tagline = f"Running {alg} for {trials} trial(s)"
        case "page rank" | "pagerank" | "pr" | "page_rank" | "page-rank":
            alg = "pagerank"
            tagline = f"Running {alg} for {trials} trial(s) with walk length {walk_length} and damping factor {damping_factor}"
        case _:
            raise RuntimeError(f"OGraph has no algorithm {algorithm}")
    print(tagline)

    # collect traces of each run
    traces = []
    for trial in range(trials):
        if print_each_trial: print(f"\nTrial {trial}")

        if pick_random_entry_points: 
            if vertex_names is None:
                entry_point = random.randint(0, ograph.nodes-1)
                next_entry_point = entry_point

                # pick distinct entry points
                while ograph.nodes > 1 and entry_point == next_entry_point:
                    next_entry_point = random.randint(0, ograph.nodes-1)
            
            elif isinstance(vertex_names, list):
                entry_point = random.choice(vertex_names)
                next_entry_point = entry_point

                # pick distinct entry points
                while ograph.nodes > 1 and entry_point == next_entry_point:
                    next_entry_point = random.choice(vertex_names)

        # call function to benchmark
        trace : Any
        assert entry_point is not None
        match alg:
            case "bfs":
                if print_each_trial: print(f"bfs on entry point {entry_point}", flush=True)
                trace = ograph.bfs_full_copy(entry_point) if not move_semantics else ograph.bfs(entry_point, make_copies)
            case "dfs":
                if print_each_trial: print(f"dfs on entry point {entry_point}", flush=True)
                trace = ograph.dfs_full_copy(entry_point) if not move_semantics else ograph.dfs(entry_point, make_copies)
            case "dijkstra":
                if print_each_trial: print(f"Dijkstra on entry point {entry_point}", flush=True)
                trace = ograph.dijkstra_full_copy(entry_point) if not move_semantics else ograph.dijkstra(entry_point, make_copies)
            case "prim":
                if print_each_trial: print(f"Prim on entry point {entry_point}")
                trace = ograph.prim_full_copy(entry_point) if not move_semantics else ograph.prim(entry_point, make_copies)
            case "random walk":
                if print_each_trial: print(f"Random Walk on entry point {entry_point} with walk length {walk_length}", flush=True)
                trace = ograph.random_walk_full_copy(entry_point, walk_length) if not move_semantics else ograph.random_walk(entry_point, walk_length, make_copies)
            case "contact discovery":
                assert next_entry_point is not None
                if print_each_trial: print(f"Contact Discovery on entry points ({entry_point}, {next_entry_point})", flush=True)
                trace = ograph.contact_discovery_full_copy(entry_point, next_entry_point) if not move_semantics else ograph.contact_discovery(entry_point, next_entry_point, make_copies)
            case "directed triangle count":
                if print_each_trial: print(f"Directed Triangle Count on entry point {entry_point}", flush=True)
                trace = ograph.directed_triangle_count_full_copy(entry_point) if not move_semantics else ograph.directed_triangle_count(entry_point, make_copies)
            case "pagerank":
                if print_each_trial: print(f"PageRank with damping_factor {damping_factor} and walk length {walk_length}", flush=True)
                trace = ograph.pagerank_full_copy(walk_length, damping_factor) if not move_semantics else ograph.pagerank(walk_length, damping_factor, make_copies)

        # print algorithm traces
        if print_trace: 
            print(trace, flush=True)

        traces.append(trace)

        # print stats for each trial run
        if print_each_trial:
            print(f"Post trial {trial} {alg} stats {get_global_counter()}", flush=True)
            print_stats_by_structure(flush=True)

    ograph.print_access_stats()
    print(f"\nPost algorithm {alg} stats {get_global_counter()}", flush=True)
    print(f"Post {alg} structure stats:", flush=True)
    print_stats_by_structure(flush=True)
    print_max_write_batches(flush=True)

    return traces
