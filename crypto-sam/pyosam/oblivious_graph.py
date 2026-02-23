from smart_pointer import SmartPointer
from sublist import sublist
from single_access_machine import get_global_counter, print_high_level_operations, print_max_write_batches, reset_write_batches, print_timing_stats
from smart_queue import SmartQueue as queue
from smart_stack import SmartStack as stack
from smart_avl_tree import SmartAVLTree
from math import log, ceil, sqrt
from sys import maxsize
from collections import deque
from networkx import karate_club_graph, erdos_renyi_graph
import networkx as nx
import random
import copy
import heapq
import sys
import struct

def get_raw_addr(sp):
    """Extracts the integer address from a SmartPointer object."""
    if sp is None: return None
    # Support both MultiWrite (head) and Original (id) pointer types
    if hasattr(sp.p, 'head'): return sp.p.head
    if hasattr(sp.p, 'id'): return sp.p.id
    return None

def get_addr(sp):
    """Extract integer address, handling None as -1."""
    if sp is None: return -1
    # Support both MultiWrite (head) and Original (id) pointer types
    if hasattr(sp.p, 'head'): return sp.p.head if sp.p.head is not None else -1
    if hasattr(sp.p, 'id'): return sp.p.id if sp.p.id is not None else -1
    return -1

def make_sp(addr):
    """Reconstruct SmartPointer from integer address."""
    return SmartPointer(addr) if addr != -1 else None

# used for dijkstra / prim in place of actual queue
class PlaintextQueue:
    def __init__(self):
        self.storage = dict()
        self.keys = []
        self.length = 0

    def is_empty(self):
        return self.length == 0

    def insert(self, key, value):
        heapq.heappush(self.keys, key)
        if key not in self.storage:
            self.storage[key] = deque([value])
        else:
            self.storage[key].append(value)
        self.length += 1

    def pop(self):
        key = heapq.heappop(self.keys)
        value = self.storage[key].popleft()
        if not self.storage[key]:
            del self.storage[key]
        self.length -= 1
        return key, value

    def peek(self):
        key = heapq.heappop(self.keys)
        heapq.heappush(self.keys, key)
        return key

    def __len__(self):
        return self.length

PQ = PlaintextQueue()







# classes for original oblivious graphs
class Vertex():
    __slots__ = ('id', 'type', 'out_degree', 'out_children', 'height', 'label', 'visited')

    def __init__(self, id=None, type=None, out_degree=0, out_children=None, height=0, label=None, visited=False):
        self.id = id
        self.type = 'VERTEX'
        self.out_degree = out_degree
        self.out_children = out_children if out_children is not None else []
        self.height = height
        self.label = label
        self.visited = visited

    def __getstate__(self):
        
        lbl = self.label if isinstance(self.label, int) else -1
        
        flat_children = [get_addr(child) for child in self.out_children]
        header = struct.pack('<iiii?B', 
                             self.id if self.id is not None else -1, 
                             self.out_degree, 
                             self.height, 
                             lbl, 
                             self.visited, 
                             len(flat_children))
        
        children_bytes = struct.pack(f'<{len(flat_children)}i', *flat_children)
        return header + children_bytes

    def __setstate__(self, state):
        self.type = 'VERTEX'
        
        header_size = struct.calcsize('<iiii?B')
        (self.id, self.out_degree, self.height, lbl, self.visited, num_children) = struct.unpack('<iiii?B', state[:header_size])
        
        if self.id == -1: self.id = None
        self.label = lbl if lbl != -1 else None
        
        children_ints = struct.unpack(f'<{num_children}i', state[header_size:])
        self.out_children = [make_sp(addr) for addr in children_ints]

    def smart_delete(self):
        SmartPointer.delete(self.out_children)

    @staticmethod
    def smart_copy(vertex, temp_copy):
        copy_v = Vertex(id=vertex.id, type=vertex.type, out_degree=vertex.out_degree, label=copy.deepcopy(vertex.label), height=vertex.height, visited=vertex.visited)
        copy_v.out_children = SmartPointer.copy(vertex.out_children, temp_copy)
        return copy_v

    def __repr__(self):
        return f"Vertex(id={self.id}, type={self.type}, out_degree={self.out_degree}, out_children={self.out_children}, height={self.height}, label={self.label}, visited={self.visited})"

    def __eq__(self, other):
        return type(self) is type(other) and self.id == other.id and self.out_degree == other.out_degree and self.height == other.height and all(self.out_children[i] == other.out_children[i] for i in range(len(self.out_children)))

    def smart_delete(self):
        SmartPointer.delete(self.out_children)

    @staticmethod
    def smart_copy(vertex, temp_copy):
        copy_v = Vertex(id=vertex.id, type=vertex.type, out_degree=vertex.out_degree, label=copy.deepcopy(vertex.label), height=vertex.height, visited=vertex.visited)
        copy_v.out_children = SmartPointer.copy(vertex.out_children, temp_copy)
        return copy_v

class OutEdge():
    __slots__ = ('type', 'children', 'label', 'weight', 'is_leaf', 'dst_id', 'ogE')

    def __init__(self, type=None, children=None, label=None, weight=0, is_leaf=False, dst_id=None, ogE=None):
        self.type = 'OUT_EDGE'
        self.children = children if children is not None else []
        self.label = label
        self.weight = weight
        self.is_leaf = is_leaf
        self.dst_id = dst_id
        self.ogE = ogE

    def __getstate__(self):        
        lbl = self.label if isinstance(self.label, int) else -1
        d_id = self.dst_id if self.dst_id is not None else -1
        ogE_addr = get_addr(self.ogE)
        
        flat_children = [get_addr(child) for child in self.children]
        
        header = struct.pack('<iiii?B', 
                             self.weight, 
                             d_id, 
                             ogE_addr, 
                             lbl, 
                             self.is_leaf, 
                             len(flat_children))
        
        children_bytes = struct.pack(f'<{len(flat_children)}i', *flat_children)
        return header + children_bytes

    def __setstate__(self, state):
        self.type = 'OUT_EDGE'
        
        header_size = struct.calcsize('<iiii?B')
        (self.weight, d_id, ogE_addr, lbl, self.is_leaf, num_children) = struct.unpack('<iiii?B', state[:header_size])
        
        self.dst_id = d_id if d_id != -1 else None
        self.ogE = make_sp(ogE_addr)
        self.label = lbl if lbl != -1 else None
        
        children_ints = struct.unpack(f'<{num_children}i', state[header_size:])
        self.children = [make_sp(addr) for addr in children_ints]

    def smart_delete(self):
        SmartPointer.delete(self.children)
        SmartPointer.delete(self.ogE)

    @staticmethod
    def smart_copy(out_edge, temp_copy=False):
        copy_v = OutEdge(type=out_edge.type, label=copy.deepcopy(out_edge.label), weight=out_edge.weight, is_leaf=out_edge.is_leaf, dst_id=out_edge.dst_id)
        copy_v.children = SmartPointer.copy(out_edge.children, temp_copy)
        copy_v.ogE = SmartPointer.copy(out_edge.ogE, temp_copy)
        return copy_v

    def __repr__(self):
        return f"OutEdge(type={self.type}, children={self.children}, label={self.label}, weight={self.weight}, is_leaf={self.is_leaf}, dst_id={self.dst_id}, ogE={self.ogE})"

    def smart_delete(self):
        SmartPointer.delete(self.children)
        SmartPointer.delete(self.ogE)

    @staticmethod
    def smart_copy(out_edge, temp_copy=False):
        copy_v = OutEdge(type=out_edge.type, label=copy.deepcopy(out_edge.label), weight=out_edge.weight, is_leaf=out_edge.is_leaf, dst_id=out_edge.dst_id)
        copy_v.children = SmartPointer.copy(out_edge.children, temp_copy)
        copy_v.ogE = SmartPointer.copy(out_edge.ogE, temp_copy)
        return copy_v

class DeletedObject():

    __slots__ = ('id', 'type')

    def __init__(self, id=None, type=None):
        self.id = id
        self.type = "DELETED_OBJECT"

    def __getstate__(self):
        return struct.pack('<i', self.id if self.id is not None else -1)
    def __setstate__(self, state):
        (self.id,) = struct.unpack('<i', state)
        if self.id == -1: self.id = None
        self.type = "DELETED_OBJECT"

    def smart_delete(self): pass

    @staticmethod
    def smart_copy(deleted_object, temp_copy=False):
        return DeletedObject(id=deleted_object.id, type=deleted_object.type)

    def __repr__(self):
        return f"DeletedObject(id={self.id}, type={self.type})"

    def smart_delete(self):
        pass

    @staticmethod
    def smart_copy(deleted_object, temp_copy=False):
        copy_v = DeletedObject(id=deleted_object.id, type=deleted_object.type)
        return copy_v





class OGraph():
    def __init__(self, branching_factor=2, avl_tree_osam=True):
        assert branching_factor > 1
        self.n = 0
        self.branching_factor = branching_factor
        self.v_ids = SmartAVLTree(avl_tree_osam=avl_tree_osam) # v_name -> v_id
        self.entry_points = SmartAVLTree(avl_tree_osam=avl_tree_osam) # v_id -> SmartPointer to Vertex
        self.can_emulate = True
        self.num_deleted_objs = 0
        
        # additional access stats
        self.ogV_counts = dict() # v_id -> number of shared pointers
        self.ogV_accesses = 0
        self.out_tree_accesses = 0
        self.ogV_degree = 0
        self.out_tree_degree = 0
        self.ogV_depth = [] # list of ogV depths to average

    def reset_access_stats(self):
        self.ogV_accesses = 0
        self.out_tree_accesses = 0
        self.ogV_degree = 0
        self.out_tree_degree = 0
        self.ogV_depth.clear()

    def print_access_stats(self):
        print("\nAccess stats: ogV accesses, out tree accesses, avg ogV degree, avg out tree degree, avg ogV depth")
        avg_ogV_degree = round(self.ogV_degree/self.ogV_accesses) if self.ogV_accesses > 0 else 0
        avg_out_tree_degree = round(self.out_tree_degree/self.out_tree_accesses) if self.out_tree_accesses > 0 else 0
        avg_ogV_depth = sum(self.ogV_depth)/len(self.ogV_depth) if len(self.ogV_depth) > 0 else 0
        print(f"({self.ogV_accesses}, {self.out_tree_accesses}, {avg_ogV_degree}, {avg_out_tree_degree}, {avg_ogV_depth})")
            
    def get_pointer(self, src_name):
        src_id = self.v_ids[src_name]
        p_src = self.entry_points[src_id]
        
        if p_src is None:
            raise KeyError(f"Error: Source name {src_name} not found.")
        
        return src_id, p_src

    def get_ogV_full_copy(self, p_src):
        pre_alloc_count = get_global_counter()[0]
        ogV = SmartPointer.get_and_copy(p_src)
        assert ogV.type == 'VERTEX'
        self.ogV_depth.append(get_global_counter()[0] - pre_alloc_count)
        self.ogV_degree += self.ogV_counts[ogV.id]
        self.ogV_accesses += 1
        return p_src, ogV
    
    def get_ogV_attr(self, p_src, req_attr=['id', 'type'], make_copies=True):
        pre_alloc_count = get_global_counter()[0]
        v_attr = SmartPointer.get_attr(p_src, req_attr, make_copies)
        assert 'id' in req_attr and 'type' in req_attr
        assert v_attr['type'] == 'VERTEX'
        self.ogV_depth.append(get_global_counter()[0] - pre_alloc_count)
        self.ogV_degree += self.ogV_counts[v_attr['id']]
        self.ogV_accesses += 1
        return p_src, v_attr

    def add_vertex(self, v_name, v_ids, entry_points, out_degree=0, out_children=None, label=None):
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
        v_id = self.n
        self.n += 1

        # assign children if they exist 
        if out_children is None:
            out_children = [None]*self.branching_factor

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
            type='VERTEX',
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

    def add_edge_full_copy(self, src_name, dst_name, weight=0):
        # manually adding an edge means the graph can no longer be statically emulated
        self.can_emulate = False

        # create src/dst SmartPointer/vertices
        src_id, p_src = self.add_vertex(v_name=src_name, v_ids=self.v_ids, entry_points=self.entry_points)
        dst_id, p_dst = self.add_vertex(v_name=dst_name, v_ids=self.v_ids, entry_points=self.entry_points)

        # ensure edge does not exist yet
        if not self.edge_exists_full_copy(p_src, dst_id):
            # add OutEdge 
            self._add_out_edge_full_copy(src_id, p_src, dst_id, p_dst, weight)          
            return True

        return False

    def add_edge(self, src_name, dst_name, weight=0, make_copies=True):
        # manually adding an edge means the graph can no longer be statically emulated
        self.can_emulate = False

        # create src/dst SmartPointer/vertices
        src_id, p_src = self.add_vertex(v_name=src_name, v_ids=self.v_ids, entry_points=self.entry_points)
        dst_id, p_dst = self.add_vertex(v_name=dst_name, v_ids=self.v_ids, entry_points=self.entry_points)

        # ensure edge does not exist yet
        if not self.edge_exists(p_src, dst_id, make_copies):
            # add OutEdge 
            self._add_out_edge(src_id, p_src, dst_id, p_dst, weight, make_copies)
            return True

        return False

    def edge_exists_full_copy(self, p_src, dst_id):
        # determine if src has path to dst
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

        # traverse child out_edges
        while sq.head is not None:
            p = queue.dequeue(sq)
            if p is None:
                continue

            # no need to traverse if dst_id was found
            if found:
                SmartPointer.delete_copy(p)
                continue
                
            # load v
            v = SmartPointer.get_and_copy(p)
            assert v.type == 'OUT_EDGE'

            # traverse until reaching OutEdge leaves
            if not v.is_leaf:
                for i in range(self.branching_factor):
                    if v.children[i] is not None:
                        queue.enqueue(sq, v.children[i])
                        v.children[i] = None
                    else:
                        break
            # reached leaf
            else:             
                if v.dst_id == dst_id:
                    found = True

            # no longer need to access v or p
            SmartPointer.delete(v)
            SmartPointer.delete_copy(p)
        
        return found

    def edge_exists(self, p_src, dst_id, make_copies=True):
        # determine if src has path to dst
        found = False

        # load ogV
        req_attr = ['out_children', 'type', 'out_degree', 'id']
        p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)

        # queue children of src to visit
        sq = queue.init_queue()
        for i in range(self.branching_factor):
            if v_attr['out_children'][i] is not None:
                queue.enqueue(sq, v_attr['out_children'][i])
            else:
                break

        # traverse child out_edges
        req_attr = ['type', 'is_leaf', 'children', 'dst_id']
        while sq.head is not None:
            p = queue.dequeue(sq)
            if p is None:
                continue

            # no need to traverse if dst_id was found
            if found:
                SmartPointer.delete_copy(p)
                continue

            # load v attributes
            v_attr = SmartPointer.get_attr(p, req_attr, make_copies)
            assert v_attr['type'] == 'OUT_EDGE'
            self.out_tree_accesses += 1
            degree = 2 if SmartPointer.is_temp_copy(p) else 1
            self.out_tree_degree += degree

            # traverse until reaching OutEdge leaves
            if not v_attr['is_leaf']:
                for i in range(self.branching_factor):
                    if v_attr['children'][i] is not None:
                        queue.enqueue(sq, v_attr['children'][i])
                    else:
                        break

            # reached leaf
            else:
                if v_attr['dst_id'] == dst_id:
                    found = True
        
            # no longer need to access p
            SmartPointer.delete_copy(p)

        return found

    def _add_out_edge_full_copy(self, src_id, p_src, dst_id, p_dst, weight):
        # new OutEdge leaf
        v_out_leaf = OutEdge(
            type='OUT_EDGE',
            children=[None]*self.branching_factor,
            label=None,
            weight=weight,
            is_leaf=True,
            dst_id=dst_id,
            ogE=SmartPointer.copy(p_dst)
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
                    SmartPointer.delete_copy(p)
                    continue

                # load v
                v = SmartPointer.get_and_copy(p)
                assert v.type == 'OUT_EDGE'

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
                        # this is also the last level of OutEdges
                        v.children[i] = p_out_leaf
                        SmartPointer.put(p, v)
                        found = True

                    else:
                        # still at internal OutEdges
                        queue.enqueue(sq, v.children[i])
                        v.children[i] = None
                        all_leaves = all_leaves[start:start+step]
                    
                    break

                SmartPointer.delete(v)
                SmartPointer.delete_copy(p)

        # create and insert at a new level
        else:
            # set current children to grandchildren of new left most child
            leftmost_child = OutEdge(
                type='OUT_EDGE',
                children=ogV.out_children,
                label=None,
                weight=0,
                is_leaf=False,
                dst_id=None,
                ogE=None
            )

            # collect new children pointers for ogV
            new_out_children = [SmartPointer(SmartPointer.new(leftmost_child))]
            
            for i in range(1, self.branching_factor):
                # p_out_leaf is the only new OutEdge to add at the tree's bottom 
                if i == 1:
                    sq = queue.init_queue()
                    queue.enqueue(sq, p_out_leaf)
                else:
                    sq = queue(head=None, tail=None)

                # build OutEdge tree
                sq = self._build_outgoing_edge_tree(sq=sq, height=height, src_id=src_id, offset=1)
                
                # empty queue
                while sq.head is not None:
                    p_child = queue.dequeue(sq)
                    if p_child is not None:
                        new_out_children.append(p_child)

            ogV.out_children = new_out_children
            ogV.height = new_height
            SmartPointer.put(p_src, ogV)
            SmartPointer.delete(ogV)

    def _add_out_edge(self, src_id, p_src, dst_id, p_dst, weight, make_copies=True):
        # new OutEdge leaf
        v_out_leaf = OutEdge(
            type='OUT_EDGE',
            children=[None]*self.branching_factor,
            label=None,
            weight=weight,
            is_leaf=True,
            dst_id=dst_id,
            ogE=SmartPointer.copy(p_dst)
        )

        self.ogV_counts[dst_id] += 1
        p_out_leaf = SmartPointer(SmartPointer.new(v_out_leaf))

        # get ogV's first layer of children and out degree
        req_attr = ['type', 'out_degree', 'id', 'height']
        p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)
        out_degree = v_attr['out_degree']

        # prepare setter dict
        set_attr = {'out_degree': out_degree + 1}

        # height calculations
        height = v_attr['height']
        new_height = ceil(log(out_degree+1, self.branching_factor)) if out_degree+1 > 1 else 1

        # insert ogE at ogV's first level
        if out_degree < self.branching_factor:
            # get out_children
            req_attr = ['type', 'out_children', 'id']
            p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)
            out_children = v_attr['out_children']

            # update children
            out_children[out_degree] = p_out_leaf
            set_attr['out_children'] = out_children 
            set_attr['height'] = new_height 
            SmartPointer.put_attr(p_src, set_attr, delete_old=make_copies)

        # insert ogE without creating a new tree level
        elif height == new_height:
            # update ogV 
            SmartPointer.put_attr(p_src, set_attr, delete_old=make_copies)
        
            # precompute path: sequence of child indices to follow at each tree level (most-significant first)
            target = out_degree
            subtree_height = ceil(log(out_degree, self.branching_factor)) if out_degree > 1 else 1
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

            leaf_idx = path.pop()
            p = p_src
            i = 0

            pre_alloc_count = get_global_counter()[0]
            for idx in path:
                child_term = 'out_children' if i == 0 else 'children'
                sl = sublist(child_term, [idx])
                next_child = SmartPointer.get_attr(p, sl, make_copies)[child_term][0]

                if i == 0:
                    self.ogV_depth.append(get_global_counter()[0] - pre_alloc_count)
                    self.ogV_degree += self.ogV_counts[src_id]
                    self.ogV_accesses += 1

                else:
                    SmartPointer.delete_copy(p)
                    self.out_tree_accesses += 1
                    degree = 2 if SmartPointer.is_temp_copy(p) else 1
                    self.out_tree_degree += degree

                i += 1
                p = next_child

            # get children 
            children = SmartPointer.get_attr(p, 'children', make_copies)
            self.out_tree_accesses += 1
            degree = 2 if SmartPointer.is_temp_copy(p) else 1
            self.out_tree_degree += degree

            # put OutEdge with ogE in tree
            children[leaf_idx] = p_out_leaf
            set_attr_intermediate = {'children': children}
            SmartPointer.put_attr(p, set_attr_intermediate, delete_old=make_copies)
            SmartPointer.delete_copy(p)
                            
        # create and insert at a new level
        else:
            # get out_children
            req_attr = ['type', 'out_children', 'id']
            p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)
            out_children = v_attr['out_children']

            for i in range(self.branching_factor):
                out_children[i].temp_copy = False

            # set current children to grandchildren of new left most child
            leftmost_child = OutEdge(
                type='OUT_EDGE',
                children=out_children,
                label=None,
                weight=0,
                is_leaf=False,
                dst_id=None,
                ogE=None
            )

            # collect new children pointers for ogV
            new_out_children = [SmartPointer(SmartPointer.new(leftmost_child))]
            
            for i in range(1, self.branching_factor):
                # p_out_leaf is the only new OutEdge to add at the tree's bottom 
                if i == 1:
                    sq = queue.init_queue()
                    queue.enqueue(sq, p_out_leaf)
                else:
                    sq = queue(head=None, tail=None)

                # build OutEdge tree
                sq = self._build_outgoing_edge_tree(sq=sq, height=height, src_id=src_id, offset=1)

                # empty queue
                while sq.head is not None:
                    p_child = queue.dequeue(sq)
                    if p_child is not None:
                        new_out_children.append(p_child)

            set_attr['out_children'] = new_out_children
            set_attr['height'] = new_height 
            SmartPointer.put_attr(p_src, set_attr, delete_old=make_copies)

    def _build_outgoing_edge_tree(self, sq, height, src_id, offset=0):
        # recursively build outgoing edge tree from bottom to top
        if height > 0:
            num_vertices = int(self.branching_factor**(height-offset))
            new_sq = queue.init_queue()

            # create one level of internal out_edges
            for i in range(num_vertices):
                v = OutEdge(
                    type='OUT_EDGE',
                    children=[None]*self.branching_factor,
                    label=None,
                    weight=0,
                    is_leaf=False,
                    dst_id=None,
                    ogE=None
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

            return self._build_outgoing_edge_tree(sq=new_sq, height=height-1, src_id=src_id, offset=offset)
        
        else: 
            return sq

    def emulate_graph(self, edges=None, graph=None, move_semantics=True, sort_ids=False, bidirectional_edges=True):
        # edges: 
        #   1) adjacency list of directed edge tuples with optional weights
        #   2) adjacency dictionary of src names to dst names with optional weights
        # graph: None or networkx Graph/DiGraph

        assert edges is not None or graph is not None

        # allow emulate graph to be called at most once
        if not self.can_emulate:
            return
        self.can_emulate = False

        # prepare networkx graph 
        match graph:
            case nx.DiGraph():
                DG = graph   
            case nx.Graph():
                DG = nx.DiGraph(graph)
            case _:
                DG = nx.DiGraph()

        # convert adjacency dictionary to adjacency list
        if type(edges) is dict:
            new_edges = []
            for src, dst in edges.items():
                weight = 0
                if type(dst) in [list, tuple] and len(dst) == 2:
                    dst, weight = dst
                
                if weight is not None:
                    new_edges.append((src, dst, weight))
                else:
                    new_edges.append((src, dst))
            
            edges = new_edges

        # add edges to DiGraph
        if edges is not None:
            if all(len(edge) == 3 for edge in edges):
                if bidirectional_edges:
                    new_edges = [(edge[1], edge[0], edge[2]) for edge in edges]
                    edges += new_edges

                DG.add_weighted_edges_from(edges)

            else:
                if bidirectional_edges:
                    new_edges = [(edge[1], edge[0]) for edge in edges]
                    edges += new_edges
               
                DG.add_edges_from(edges)

        # local storage for building
        v_ids = dict()
        entry_points = dict()

        if move_semantics:
            self._emulate_graph(DG, entry_points, v_ids, sort_ids)
        else:
            self._emulate_graph_full_copy(DG, entry_points, v_ids, sort_ids)

        # transfer local storage to smart AVL tree
        for v_name, v_id in v_ids.items():
            self.v_ids[v_name] = v_id

        for v_id, p in entry_points.items():
            self.entry_points[v_id] = p
    
    def _emulate_graph_full_copy(self, DG, entry_points, v_ids, sort_ids):
        if sort_ids:
            nodes = sorted(list(DG.nodes))
        else:
            nodes = DG.nodes
        
        # create entry points
        for v_name in nodes:
            out_degree = len(list(DG.neighbors(v_name)))
            self.add_vertex(v_name=v_name, v_ids=v_ids, entry_points=entry_points, out_degree=out_degree)

        # build outgoing edge tree
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
                    weight = DG.edges[src_name, dst_name]['weight']
                except:
                    weight = 0

                pre_alloc_count = get_global_counter()[0]
                v_out_leaf = OutEdge(
                    type='OUT_EDGE',
                    children=[None]*self.branching_factor,
                    label=None,
                    weight=weight,
                    is_leaf=True,
                    dst_id=dst_id,
                    ogE=SmartPointer.copy(p_dst)
                )

                self.ogV_counts[dst_id] += 1

                total_allocs += get_global_counter()[0] - pre_alloc_count
                p_out_leaf = SmartPointer(SmartPointer.new(v_out_leaf))
                queue.enqueue(sq, p_out_leaf)

            sq = self._build_outgoing_edge_tree(sq=sq, height=height, src_id=src_id)

            # assign OutEdge children
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

    def _emulate_graph(self, DG, entry_points, v_ids, sort_ids):
        if sort_ids:
            nodes = sorted(list(DG.nodes))
        else:
            nodes = DG.nodes

        # create entry points
        for v_name in nodes:
            out_degree = len(list(DG.neighbors(v_name)))
            self.add_vertex(v_name=v_name, v_ids=v_ids, entry_points=entry_points, out_degree=out_degree)

        # build outgoing edge tree
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
                    weight = DG.edges[src_name, dst_name]['weight']
                except:
                    weight = 0

                v_out_leaf = OutEdge(
                    type='OUT_EDGE',
                    children=[None]*self.branching_factor,
                    label=None,
                    weight=weight,
                    is_leaf=True,
                    dst_id=dst_id,
                    ogE=SmartPointer.copy(p_dst)
                )

                self.ogV_counts[dst_id] += 1

                p_out_leaf = SmartPointer(SmartPointer.new(v_out_leaf))
                queue.enqueue(sq, p_out_leaf)

            sq = self._build_outgoing_edge_tree(sq=sq, height=height, src_id=src_id)

            # assign OutEdge children
            out_children = []
            for i in range(self.branching_factor):
                p_child = queue.dequeue(sq)
                out_children.append(p_child)
            
            while sq.head is not None:
                queue.dequeue(sq)

            # update ogV entry
            set_attr = {'out_children': out_children}
            p_src = entry_points[src_id]
            SmartPointer.put_attr(p_src, set_attr)

    def delete_vertex_full_copy(self, src_name):
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

        # delete OutEdge tree
        while sq.head is not None:
            ogE = None
            p = queue.dequeue(sq)
            if p is None:
                continue 
            
            # load v 
            v = SmartPointer.get_and_copy(p)
            assert v.type == 'OUT_EDGE'
            self.out_tree_accesses += 1
            self.out_tree_degree += 2

            # at internal OutEdge
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

            # need to delete ogE at original OutEdge first
            # otherwise is_single_reference always fails
            if SmartPointer.is_single_reference(ogE): 
                self.num_deleted_objs -= 1

            SmartPointer.delete(ogE)

        # wait until tree has been deleted so that putting a deleted object
        # as the pointee; original smart pointer always deletes with put()
        self.ogV_counts[src_id] -= 1
        deleted_obj = DeletedObject(id=src_id, type='DELETED_OBJECT')
        SmartPointer.put(p_src, deleted_obj, delete_old=True)
        self.num_deleted_objs += 1
        if SmartPointer.is_single_reference(p_src): 
            self.num_deleted_objs -= 1

        SmartPointer.delete(p_src)

    def delete_vertex(self, src_name, make_copies=True):
        src_id = self.v_ids[src_name]
        p_src = self.entry_points.delete(src_id)
        if p_src is None:
            return

        # load ogV attrs
        req_attr = ['out_children', 'type', 'id']
        p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)
        out_children = v_attr['out_children']

        # queue children of src to visit
        sq = queue.init_queue()
        for i in range(self.branching_factor):
            if out_children[i] is not None:
                queue.enqueue(sq, out_children[i])
            else:
                break

        # delete OutEdge tree
        req_attr = ['type', 'is_leaf', 'children', 'dst_id', 'ogE']
        while sq.head is not None:
            p = queue.dequeue(sq)
            if p is None:
                continue 
            
            # load v attributes
            v_attr = SmartPointer.get_attr(p, req_attr, make_copies)
            assert v_attr['type'] == 'OUT_EDGE'
            self.out_tree_accesses += 1
            degree = 2 if SmartPointer.is_temp_copy(p) else 1
            self.out_tree_degree += degree
            children = v_attr['children']
            ogE = None

            # at internal node
            if not v_attr['is_leaf']:
                assert v_attr['ogE'] is None and v_attr['dst_id'] is None
                for i in range(self.branching_factor):
                    if children[i] is not None:
                        queue.enqueue(sq, children[i])
                        children[i] = None
                    else:
                        break
                        
            # ogE is reached
            else:
                self.ogV_counts[v_attr['dst_id']] -= 1
                ogE = v_attr['ogE']
            
            # update pointer with deleted pointers
            set_attr = {'children': children, 'ogE': None, 'dst_id': None, 'weight': 0}
            SmartPointer.put_attr(p, set_attr, delete_old=make_copies)
            SmartPointer.delete(p)

            # wait until underlying tree and the original ogE are deleted
            # so that make_copies doesn't prevent us from using
            # is_single_reference correctly
            if SmartPointer.is_single_reference(ogE): 
                self.num_deleted_objs -= 1
            SmartPointer.delete(ogE)

        # place deleted object in place of ogV
        self.ogV_counts[src_id] -= 1
        deleted_obj = DeletedObject(id=src_id, type='DELETED_OBJECT')
        SmartPointer.put(p_src, deleted_obj, delete_old=make_copies)
        self.num_deleted_objs += 1
        if SmartPointer.is_single_reference(p_src): 
            self.num_deleted_objs -= 1
        SmartPointer.delete(p_src)

    def delete_edge_full_copy(self, src_name, dst_name):
        # get src_id/pointer and dst_id
        src_id, p_src = self.get_pointer(src_name) 
        dst_id = self.v_ids[dst_name]

        # rearrange and resize
        new_out_degree, found = self.reorder_outgoing_edge_tree_full_copy(p_src, None, dst_id)
        self.downsize_outgoing_tree_full_copy(p_src, new_out_degree)

        if not found:
            print(f"Could not find edge: {src_name}->{dst_name}")
            
    def delete_edge(self, src_name, dst_name, make_copies=True):
        # get src_id/pointer and dst_id
        src_id, p_src = self.get_pointer(src_name) 
        dst_id = self.v_ids[dst_name]
    
        # rearrange and resize
        new_out_degree, found = self.reorder_outgoing_edge_tree(p_src, None, make_copies, dst_id)
        self.downsize_outgoing_tree(p_src, new_out_degree, make_copies)

        if not found:
            print(f"Could not find edge: {src_name}->{dst_name}")

    def process_deleted_objects_full_copy(self, p, v, out_degree):
        # check if ogE points to vertex or deleted object
        pre_alloc_count = get_global_counter()[0]
        obj = SmartPointer.get_and_copy(v.ogE)

        if obj.type == 'VERTEX':
            self.ogV_depth.append(get_global_counter()[0] - pre_alloc_count)
            self.ogV_degree += self.ogV_counts[obj.id]
            self.ogV_accesses += 1
            SmartPointer.delete(obj)
            return p, v, out_degree
            
        assert obj.type == 'DELETED_OBJECT'

        # save ogE    
        ogE = v.ogE
        dst_id = v.dst_id

        # delete ogE at parent pointer
        v.ogE = None
        v.dst_id = None
        v.weight = 0
        SmartPointer.put(p, v)
        
        # delete ogE temp copy and check single reference
        if SmartPointer.is_single_reference(ogE):
            self.num_deleted_objs -= 1
            del self.v_ids[dst_id]
        SmartPointer.delete(ogE)
        self.ogV_counts[dst_id] -= 1
        out_degree -= 1

        return p, v, out_degree

    def process_deleted_objects(self, p, ogE, dst_id, out_degree, make_copies):
        # a pointer with exactly one reference must point to a deleted 
        # object otherwise it would have at least two pointers between 
        # itself and its entry point in the AVL Tree
        if make_copies or not SmartPointer.is_single_reference(ogE):
            # check if ogE points to vertex or deleted object
            req_attr = ['type', 'id']
            pre_alloc_count = get_global_counter()[0]
            obj_attr = SmartPointer.get_attr(ogE, req_attr, make_copies=False)
            
            if obj_attr['type'] == 'VERTEX':
                self.ogV_depth.append(get_global_counter()[0] - pre_alloc_count)
                self.ogV_degree += self.ogV_counts[obj_attr['id']]
                self.ogV_accesses += 1
                return p, ogE, dst_id, out_degree

            assert obj_attr['type'] == 'DELETED_OBJECT'
            
        # update parent
        set_attr = {'ogE': None, 'dst_id': None, 'weight': 0}
        SmartPointer.put_attr(p, set_attr, delete_old=make_copies)

        # delete ogE
        if SmartPointer.is_single_reference(ogE): 
            self.num_deleted_objs -= 1
            del self.v_ids[dst_id]
        SmartPointer.delete(ogE)
        self.ogV_counts[dst_id] -= 1
        out_degree -= 1

        return p, None, None, out_degree

    def visit_full_copy(self, p_src, ogV, label):
        # update label at ogV
        ogV.label = label
        ogV.visited = True
        src_id = ogV.id  
        SmartPointer.put(p_src, ogV)
        return ogV, src_id

    def visit(self, p_src, label, make_copies=True):
        # update label at ogV
        req_attr = ['type', 'id']
        p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)
        src_id = v_attr['id']
        set_attr = {'label': label, 'visited': True}
        SmartPointer.put_attr(p_src, set_attr)
        return p_src, src_id

    def _unvisit_full_copy(self, visited):
        # reset all visited vertices 
        while visited.head is not None:
            p = queue.dequeue(visited)
            if p is not None:
                p, v = self.get_ogV_full_copy(p)
                v.visited = False
                SmartPointer.put(p, v)
                SmartPointer.delete(v)
                if SmartPointer.is_temp_copy(p): self.ogV_counts[v.id] -= 1
                SmartPointer.delete_copy(p)

    def _unvisit(self, visited, make_copies=True):
        # reset all visited vertices 
        set_attr = {'visited' : False}
        while visited.head is not None:
            po = queue.dequeue(visited)
            if po is not None:
                p, og_id = po
                SmartPointer.put_attr(p, set_attr)
                if SmartPointer.is_temp_copy(p): self.ogV_counts[og_id] -= 1 
                SmartPointer.delete_copy(p)

    def add_neighbors_full_copy(self, p_src, ogV, ds, pDS, getL=None, getP=None):
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
            assert v.type == 'OUT_EDGE'
            self.out_tree_accesses += 1
            self.out_tree_degree += 2

            # OutEdge children contains ogEs or more out_edges
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
                match getL:
                    case "ID":
                        label = og_id
                    case "LW":
                        label = og_label + v.weight
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
                    case 'QUEUE':
                        queue.enqueue(pDS, [label, v.ogE])
                    case 'STACK':
                        pDS = stack.push(pDS, [label, v.ogE])
                    case 'PQ':
                        PQ.insert(pty, [label, v.ogE, og_id])
                    case 'SET':
                        pDS.add(v.dst_id)
                        SmartPointer.delete_copy(v.ogE)
                    case 'LIST':
                        pDS.append(v.dst_id)
                        SmartPointer.delete_copy(v.ogE)
                
                if ds not in ['SET', 'LIST']: self.ogV_counts[v.dst_id] += 1
                v.ogE = None
            
            SmartPointer.delete(v)
            SmartPointer.delete_copy(p)

        # rearrange and resize OutEdge tree
        if new_out_degree < out_degree:
            new_out_degree = self.reorder_outgoing_edge_tree_full_copy(p_src, new_out_degree)
            self.downsize_outgoing_tree_full_copy(p_src, new_out_degree)

        return pDS

    def add_neighbors(self, p_src, ds, pDS, getL=None, getP=None, make_copies=True):
        # load ogV
        req_attr = ['type', 'out_children', 'id', 'label', 'out_degree', 'height']
        p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)
        og_id = v_attr['id']
        og_label = v_attr['label']
        out_degree = new_out_degree = v_attr['out_degree']
        height = v_attr['height']
        out_children = v_attr['out_children']

        # queue first level of vertices for visiting
        sq = queue.init_queue()
        for i in range(self.branching_factor):
            if out_children[i] is not None:
                # queue child
                queue.enqueue(sq, out_children[i])
            else:
                break

        # exclude ogE if possible to avoid useless copies
        req_attr = ['type', 'is_leaf', 'children', 'weight', 'ogE', 'dst_id']
        if ds in ['LIST', 'SET'] and self.num_deleted_objs <= 0: req_attr.remove('ogE')

        # traverse down until ogEs are reached 
        while sq.head is not None:
            p = queue.dequeue(sq)
            if p is None:
                continue 

            # load v attributes
            v_attr = SmartPointer.get_attr(p, req_attr, make_copies)
            assert v_attr['type'] == 'OUT_EDGE'
            self.out_tree_accesses += 1
            degree = 2 if SmartPointer.is_temp_copy(p) else 1
            self.out_tree_degree += degree

            # important variables
            ogE = v_attr['ogE'] if 'ogE' in v_attr else None
            dst_id = v_attr['dst_id']
            is_leaf = v_attr['is_leaf']
            weight = v_attr['weight']
            children = v_attr['children']
                    
            # OutEdge children contains ogEs or more out_edges
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
            if ogE is not None or (dst_id is not None and ds in ['LIST', 'SET']):
                # get proper label
                match getL:
                    case "ID":
                        label = og_id
                    case "LW":
                        label = og_label + weight
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
                    case 'QUEUE':
                        queue.enqueue(pDS, [label, ogE])
                    case 'STACK':
                        pDS = stack.push(pDS, [label, ogE])
                    case 'PQ':
                        PQ.insert(pty, [label, ogE, og_id])
                    case 'SET':
                        pDS.add(dst_id)
                        SmartPointer.delete_copy(ogE)
                        ogE = None
                    case 'LIST':
                        pDS.append(dst_id)
                        SmartPointer.delete_copy(ogE)
                        ogE = None
                        
                if SmartPointer.is_temp_copy(ogE): 
                    self.ogV_counts[dst_id] += 1
            
            SmartPointer.delete_copy(p)

        # rearrange and resize OutEdge tree
        if new_out_degree < out_degree:
            new_out_degree = self.reorder_outgoing_edge_tree(p_src, new_out_degree, make_copies)
            self.downsize_outgoing_tree(p_src, new_out_degree, make_copies)

        return pDS

    def reorder_outgoing_edge_tree_full_copy(self, p_src, new_out_degree=None, dst_id_to_delete=None):
        # traverse tree and collect pointers to internal
        # OutEdges, leaf OutEdges with ogEs, and leaf OutEdges
        # with no ogEs

        # load ogV
        p_src, ogV = self.get_ogV_full_copy(p_src)
        height = ogV.height
        if new_out_degree is None: new_out_degree = ogV.out_degree
        found = False

        # assign each ogV/outEdge an id
        parent_id = 0
        child_id = 1
        parent_map = dict() # child ids to parent ids
        child_map = dict() # parent ids to list of children ids
        child_list = []

        # storage devices for preserving left to right order
        # if a deleted object is found
        deleted_leaf_sq = queue.init_queue() 
        leaf_top = stack.init_stack() # stack pointers to leaf outEdges
        internal_top = stack.init_stack() # stack pointers to internal outEdges
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
            assert v.type == 'OUT_EDGE'
            self.out_tree_accesses += 1
            self.out_tree_degree += 2

            # OutEdge children contains ogEs or more out_edges
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

            # stack outEdge leaves with an ogE
            elif v.ogE is not None:
                leaf_top = stack.push(leaf_top, (parent_id, p, v))

            # track id of leaf outEdges with no ogE
            else:
                queue.enqueue(deleted_leaf_sq, (parent_id, p))   

            # increase parent_id
            parent_id += 1

        # shift ogEs in right of tree to replace deleted
        # ogEs in the left side of the tree
        parent_indices_to_update = dict()
        while deleted_leaf_sq.head is not None:
            # get pointer to outEdge with deleted ogE
            ip1 = queue.dequeue(deleted_leaf_sq)
            if ip1 is None:
                deleted_id = p_deleted = None
            else:           
                deleted_id, p_deleted = ip1
            
            # get pointer to outEdge with intact ogE
            ip2, leaf_top = stack.pop(leaf_top)   
            if ip2 is None:
                child_id = p_child = v_child = None
            else: 
                child_id, p_child, v_child = ip2

            # only swap if the child/leaf is to the right 
            # of the pointer with the deleted ogE
            if deleted_id is not None:
                if child_id is not None and deleted_id < child_id:
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

            SmartPointer.delete_copy(p_child)
            SmartPointer.delete_copy(p_deleted)

        # delete leftover pointers
        while leaf_top is not None:
            ip2, leaf_top = stack.pop(leaf_top)   
            if ip2 is None:
                continue

            child_id, p_child, v_child = ip2
            SmartPointer.delete(v_child)
            SmartPointer.delete_copy(p_child)

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

            SmartPointer.delete_copy(p_parent)

        if dst_id_to_delete is not None:
            return new_out_degree, found
        else:
            return new_out_degree

    def reorder_outgoing_edge_tree(self, p_src, new_out_degree=None, make_copies=True, dst_id_to_delete=None):
        # traverse tree and collect pointers to internal
        # OutEdges, leaf OutEdges with ogEs, and leaf OutEdges
        # with no ogEs

        # load ogV
        req_attr = ['type', 'out_children', 'id', 'height', 'out_degree']
        p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)
        height = v_attr['height']
        out_children = v_attr['out_children']
        if new_out_degree is None: new_out_degree = v_attr['out_degree']
        found = False

        # assign each ogV/outEdge an id
        parent_id = 0
        child_id = 1
        parent_map = dict() # child ids to parent ids
        child_map = dict() # parent ids to list of children ids
        child_list = []

        # storage devices for preserving left to right order
        # if a deleted object is found
        deleted_leaf_sq = queue.init_queue() 
        leaf_top = stack.init_stack() # stack pointers to leaf outEdges
        internal_top = stack.init_stack() # stack pointers to internal outEdges
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
        req_attr = ['type', 'is_leaf', 'children', 'ogE', 'dst_id', 'weight']
        while sq.head is not None:
            p = queue.dequeue(sq)
            if p is None:
                continue 

            # load v attributes
            v_attr = SmartPointer.get_attr(p, req_attr, make_copies)
            assert v_attr['type'] == 'OUT_EDGE'
            self.out_tree_accesses += 1
            degree = 2 if SmartPointer.is_temp_copy(p) else 1
            self.out_tree_degree += degree

            # important variables
            ogE = v_attr['ogE'] if 'ogE' in v_attr else None
            dst_id = v_attr['dst_id']
            is_leaf = v_attr['is_leaf']
            children = v_attr['children']
            weight = v_attr['weight']
                    
            # OutEdge children contains ogEs or more out_edges
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
                set_attr = {'ogE': None, 'dst_id': None, 'weight': 0}
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

            # stack outEdge leaves with an ogE
            elif ogE is not None:
                leaf_top = stack.push(leaf_top, (parent_id, p, ogE, dst_id, weight))

            # track id of leaf outEdges with no ogE
            else:
                queue.enqueue(deleted_leaf_sq, (parent_id, p))   

            # increase parent_id
            parent_id += 1
        
        # shift ogEs in right of tree to replace deleted
        # ogEs in the left side of the tree
        parent_indices_to_update = dict()
        while deleted_leaf_sq.head is not None:
            parent_id = None
            # get pointer to outEdge with deleted ogE
            ip1 = queue.dequeue(deleted_leaf_sq)
            if ip1 is None:
                deleted_id = p_deleted = None
            else:           
                deleted_id, p_deleted = ip1
            
            # get pointer to outEdge with intact ogE
            ip2, leaf_top = stack.pop(leaf_top)   
            if ip2 is None:
                child_id = p_child = ogE = dst_id = weight = None
            else: 
                child_id, p_child, ogE, dst_id, weight = ip2

            # only swap if the child/leaf is to the right 
            # of the pointer with the deleted ogE
            if deleted_id is not None:
                if child_id is not None and deleted_id < child_id:
                    # collect index of parent to update
                    parent_id = parent_map[child_id]
                    i = child_map[parent_id].index(child_id)

                    # transfer rightmost attrs left
                    set_attr = {'ogE': ogE, 'dst_id': dst_id, 'weight': weight}
                    SmartPointer.put_attr(p_deleted, set_attr, delete_old=make_copies)

                    # update rightmost pointer with deleted items
                    set_attr = {'ogE': None, 'dst_id': None, 'weight': None}
                    SmartPointer.put_attr(p_child, set_attr, delete_old=make_copies)

                else:
                    # collect index of parent to update
                    parent_id = parent_map[deleted_id]
                    i = child_map[parent_id].index(deleted_id)
                    SmartPointer.delete_copy(ogE)

                # dictionary of indices to update
                if parent_id in parent_indices_to_update:
                    parent_indices_to_update[parent_id].append(i)
                else:
                    parent_indices_to_update[parent_id] = [i]

            SmartPointer.delete_copy(p_child)
            SmartPointer.delete_copy(p_deleted)

        # delete leftover pointers
        while leaf_top is not None:
            ip2, leaf_top = stack.pop(leaf_top)   
            if ip2 is None:
                continue

            child_id, p_child, ogE, dst_id, weight = ip2
            SmartPointer.delete_copy(p_child)
            SmartPointer.delete_copy(ogE)

        # delete leftover pointers
        while internal_top is not None:
            ip3, internal_top = stack.pop(internal_top)   
            if ip3 is None:
                continue
            parent_id, p_parent = ip3

            if parent_id in parent_indices_to_update:
                # get children of internal parent
                if parent_id == 0:
                    req_attr = ['id', 'type', 'out_children']
                    p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)
                    children = v_attr['out_children']

                else: 
                    children = SmartPointer.get_attr(p_parent, 'children', make_copies)
                    self.out_tree_accesses += 1
                    degree = 2 if SmartPointer.is_temp_copy(p_parent) else 1
                    self.out_tree_degree += degree

                # update deleted children
                for i in parent_indices_to_update[parent_id]:
                    children[i] = None

                # put changes back
                if parent_id == 0:
                    SmartPointer.put_attr(p_src, {'out_children': children}, delete_old=make_copies)
                else: 
                    SmartPointer.put_attr(p_parent, {'children': children}, delete_old=make_copies)

            SmartPointer.delete_copy(p_parent)

        if dst_id_to_delete is not None:
            return new_out_degree, found
        else:
            return new_out_degree

    def downsize_outgoing_tree_full_copy(self, p_src, new_out_degree):
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
                    v = SmartPointer.get_and_copy(p)
                    assert v.type == 'OUT_EDGE' and not v.is_leaf
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
                assert v.type == 'OUT_EDGE'
                self.out_tree_accesses += 1
                self.out_tree_degree += 2

                # at internal OutEdge
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

    def downsize_outgoing_tree(self, p_src, new_out_degree, make_copies=True):
        # load ogV attrs
        req_attr = ['id', 'type', 'out_children', 'out_degree', 'height']
        p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies)
        out_children = v_attr['out_children']
        height = v_attr['height']

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
                req_attr = ['children', 'type', 'is_leaf']
                for i in range(height-new_height):
                    # get next level of children
                    v_attr = SmartPointer.get_attr(p, req_attr, make_copies)
                    assert v_attr['type'] == 'OUT_EDGE' and not v_attr['is_leaf']
                    self.out_tree_accesses += 1
                    self.out_tree_degree += 1
                    children = v_attr['children']

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
                        SmartPointer.put_attr(p, {'children': children}, delete_old=make_copies)
                        SmartPointer.delete(p)
                        p = p_child

                    else:
                        # set leftmost children as new ogV children
                        for j in range(self.branching_factor):
                            out_children[j] = children[j]
                            children[j] = None

                        # delete leftmost child and its child pointers
                        SmartPointer.put_attr(p, {'children': children}, delete_old=make_copies)
                        SmartPointer.delete(p)

            # delete subtree
            req_attr = ['children', 'type', 'is_leaf', 'ogE', 'dst_id']
            while sq.head is not None:
                p = queue.dequeue(sq)
                if p is None:
                    continue 
                
                # load v attributes
                v_attr = SmartPointer.get_attr(p, req_attr)
                assert v_attr['type'] == 'OUT_EDGE'
                self.out_tree_accesses += 1
                degree = 2 if SmartPointer.is_temp_copy(p) else 1
                self.out_tree_degree += degree
                children = v_attr['children']

                # at internal OutEdge
                if not v_attr['is_leaf']:
                    assert v_attr['ogE'] is None and v_attr['dst_id'] is None
                    for i in range(self.branching_factor):
                        if children[i] is not None:
                            queue.enqueue(sq, children[i])
                            children[i] = None
                        else:
                            break

                set_attr = {'children': children, 'ogE': None, 'dst_id': None, 'weight': 0}
                SmartPointer.put_attr(p, set_attr, delete_old=make_copies)
                SmartPointer.delete(p)

        # update ogV
        set_attr = {'out_children': out_children, 'out_degree': new_out_degree, 'height': new_height}
        SmartPointer.put_attr(p_src, set_attr, delete_old=make_copies)

    def get_random_neighbor_full_copy(self, p_src): 
        if p_src is None:
            return None, None

        # return random dst_id if possible
        dst_id = ogE = None

        # load ogV and compute stats
        p_src, ogV = self.get_ogV_full_copy(p_src)
        out_degree = new_out_degree = ogV.out_degree

        # ensure src Vertex has outgoing edges
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
                        SmartPointer.delete_copy(p)
                        continue
                    
                    # load v
                    v = SmartPointer.get_and_copy(p)
                    assert v.type == 'OUT_EDGE'
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
                        SmartPointer.delete_copy(p)

                    trials += 1

        SmartPointer.delete(ogV)

        # rearrange and resize OutEdge tree
        if new_out_degree < out_degree:
            new_out_degree = self.reorder_outgoing_edge_tree_full_copy(p_src, new_out_degree)
            self.downsize_outgoing_tree_full_copy(p_src, new_out_degree)

        # delete entry point if desired
        if SmartPointer.is_temp_copy(p_src): self.ogV_counts[ogV.id] -= 1
        SmartPointer.delete_copy(p_src)
    
        return dst_id, ogE

    def get_random_neighbor(self, p_src, make_copies=True):
        if p_src is None:
            return None, None

        # return random dst_id if possible
        dst_id = ogE = None
        
        # load ogV attributes
        req_attr = ['out_degree', 'type', 'id', 'height']
        p_src, v_attr = self.get_ogV_attr(p_src, req_attr, make_copies) 
        src_id = v_attr['id']
        out_degree = new_out_degree = v_attr['out_degree']
        height = v_attr['height']

        # ensure src Vertex has outgoing edges
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
                    child_term = 'out_children' if i == 0 else 'children'
                    sl = sublist(child_term, [idx])
                    next_child = SmartPointer.get_attr(p, sl, make_copies)[child_term][0]

                    if i == 0:
                        self.ogV_depth.append(get_global_counter()[0] - pre_alloc_count)
                        self.ogV_degree += self.ogV_counts[src_id]
                        self.ogV_accesses += 1

                    else:
                        SmartPointer.delete_copy(p)
                        self.out_tree_accesses += 1
                        degree = 2 if SmartPointer.is_temp_copy(p) else 1
                        self.out_tree_degree += degree

                    i += 1
                    p = next_child

                # get ogE
                req_attr = ['type', 'is_leaf', 'dst_id', 'ogE']
                v_attr = SmartPointer.get_attr(p, req_attr, make_copies)
                assert v_attr['is_leaf'] and v_attr['type'] == 'OUT_EDGE'
                dst_id = v_attr['dst_id']
                ogE = v_attr['ogE']

                # compute OutEdge tree accesses
                self.out_tree_accesses += 1
                degree = 2 if SmartPointer.is_temp_copy(p) else 1
                self.out_tree_degree += degree

                # ensure item is not deleted
                if ogE is not None and self.num_deleted_objs > 0:
                    p, ogE, dst_id, new_out_degree = self.process_deleted_objects(p, ogE, dst_id, new_out_degree, make_copies)

                # compute OutEdge tree accesses
                SmartPointer.delete_copy(p)
                if SmartPointer.is_temp_copy(ogE): self.ogV_counts[dst_id] += 1

        # rearrange and resize OutEdge tree
        if new_out_degree < out_degree:
            new_out_degree = self.reorder_outgoing_edge_tree(p_src, new_out_degree, make_copies)
            self.downsize_outgoing_tree(p_src, new_out_degree, make_copies)

        if SmartPointer.is_temp_copy(p_src): self.ogV_counts[src_id] -= 1
        SmartPointer.delete_copy(p_src)

        return dst_id, ogE
  
    def bfs_full_copy(self, src_name):
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
        sq = self.add_neighbors_full_copy(p_src, ogV, 'QUEUE', sq, "ID", None)

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
                sq = self.add_neighbors_full_copy(p, v, 'QUEUE', sq, "ID", None)
            else:
                SmartPointer.delete(v)
                SmartPointer.delete_copy(p)
                self.ogV_counts[v.id] -= 1

        self._unvisit_full_copy(visited)

        return path

    def bfs(self, src_name, make_copies=True):
        src_id, p_src = self.get_pointer(src_name)
        path = [src_id]
        
        # initialize queue
        sq = queue.init_queue()

        # initialize queue to collect visited ogVs
        visited = queue.init_queue()
        
        # mark ogV as visited
        p_src, src_id = self.visit(p_src, None)
        
        # queue neighbors for visiting
        sq = self.add_neighbors(p_src, 'QUEUE', sq, "ID", None, make_copies)
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
            req_attr = ['visited', 'type', 'id']
            p, v_attr = self.get_ogV_attr(p, req_attr, make_copies)
            if p is not None and not v_attr['visited']:
                p, src_id = self.visit(p, label)
                path.append(src_id)
                sq = self.add_neighbors(p, 'QUEUE', sq, "ID", None, make_copies)
                queue.enqueue(visited, (p, src_id))
            else:
                SmartPointer.delete_copy(p)
                if SmartPointer.is_temp_copy(p): self.ogV_counts[v_attr['id']] -= 1

        self._unvisit(visited, make_copies)
        return path

    def dfs_full_copy(self, src_name):
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
        top = self.add_neighbors_full_copy(p_src, ogV, 'STACK', top, "ID", None)

        # process OutEdges for visiting
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
                top = self.add_neighbors_full_copy(p, v, 'STACK', top, "ID", None)
            else:
                SmartPointer.delete(v)
                self.ogV_counts[v.id] -= 1
                SmartPointer.delete_copy(p)

        self._unvisit_full_copy(visited)
        return path

    def dfs(self, src_name, make_copies=True):
        src_id, p_src = self.get_pointer(src_name)
        path = [src_id]

        # initialize stack
        top = stack.init_stack()

        # initialize queue to collect visited ogVs
        visited = queue.init_queue()
        
        # mark ogV as visited
        p_src, src_id = self.visit(p_src, None)

        # stack neighbors for visiting
        top = self.add_neighbors(p_src, 'STACK', top, "ID", None, make_copies)
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
            req_attr = ['visited', 'type', 'id']
            p, v_attr = self.get_ogV_attr(p, req_attr, make_copies)
            if p is not None and not v_attr['visited']:
                p, src_id = self.visit(p, label)
                path.append(src_id)
                top = self.add_neighbors(p, 'STACK', top, "ID", None, make_copies)
                queue.enqueue(visited, (p, src_id))
            else:
                SmartPointer.delete_copy(p)
                if SmartPointer.is_temp_copy(p): self.ogV_counts[v_attr['id']] -= 1

        self._unvisit(visited, make_copies)
        return path

    def dijkstra_full_copy(self, src_name):
        src_id, p_src = self.get_pointer(src_name)
        costs = {src_id : 0}
        edges = []

        # initialize vistited queue
        visited = queue.init_queue()
        
        # mark ogV as visited
        p_src, ogV = self.get_ogV_full_copy(p_src)
        ogV, src_id = self.visit_full_copy(p_src, ogV, 0)
        queue.enqueue(visited, p_src)

        # heap neighbors for visiting
        self.add_neighbors_full_copy(p_src, ogV, 'PQ', None, "LW", "LW")
        
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
                self.add_neighbors_full_copy(p, v, 'PQ', None, "LW", "LW")
            else:
                SmartPointer.delete(v)
                SmartPointer.delete_copy(p)
                self.ogV_counts[v.id] -= 1

        total = 0
        for cost in costs.values():
            total += cost

        self._unvisit_full_copy(visited)
        return total, costs, edges

    def dijkstra(self, src_name, make_copies):
        src_id, p_src = self.get_pointer(src_name)
        costs = {src_id : 0}
        edges = []

        # initialize vistited queue
        visited = queue.init_queue()
        
        # mark ogVs as visited
        p_src, src_id = self.visit(p_src, 0)

        # heap neighbors for visiting
        self.add_neighbors(p_src, 'PQ', None, "LW", "LW", make_copies)
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
            req_attr = ['visited', 'type', 'id']
            p, v_attr = self.get_ogV_attr(p, req_attr, make_copies)
            if p is not None and not v_attr['visited']:
                p, src_id = self.visit(p, label)
                queue.enqueue(visited, (p, src_id))
                costs[src_id] = weight
                edges.append((old_src_id, src_id, weight))
                self.add_neighbors(p, 'PQ', None, "LW", "LW", make_copies)
            else:
                SmartPointer.delete_copy(p)
                if SmartPointer.is_temp_copy(p): self.ogV_counts[v_attr['id']] -= 1

        total = 0
        for cost in costs.values():
            total += cost
        
        self._unvisit(visited, make_copies)
        return total, costs, edges

    def prim_full_copy(self, src_name):
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
        self.add_neighbors_full_copy(p_src, ogV, 'PQ', None, "ID", "W")
        
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
                self.add_neighbors_full_copy(p, v, 'PQ', None, "ID", "W")
            else:
                SmartPointer.delete(v)
                SmartPointer.delete_copy(p)
                self.ogV_counts[v.id] -= 1

        self._unvisit_full_copy(visited)
        return cost, edges

    def prim(self, src_name, make_copies=True):
        src_id, p_src = self.get_pointer(src_name)
        cost = 0
        edges = []

        # initialize vistited queue
        visited = queue.init_queue()
        
        # mark ogVs as visited
        p_src, src_id = self.visit(p_src, None)

        # heap neighbors for visiting
        self.add_neighbors(p_src, 'PQ', None, "ID", "W", make_copies)
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
            req_attr = ['visited', 'type', 'id']
            p, v_attr = self.get_ogV_attr(p, req_attr, make_copies)
            if p is not None and not v_attr['visited']:
                p, src_id = self.visit(p, label)
                queue.enqueue(visited, (p, src_id))
                cost += weight
                edges.append((old_src_id, src_id, weight))
                self.add_neighbors(p, 'PQ', None, "ID", "W", make_copies)
            else:
                SmartPointer.delete_copy(p)
                if SmartPointer.is_temp_copy(p): self.ogV_counts[v_attr['id']] -= 1
            
        self._unvisit(visited, make_copies)
        return cost, edges

    def random_walk_full_copy(self, src_name, walk_length):
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
        SmartPointer.delete_copy(p_src)
                
        return path

    def random_walk(self, src_name, walk_length, make_copies=True):
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
        SmartPointer.delete_copy(p_src)
                
        return path

    def contact_discovery_full_copy(self, src_name1, src_name2):
        src_id1, p_src1 = self.get_pointer(src_name1)
        p_src1, ogV1 = self.get_ogV_full_copy(p_src1)

        src_id2, p_src2 = self.get_pointer(src_name2)
        p_src2, ogV2 = self.get_ogV_full_copy(p_src2)

        # get neighbors of each
        src_id1_outgoing = self.add_neighbors_full_copy(p_src1, ogV1, 'SET', set(), None, None)
        src_id2_outgoing = self.add_neighbors_full_copy(p_src2, ogV2, 'SET', set(), None, None)

        # compute intersection
        shared = src_id1_outgoing.intersection(src_id2_outgoing)
        result = True if shared else False

        return result, shared

    def contact_discovery(self, src_name1, src_name2, make_copies=True):
        src_id1, p_src1 = self.get_pointer(src_name1)
        src_id2, p_src2 = self.get_pointer(src_name2)

        # get neighbors of each
        src_id1_outgoing = self.add_neighbors(p_src1, 'SET', set(), None, None, make_copies)
        src_id2_outgoing = self.add_neighbors(p_src2, 'SET', set(), None, None, make_copies)

        # compute intersection
        shared = src_id1_outgoing.intersection(src_id2_outgoing)
        result = True if shared else False

        return result, shared

    def directed_triangle_count_full_copy(self, src_name):
        # a triangle is defined as a cycle of three nodes (u, v, w)
        # with the edges u -> v, v -> w, and w -> u
        src_id, p_src = self.get_pointer(src_name)
        triangles = []

        # retrieve OutEdges entry points
        p_src, ogV = self.get_ogV_full_copy(p_src)
        outgoing1 = queue.init_queue()
        outgoing1 = self.add_neighbors_full_copy(p_src, ogV, 'QUEUE', outgoing1, None, None)

        # process each OutEdge pointer to an ogV
        while outgoing1.head is not None:
            # get out_id and p
            op1 = queue.dequeue(outgoing1)
            if op1 is None:
                continue

            out_id1, p1 = op1
            if p1 is None:
                continue

            # retrieve OutEdges entry points
            p1, ogV1 = self.get_ogV_full_copy(p1)
            outgoing2 = queue.init_queue()
            outgoing2 = self.add_neighbors_full_copy(p1, ogV1, 'QUEUE', outgoing2, None, None)
            SmartPointer.delete_copy(p1)

            # process each OutEdge pointer to an ogV
            while outgoing2.head is not None:
                # get out_id and p
                op2 = queue.dequeue(outgoing2)
                if op2 is None:
                    continue

                out_id2, p2 = op2
                if p2 is None:
                    continue

                # retrieve OutEdge ids
                p2, ogV2 = self.get_ogV_full_copy(p2)
                outgoing3 = self.add_neighbors_full_copy(p2, ogV2, 'SET', set(), None, None)
                SmartPointer.delete_copy(p2)

                # triangle is complete if src_id is found
                if src_id in outgoing3:
                    triangles.append((src_id, out_id1, out_id2))

        count = len(triangles)
        return count, triangles

    def directed_triangle_count(self, src_name, make_copies=True):
        # a triangle is defined as a cycle of three nodes (u, v, w)
        # with the edges u -> v, v -> w, and w -> u
        src_id, p_src = self.get_pointer(src_name)
        triangles = []

        # retrieve OutEdge entry points
        outgoing1 = queue.init_queue()
        outgoing1 = self.add_neighbors(p_src, 'QUEUE', outgoing1, None, None, make_copies)
        
        # process each OutEdge pointer to an ogV
        while outgoing1.head is not None:
            # get out_id and p
            op1 = queue.dequeue(outgoing1)
            if op1 is None:
                continue

            out_id1, p1 = op1
            if p1 is None:
                continue

            # retrieve OutEdges entry points
            # outgoing2 = self.get_out_edges(p1, collect_entry_points=True, make_copies=make_copies)
            outgoing2 = queue.init_queue()
            outgoing2 = self.add_neighbors(p1, 'QUEUE', outgoing2, None, None, make_copies)

            # process each OutEdge pointer to an ogV
            while outgoing2.head is not None:
                # get out_id and p
                op2 = queue.dequeue(outgoing2)
                if op2 is None:
                    continue

                out_id2, p2 = op2
                if p2 is None:
                    continue

                # retrieve OutEdge ids
                # outgoing3 = self.get_out_edges(p2, collect_entry_points=False, make_copies=make_copies)
                outgoing3 = self.add_neighbors(p2, 'SET', set(), None, None, make_copies)
                
                # triangle is complete if src_id is found
                if src_id in outgoing3:
                    triangles.append((src_id, out_id1, out_id2))

        count = len(triangles)
        return count, triangles

    def pagerank_full_copy(self, walk_length, damping_factor):
        visits = dict()
        p_src = src_id = None

        for i in range(walk_length):
            probability = random.random()

            # go to random neighbor of current node
            if probability <= damping_factor:
                src_id, p_src = self.get_random_neighbor_full_copy(p_src)

            # get new starting node if damping_factor permits or
            # get_random_neighbor could not traverse further
            if (src_id is None and p_src is None) or probability > damping_factor:
                if SmartPointer.is_temp_copy(p_src): self.ogV_counts[src_id] -= 1
                SmartPointer.delete_copy(p_src)
                p_src = src_id = None

                # loop until a valid pointer is found
                # with deletions, a number may be gone from the graph
                while p_src is None:
                    src_id = random.randint(0, self.n-1)
                    p_src = self.entry_points[src_id]

            try:
                visits[src_id] += 1
            except:
                visits[src_id] = 1

        if SmartPointer.is_temp_copy(p_src): self.ogV_counts[src_id] -= 1
        SmartPointer.delete_copy(p_src)

        ratios = dict()

        for v_id, count in visits.items():
            ratios[v_id] = count / walk_length

        # assert we traversed the entire walk_length
        count = 0
        for visit in visits.values():
            count += visit
        assert count == walk_length
                
        return visits, ratios

    def pagerank(self, walk_length, damping_factor, make_copies=True):
        visits = dict()
        p_src = src_id = None

        for i in range(walk_length):
            probability = random.random()

            # go to random neighbor of current node
            if probability <= damping_factor:
                src_id, p_src = self.get_random_neighbor(p_src, make_copies)

            # get new starting node if damping_factor permits or
            # get_random_neighbor could not traverse further
            if (src_id is None and p_src is None) or probability > damping_factor:
                if SmartPointer.is_temp_copy(p_src): self.ogV_counts[src_id] -= 1
                SmartPointer.delete_copy(p_src)
                p_src = src_id = None

                # loop until a valid pointer is found
                # with deletions, a number may be gone from the graph
                while p_src is None:
                    src_id = random.randint(0, self.n-1)
                    p_src = self.entry_points[src_id]
                
            try:
                visits[src_id] += 1
            except:
                visits[src_id] = 1

        # delete ending pointer
        if SmartPointer.is_temp_copy(p_src): self.ogV_counts[src_id] -= 1
        SmartPointer.delete_copy(p_src)

        ratios = dict()

        for v_id, count in visits.items():
            ratios[v_id] = count / walk_length

        # assert we traversed the entire walk_length
        count = 0
        for visit in visits.values():
            count += visit
        assert count == walk_length
                
        return visits, ratios

def build_from_edges(edges, name, branching_factor=2, static_insertion=True, move_semantics=True, 
                    make_copies=True, queue_osam=True, stack_osam=True, avl_tree_osam=True, random_seed=None,
                    sort_ids=True, bidirectional_edges=True, show_edges=False, ordered_dynamic_vertices=False):

    reset_write_batches()

    if random_seed is not None:
        random.seed(random_seed)
    
    # expects graph to be completely prepared in dictionary form
    print(f"\nGraph from Edges: {name}", flush=True)

    if show_edges:
        print("\nEdges:", flush=True)
        match edges:
            case list() | tuple() | set():
                if ordered_dynamic_vertices:
                    nodes = set()
                    for edge in edges:
                        nodes.add(edge[0])
                        nodes.add(edge[1])

                    for src_name in sorted(list(nodes)):
                        ograph.add_vertex(src_name, ograph.v_ids, ograph.entry_points)

                for edge in edges:
                    weight = 0
                    if len(edge) == 3:
                        src, dst, weight = edge
                    else:
                        src, dst = edge

                    if weight is not None:
                        print(f"{src}->{dst}, weight: {weight}", flush=True)
                    else:
                        print(f"{src}->{dst}", flush=True)

            case dict():
                for src, dsts in edges.items():
                    for dst in dsts:
                        weight = None
                        if type(dst) in [list, tuple] and len(dst) == 2:
                            dst, weight = dst
                        
                        if weight is not None:
                            print(f"{src}->{dst}, weight: {weight}", flush=True)
                        else:
                            print(f"{src}->{dst}", flush=True)

    ograph = OGraph(branching_factor=branching_factor, avl_tree_osam=avl_tree_osam)

    queue.queue_osam = queue_osam
    stack.stack_osam = stack_osam

    print("\nAllocs, Reads, Read/Rem, Writes", flush=True)
    print(f"Pre build stats {get_global_counter()}", flush=True) 

    if static_insertion:
        ograph.emulate_graph(edges=edges, move_semantics=move_semantics, sort_ids=sort_ids, bidirectional_edges=bidirectional_edges)
    else:
        match edges:
            case list() | tuple() | set():
                for edge in edges:
                    weight = 0
                    if len(edge) == 3:
                        src_name, dst_name, weight = edge
                    else:
                        src_name, dst_name = edge

                    if move_semantics:
                        ograph.add_edge(src_name, dst_name, weight, make_copies)
                    else:
                        ograph.add_edge_full_copy(src_name, dst_name, weight)

            case dict():
                for src_name, dst_names in edges.items():
                    for dst_name in dst_names:
                        weight = 0
                        if type(dst_name) in [list, tuple] and len(dst_name) == 2:
                            dst_name, weight = dst_name

                        if move_semantics:
                            ograph.add_edge(src_name, dst_name, weight, make_copies)
                        else:
                            ograph.add_edge_full_copy(src_name, dst_name, weight)

    print(f"Post build stats {get_global_counter()}", flush=True)
    print_high_level_operations(flush=True)
    print_max_write_batches(flush=True)

    return ograph

def build_from_networkx(graph_function, name, graph=None, args=None, branching_factor=2, static_insertion=True, 
                        move_semantics=True, make_copies=True, prime_graph=False, queue_osam=True, stack_osam=True, 
                        avl_tree_osam=True, connect_all=True, random_weight_min=None, random_weight_max=None, 
                        random_seed=None, sort_ids=False, ordered_dynamic_vertices=False, show_edges=False):

    reset_write_batches()

    if random_seed is not None:
        random.seed(random_seed)
    
    print(f"\nClass Graph {name}", flush=True)
        
    if graph is not None:
        assert type(graph) in [nx.Graph, nx.DiGraph]  
    elif args:
        graph = graph_function(*args)
    else:
        graph = graph_function()

    if connect_all and type(graph) is nx.Graph:
        components = list(nx.connected_components(graph))
        for i in range(len(components)-1):
            u = random.choice(list(components[i]))
            v = random.choice(list(components[i+1]))
            graph.add_edge(u, v)
    
    weights = nx.get_edge_attributes(graph, "weight")
    if not weights and type(random_weight_min) is int and type(random_weight_max) is int:
        for u, v in graph.edges:
            graph.add_edge(u, v, weight=random.randint(random_weight_min, random_weight_max))

    if show_edges:
        print("\nEdges:", flush=True)
        for src in graph:
            for dst in graph.neighbors(src):
                try:
                    weight = graph.edges[src_name, dst_name]['weight']
                except:
                    weight = 0

                if weight:
                    print(f"{src}->{dst}, weight: {weight}", flush=True)
                else:
                    print(f"{src}->{dst}", flush=True)

    ograph = OGraph(branching_factor=branching_factor, avl_tree_osam=avl_tree_osam)
    
    queue.queue_osam = queue_osam
    stack.stack_osam = stack_osam

    print("\nAllocs, Reads, Read/Rem, Writes", flush=True)
    print(f"Pre build stats {get_global_counter()}", flush=True) 
    
    if static_insertion:
        ograph.emulate_graph(graph=graph, move_semantics=move_semantics, sort_ids=sort_ids) 
    else:
        if ordered_dynamic_vertices:
            for src_name in graph.nodes:
                ograph.add_vertex(src_name, ograph.v_ids, ograph.entry_points)

        for src_name in graph.nodes:
            for dst_name in graph.neighbors(src_name):
                try:
                    weight = graph.edges[src_name, dst_name]['weight']
                except:
                    weight = 0

                if move_semantics:
                    ograph.add_edge(src_name, dst_name, weight, make_copies)
                else:
                    ograph.add_edge_full_copy(src_name, dst_name, weight)
                
    if prime_graph:
        for count in range(len(graph.nodes)//10):
            src = random.choice(list(graph.nodes))
            if move_semantics:
                ograph.random_walk(src, 50, make_copies)
            else: 
                ograph.random_walk_full_copy(src, 50)

    print(f"Post build stats {get_global_counter()}", flush=True)
    print_high_level_operations(flush=True)
    print_max_write_batches(flush=True)

    return ograph
        
def benchmark(ograph, name, trials=1, entry_point=0, function='rw', move_semantics=True, 
            make_copies=True, queue_osam=True, stack_osam=True, avl_tree_osam=True, walk_length=0,
            damping_factor=0.9, next_entry_point=1, print_trace=False, print_each_trial=False, 
            pick_random_entry_points=False):
    
    reset_write_batches()
    
    assert trials > 0
    
    queue.queue_osam = queue_osam
    stack.stack_osam = stack_osam
    ograph.v_ids.avl_tree_osam = avl_tree_osam
    ograph.entry_points.avl_tree_osam = avl_tree_osam

    ograph.reset_access_stats()
    
    print(f"\n--------------------------------------------------------------")
    print(f"Benchmarking Graph {name}", flush=True) 

    # print(f"Pre algorithm {function} stats {get_global_counter()}", flush=True) 
    # print_high_level_operations(flush=True)

    fn = function.lower()
    match fn:
        case 'dfs' | 'bfs' | 'dijkstra' | 'prim':
            pass
        case 'random walk' | 'randomwalk' | 'rw' | 'random_walk' | 'random-walk':
            fn = 'random walk'
        case 'contact discovery' | 'contactdiscovery' | 'cd' | 'contact_discovery' | 'contact-discovery':
            fn = 'contact discovery'
        case 'directed triangle count' | 'directedtrianglecount' | 'dtc' | 'directed_triangle_count' | 'directed-triangle-count':
            fn = 'directed triangle count'
        case 'page rank' | 'pagerank' | 'pr' | 'page_rank' | 'page-rank':
            fn = 'pagerank'
        case _:
            raise RuntimeError(f'OGraph has no function {function}')

    print(f"Running {fn} for {trials} trial(s)")
    traces = []
    for trial in range(trials):
        if print_each_trial: print(f"\nTrial {trial}")

        if pick_random_entry_points: 
            entry_point = random.randint(0, ograph.n-1)
            next_entry_point = entry_point

            # pick distinct entry points
            while ograph.n > 1 and entry_point == next_entry_point:
                next_entry_point = random.randint(0, ograph.n-1)

        match fn:
            case 'dfs':
                if print_each_trial: print(f"dfs on entry point {entry_point}", flush=True)
                trace = ograph.dfs_full_copy(entry_point) if not move_semantics else ograph.dfs(entry_point, make_copies)
            case 'bfs':
                if print_each_trial: print(f"bfs on entry point {entry_point}", flush=True)
                trace = ograph.bfs_full_copy(entry_point) if not move_semantics else ograph.bfs(entry_point, make_copies)
            case 'dijkstra':
                if print_each_trial: print(f"Dijkstra on entry point {entry_point}", flush=True)
                trace = ograph.dijkstra_full_copy(entry_point) if not move_semantics else ograph.dijkstra(entry_point, make_copies)
            case 'prim':
                if print_each_trial: print(f"Prim on entry point {entry_point}")
                trace = ograph.prim_full_copy(entry_point) if not move_semantics else ograph.prim(entry_point, make_copies)
            case 'random walk':
                if print_each_trial: print(f"Random Walk on entry point {entry_point} with walk length {walk_length}", flush=True)
                trace = ograph.random_walk_full_copy(entry_point, walk_length) if not move_semantics else ograph.random_walk(entry_point, walk_length, make_copies)
            case 'contact discovery':
                if print_each_trial: print(f"Contact Discovery on entry points ({entry_point}, {next_entry_point})", flush=True)
                trace = ograph.contact_discovery_full_copy(entry_point, next_entry_point) if not move_semantics else ograph.contact_discovery(entry_point, next_entry_point, make_copies)
            case 'directed triangle count':
                if print_each_trial: print(f"Directed Triangle Count on entry point {entry_point}", flush=True)
                trace = ograph.directed_triangle_count_full_copy(entry_point) if not move_semantics else ograph.directed_triangle_count(entry_point, make_copies)
            case 'pagerank':
                if print_each_trial: print(f"PageRank with damping_factor {damping_factor} and walk length {walk_length}", flush=True)
                trace = ograph.pagerank_full_copy(walk_length, damping_factor) if not move_semantics else ograph.pagerank(walk_length, damping_factor, make_copies)

        if print_trace: 
            print(trace, flush=True)

        traces.append(trace)

        if print_each_trial:
            print(f"Post trial {trial} {fn} stats {get_global_counter()}", flush=True)
            print_high_level_operations(flush=True)
    
    ograph.print_access_stats()

    print(f"\nPost algorithm {fn} stats {get_global_counter()}", flush=True)
    
    print_high_level_operations(flush=True)
    print_max_write_batches(flush=True)
    print_timing_stats(flush=True)

    return traces
        
if __name__ == "__main__":
    # test one pointer
    # g = karate_club_graph()
    # random.seed(1)
    # SmartPointer.set_smart_pointer('multiwrite')
    # name = 'Karate Club'
    # static_insertion = True
    # move_semantics = True
    # queue_osam = True
    # stack_osam = True
    # avl_tree_osam = True
    # make_copies = False
    # ograph = build_from_networkx(graph_function=karate_club_graph, name=name, move_semantics=move_semantics, static_insertion=static_insertion, branching_factor=3, queue_osam=queue_osam, stack_osam=stack_osam, avl_tree_osam=avl_tree_osam, ordered_dynamic_vertices=True)    
    # for i in ['bfs', 'dfs', 'prim', 'dijkstra', 'random walk', 'contact discovery', 'directed triangle count', 'pagerank']: 
    # # for i in ['bfs', 'dfs', 'prim', 'dijkstra']: 
    #     for j in range(1):
    #         traces = benchmark(ograph, name, trials=50, function=i, entry_point=j, 
    #         move_semantics=move_semantics, walk_length=50, next_entry_point=1, damping_factor=0.9, 
    #         queue_osam=queue_osam, stack_osam=stack_osam, avl_tree_osam=avl_tree_osam, 
    #         print_each_trial=False, print_trace=True, pick_random_entry_points=True,
    #         make_copies=make_copies)
    #         for k in range(34):
    #             # print(k, ograph.ogV_counts[k], len(list(g.neighbors(k))) +1)
    #             assert ograph.ogV_counts[k] == len(list(g.neighbors(k))) + 1

    # test all pointers
    # g = karate_club_graph()
    # random.random_seed(1)
    # name = 'Karate Club'
    # static_insertion = False
    # queue_osam = True
    # stack_osam = True
    # avl_tree_osam = True
    # prime_graph = True
    # # pointers = {'original': [True, False], 'multiwrite': [True], 'recursive': [True]}
    # pointers = {'original': [True]}
    # # pointers = {'multiwrite': [True]}
    # for pointer, move_semantics_plural in pointers.items():
    #     SmartPointer.set_smart_pointer(pointer)
    #     for move_semantics in move_semantics_plural:
    #         for make_copies in [True, False]:
    #             ograph = build_from_networkx(graph_function=karate_club_graph, name=name, move_semantics=move_semantics, static_insertion=static_insertion, 
    #             branching_factor=3, queue_osam=queue_osam, stack_osam=stack_osam, avl_tree_osam=avl_tree_osam, 
    #             prime_graph=prime_graph, ordered_dynamic_vertices=True, make_copies=make_copies)    
    #             for i in ['bfs', 'dfs', 'prim', 'dijkstra', 'random walk', 'contact discovery', 'directed triangle count', 'pagerank']:
    #             # for i in ['bfs', 'dfs', 'prim', 'dijkstra']:
    #             # for i in ['random walk', 'contact discovery', 'directed triangle count', 'pagerank']:
    #             # for i in ['prim']: 
    #                 for j in range(1):
    #                     traces = benchmark(ograph, name, trials=10, function=i, entry_point=j, 
    #                     move_semantics=move_semantics, walk_length=50, next_entry_point=1, damping_factor=0.9, 
    #                     queue_osam=queue_osam, stack_osam=stack_osam, avl_tree_osam=avl_tree_osam, 
    #                     print_each_trial=False, print_trace=True, pick_random_entry_points=True,
    #                     make_copies=make_copies)
    #                     for k in range(34):
    #                         # print(k, ograph.ogV_counts[k], len(list(g.neighbors(k))) + 1)
    #                         assert ograph.ogV_counts[k] == len(list(g.neighbors(k))) + 1

    # test edge deletion
    # g = karate_club_graph()
    # random_seed = 1
    # name = 'Karate Club'
    # queue_osam = True
    # stack_osam = True
    # avl_tree_osam = True
    # prime_graph = True
    # # pointers = {'original': [True, False], 'multiwrite': [True], 'recursive': [True]}
    # pointers = {'original': [False]}
    # a = [i for i in range(34)]
    # a.remove(0)
    # num_deleted_pointers = {0: len(list(g.neighbors(0)))} 
    # for pointer, move_semantics_plural in pointers.items():
    #     SmartPointer.set_smart_pointer(pointer)
    #     for move_semantics in move_semantics_plural:
    #         for static_insertion in [False, True]:
    #             for make_copies in [False, True]:
    #                 ograph = build_from_networkx(graph_function=karate_club_graph, name=name, move_semantics=move_semantics, static_insertion=static_insertion, 
    #                 branching_factor=3, queue_osam=queue_osam, stack_osam=stack_osam, avl_tree_osam=avl_tree_osam, 
    #                 prime_graph=prime_graph, ordered_dynamic_vertices=True, make_copies=make_copies, random_seed=random_seed)    
                    
    #                 # isolate vertex 26
    #                 if move_semantics:
    #                     for n in g.neighbors(0):
    #                         # if n == 31:
    #                         #     break
    #                         print(f"DELETING {0}->{n}")
    #                         ograph.delete_edge(0, n)     
    #                         print(f"DELETING {n}->{0}")
    #                         ograph.delete_edge(n, 0)        

    #                 else:
    #                     for n in g.neighbors(0):
    #                         print(f"DELETING {0}->{n}")
    #                         ograph.delete_edge_full_copy(0, n)     
    #                         print(f"DELETING {n}->{0}")
    #                         ograph.delete_edge_full_copy(n, 0)   

    #                 for i in ['bfs', 'dfs', 'prim', 'dijkstra', 'random walk', 'contact discovery', 'directed triangle count', 'pagerank']:
    #                     for j in a:
    #                     # for j in range(1):
    #                         traces = benchmark(ograph, name, trials=1, function=i, entry_point=j, 
    #                         move_semantics=move_semantics, walk_length=50, next_entry_point=1, damping_factor=0.9, 
    #                         queue_osam=queue_osam, stack_osam=stack_osam, avl_tree_osam=avl_tree_osam, 
    #                         print_each_trial=False, print_trace=True, pick_random_entry_points=False,
    #                         make_copies=make_copies)
    #                         for trace in traces:
    #                             match i:
    #                                 case 'bfs' | 'dfs' | 'random walk' | 'rw':
    #                                     assert 0 not in trace
    #                                 case 'dijkstra':
    #                                     assert 0 not in trace[1]
    #                                 case 'prim':
    #                                     for edge in trace[1]:
    #                                         assert edge[0] != 0 and edge[1] != 0
    #                                 case 'dtc' | 'directed triangle count':
    #                                     for triangle in trace[1]:
    #                                         assert 0 not in triangle
    #                                 case 'cd' | 'contact discovery':
    #                                     assert 0 not in trace[1]
    #                                 case _:
    #                                     pass

    # test deleting vertices
    random.seed(1)
    name = 'Karate Club'
    move_semantics = True
    queue_osam = True
    stack_osam = True
    avl_tree_osam = True
    outer_functions = ['bfs', 'dfs', 'prim', 'dijkstra'] #, 'cd', 'dtc', 'pagerank', 'rw']
    
    # pointers = {'original': [False]}
    pointers = {'original': [True, False], 'multiwrite': [True], 'recursive': [True]}
    for pointer, move_semantics_plural in pointers.items():
        SmartPointer.set_smart_pointer(pointer)
        for move_semantics in move_semantics_plural:
            for make_copies in [False]:
                for static_insertion in [False]:
                    counter = 0
                    while counter < 2:
                        for fn in outer_functions:
                            ograph = build_from_networkx(graph_function=karate_club_graph, name=name, 
                            make_copies=make_copies, move_semantics=move_semantics, static_insertion=static_insertion,
                            branching_factor=3, queue_osam=queue_osam, stack_osam=stack_osam, 
                            avl_tree_osam=avl_tree_osam, ordered_dynamic_vertices=True)    

                            nodes = [node for node in range(34)]

                            # present_nodes = [0, 3, 4, 5, 9, 11, 13, 14, 16, 23, 24, 25, 26, 27, 29, 31, 33]
                            # deleted_nodes = [1, 2, 6, 7, 8, 10, 12, 15, 17, 18, 19, 20, 21, 22, 28, 30, 32]
                            for node in range(34):
                                if random.random() > 0.5:
                                # if node in deleted_nodes:
                                    nodes.remove(node)
                                    print("\nDELETING", node)

                                    if move_semantics:
                                        ograph.delete_vertex(node)
                                    else:
                                        ograph.delete_vertex_full_copy(node)

                            print("\nOUTER FUNCTION", fn, counter)
                            print("PRESENT NODES", nodes, len(nodes))
                            # print("DELETED NODES", deleted_nodes, len(deleted_nodes))
                            print("NUM DELETED OBJECTS BEFORE", ograph.num_deleted_objs)

                            for node in nodes:
                                traces = benchmark(ograph, name, trials=1, function=fn, entry_point=node, 
                                move_semantics=move_semantics, walk_length=50, next_entry_point=random.choice(nodes), damping_factor=0.9, 
                                queue_osam=queue_osam, stack_osam=stack_osam, avl_tree_osam=avl_tree_osam, 
                                print_each_trial=False, print_trace=False, pick_random_entry_points=False,
                                make_copies=make_copies)

                            # node = random.choice(nodes)
                            # traces = benchmark(ograph, name, trials=2, function=fn, entry_point=node, 
                            # move_semantics=move_semantics, walk_length=50, next_entry_point=random.choice(nodes), damping_factor=0.9, 
                            # queue_osam=queue_osam, stack_osam=stack_osam, avl_tree_osam=avl_tree_osam, 
                            # print_each_trial=False, print_trace=False, pick_random_entry_points=False,
                            # make_copies=make_copies)

                            # print(traces[0])

                            print("NUM DELETED OBJECTS AFTER", ograph.num_deleted_objs)
                            print(ograph.ogV_counts)

                            for i in ['random walk', 'pagerank', 'contact discovery', 'directed triangle count', 'bfs', 'dfs', 'prim', 'dijkstra']: 
                            # for i in ['bfs', 'dfs', 'prim', 'dijkstra', 'random walk', 'contact discovery', 'directed triangle count', 'pagerank']: 
                                for j in nodes:
                                    traces = benchmark(ograph, name, trials=2, function=i, entry_point=j, 
                                    move_semantics=move_semantics, walk_length=50, next_entry_point=random.choice(nodes), damping_factor=0.9, 
                                    queue_osam=queue_osam, stack_osam=stack_osam, avl_tree_osam=avl_tree_osam, 
                                    print_each_trial=False, print_trace=False, pick_random_entry_points=False,
                                    make_copies=make_copies)
                                
                            assert ograph.num_deleted_objs == 0
                            
                        counter += 1

sys.modules['m'] = sys.modules[__name__]
Vertex.__module__ = 'm'
OutEdge.__module__ = 'm'
DeletedObject.__module__ = 'm'
