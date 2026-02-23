from single_access_machine import alloc, read, write, get_global_counter, set_max_reads, set_max_writes
import copy
import random
from sys import maxsize
from sublist import sublist

class RecursivePointer:
    label = 'recursive'
    pointer_osam = True
    structure = "RecursivePointer"
    reference_counts = dict()
    
    def __init__(self, location):
        if type(location) == int or location is None:
            self.head = location
        elif type(location) == RecursivePointer:
            self.head = location.head
        else:
            raise ValueError("Invalid type for RecursivePointer initialization: {}".format(type(location)))

    def __str__(self):
        return str(self.head) if self.head is not None else "None"

    @staticmethod
    def put_attr(recursive_pointer, attributes, delete_old=False):
        n = RecursivePointer.get(recursive_pointer)
        for attr in attributes:
            if delete_old and hasattr(n, attr):
                RecursivePointer.delete(getattr(n, attr))
            setattr(n, attr, attributes[attr])
        write(recursive_pointer.head, n, RecursivePointer.structure)

    @staticmethod
    def get_attr(recursive_pointer, attributes, make_copies=True):
        n = RecursivePointer.get(recursive_pointer)
        if type(attributes) == sublist:
            content = {}
            if not hasattr(n, attributes.attribute):
                content[attributes.attribute] = None
            else:
                lst = getattr(n, attributes.attribute)
                sublst = []
                for pos in attributes.positions:
                    if make_copies:
                        sublst.append(RecursivePointer.copy(lst[pos], temp_copy=make_copies))
                    else:
                        sublst.append(lst[pos])
                content[attributes.attribute] = sublst
        elif type(attributes) == list:
            content = {}
            for attr in attributes:
                if not hasattr(n, attr):
                    content[attr] = None
                else:
                    if make_copies:
                        content[attr] = RecursivePointer.copy(getattr(n, attr), temp_copy=make_copies)
                    else:
                        content[attr] = getattr(n, attr)
        elif type(attributes) == str:
            if make_copies:
                content = RecursivePointer.copy(getattr(n, attributes), temp_copy=make_copies)
            else:
                content = getattr(n, attributes)
        else:
            raise TypeError("Attributes must be a list, sublist, or a string")
            
        return content

    @staticmethod
    def get_count(recursive_pointer):
        return RecursivePointer.reference_counts[recursive_pointer.head]

    @staticmethod
    def __smart_copy_pointer(original):
        if original.head is None:
            return RecursivePointer(None)
        
        RecursivePointer.reference_counts[original.head] += 1
        return RecursivePointer(original)

    @staticmethod
    def copy(v1, temp_copy=False):
        if v1 is None:
            return None
        if type(v1) in [float, int, str, bytes, bool]:
            return copy.deepcopy(v1)
        elif type(v1) == RecursivePointer:
            return RecursivePointer.__smart_copy_pointer(v1)
        elif type(v1) == list:
            v0 = []
            for i in range(len(v1)):
                v0.append(RecursivePointer.copy(v1[i], temp_copy))
            return v0
        elif type(v1) == tuple:
            v0 = []
            for i in range(len(v1)):
                v0.append(RecursivePointer.copy(v1[i], temp_copy))
            return tuple(v0)
        elif type(v1) == dict:
            v0 = dict()
            for key, value in v1.items():
                v0[RecursivePointer.copy(key)] = RecursivePointer.copy(value, temp_copy)
            return v0
        else:
            return v1.smart_copy(v1, temp_copy)

    @staticmethod
    def smart_copy(recursive_pointer):
        return RecursivePointer.copy(recursive_pointer)
        
    @staticmethod
    def new(value = None):
        addr = alloc(RecursivePointer.pointer_osam, RecursivePointer.structure)
        write(addr, value, RecursivePointer.structure)
        
        if addr in RecursivePointer.reference_counts:
            RecursivePointer.reference_counts[addr] += 1
        else:
            RecursivePointer.reference_counts[addr] = 1
        
        return addr
    
    @staticmethod 
    def get_and_copy(recursive_pointer):
        n = RecursivePointer.get(recursive_pointer)
        nCopy = RecursivePointer.copy(n)
        return nCopy
    
    @staticmethod
    def get(recursive_pointer):
        return read(recursive_pointer.head, RecursivePointer.structure)
    
    @staticmethod
    def delete(recursive_pointer):
        if type(recursive_pointer) in [str, float, int, bytearray, bool]:
            return 
        elif type(recursive_pointer) in [list, tuple, set, frozenset]:
            for p in recursive_pointer:
                RecursivePointer.delete(p)
            return
        elif hasattr(recursive_pointer, "smart_delete"):
            recursive_pointer.smart_delete()
            return 
        elif recursive_pointer is None:
            return 

        assert type(recursive_pointer) is RecursivePointer
        if recursive_pointer.head is not None:
            RecursivePointer.reference_counts[recursive_pointer.head] -= 1
            recursive_pointer.head = None

    @staticmethod
    def put(recursive_pointer, value, delete_old=True):
        if delete_old:
            n = RecursivePointer.get(recursive_pointer)
            RecursivePointer.delete(n)
        write(recursive_pointer.head, RecursivePointer.copy(value), RecursivePointer.structure)

    @staticmethod
    def is_single_reference(recursive_pointer):
        if recursive_pointer is not None and recursive_pointer.head is not None:
            return RecursivePointer.reference_counts[recursive_pointer.head] == 1

def smart_pointer_test_3():
    animal_list = ["cow", "cat", "dog", "horse", "sheep", "pig", "goat", "chicken"]
    smart_pointer_list = []
    for animal in animal_list:
        sp = RecursivePointer(RecursivePointer.new(animal))
        smart_pointer_list.append(sp)
    print("Loaded smart pointers with animals:",get_global_counter())

    pointer_to_pointer = RecursivePointer(RecursivePointer.new(smart_pointer_list))
    print("Loaded pointer to pointers:",get_global_counter())
    list_of_ptrs= RecursivePointer.get(pointer_to_pointer)
    i=0
    for ptr in list_of_ptrs:
        assert(RecursivePointer.get(ptr)==animal_list[i])
        i += 1
    pointer = RecursivePointer.get_and_copy(pointer_to_pointer)
    
    list_of_ptrs= RecursivePointer.get(pointer)
    i=0
    for ptr in list_of_ptrs:
        assert(RecursivePointer.get(ptr)==animal_list[i])
        i += 1

    print("Test 3 completed successfully with animals:",get_global_counter())


        
def smart_pointer_test_1():
    num_ops = get_global_counter()

    p1 = RecursivePointer(RecursivePointer.new("cat"))
    print("Values initial no cp,",p1.head)
    p2 = RecursivePointer.copy(p1)
    print("First copy,", get_global_counter())
    num_ops = get_global_counter()
    # print("Post copy values ",p1.head, p2.head, p3.head)
    print(RecursivePointer.get(p1)=="cat")

    assert(RecursivePointer.get(p1)=="cat")
    print("First get,", get_global_counter())
    #Check if you can get the same value twice
    assert(RecursivePointer.get(p1)=="cat")
    print("Second get,", get_global_counter())
    num_ops = get_global_counter()
    p3 = RecursivePointer.copy(p1)
    print("Second copy,", get_global_counter())
    num_ops = get_global_counter()
    assert(RecursivePointer.get(p1)=="cat")
    print("Third get,", get_global_counter())
    num_ops = get_global_counter()
    RecursivePointer.put(p1, "dog")
    print("Put,", get_global_counter())
    num_ops = get_global_counter()
    for i in range(10):
        assert(RecursivePointer.get(p1)=="dog")
        print("Get,", get_global_counter())
        num_ops = get_global_counter()
        assert(RecursivePointer.get(p2)=="dog")
        print("Get,", get_global_counter())
        num_ops = get_global_counter()
    p4 = RecursivePointer(RecursivePointer.new(p1.get(p2)))
    assert(RecursivePointer.get(p4)=="dog")
    print("Get three copies,", get_global_counter())
    RecursivePointer.put(p1, "horse")
    assert(RecursivePointer.get(p2)=="horse")
    print("Get three copies,", get_global_counter())
    assert(RecursivePointer.get(p4)=="dog")
    print("Get p4 unrelated,", get_global_counter())
    RecursivePointer.delete(p3)
    print("Get post delete p3,", get_global_counter())
    assert(RecursivePointer.get(p2)=="horse")
    print("Get two copies,", get_global_counter())
    RecursivePointer.delete(p2)
    print("Get post delete p2,", get_global_counter())
    assert(RecursivePointer.get(p1)=="horse")
    print("Get post delete p2,", get_global_counter())
    RecursivePointer.delete(p1)
    RecursivePointer.delete(p4)
    print("Test 1 completed successfully.")

if __name__ == "__main__":
    print("Allocs, Reads, Read/Rem, Cache Hits, Misses, Writes, Evicts, Flushes, Time")
    set_max_reads(maxsize)
    set_max_writes(maxsize)
    smart_pointer_test_1()
    smart_pointer_test_3()

