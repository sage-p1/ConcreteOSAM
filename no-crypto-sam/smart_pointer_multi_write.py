from single_access_machine import alloc, read, write, get_global_counter, set_max_reads, set_max_writes
import copy
import random
import traceback
from sys import maxsize
from sublist import sublist

class SmartPointerMultiWrite:
    label = 'multiwrite'
    pointer_osam = True
    structure = "SmartPointerMultiWrite"

    def __init__(self, head):
        if type(head) == int or head is None:
            self.head = head
        elif type(head) == SmartPointerMultiWrite:
            (lnew, rnew) = SmartPointerMultiWrite.copy_raw_address(head.head)
            self.head = lnew
            head.head = rnew
        else:
            raise ValueError("Invalid type for SmartPointerMultiWrite initialization: {}".format(type(head)))

    def __str__(self):
        return str(self.head) if self.head is not None else "None"

    @staticmethod
    def link(left, right, parent):
        write(left, ["TREE", parent, right], SmartPointerMultiWrite.structure)
        write(right, ["TREE", parent, left], SmartPointerMultiWrite.structure)

    @staticmethod
    def put_attr(smart_pointer, attributes, delete_old=False):
        new_address, root, value = SmartPointerMultiWrite.deref(smart_pointer.head)
        assert (value[1] is None)
        n = value[2]
        for attr in attributes:
            if delete_old and hasattr(n, attr):
                SmartPointerMultiWrite.delete(getattr(n, attr))
            setattr(n, attr, attributes[attr])
        smart_pointer.head = new_address
        write(root, ["ROOT", None, n], SmartPointerMultiWrite.structure)


    @staticmethod
    def is_single_reference(smart_pointer):
        new_pointer, root, value = SmartPointerMultiWrite.deref(smart_pointer.head)
        write(root, value)
        smart_pointer.head = new_pointer
        return new_pointer == root
        
        
    @staticmethod
    def get_attr(smart_pointer, attributes, make_copies=True):
        n = SmartPointerMultiWrite.get(smart_pointer)

        if type(attributes) == sublist:
            content = {}
            if not hasattr(n, attributes.attribute):
                content[attributes.attribute] = None
            else:
                lst = getattr(n, attributes.attribute)
                sublst = []
                for pos in attributes.positions:
                    if make_copies:
                        sublst.append(SmartPointerMultiWrite.copy(lst[pos], temp_copy=make_copies))
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
                        content[attr] = SmartPointerMultiWrite.copy(getattr(n, attr), temp_copy=make_copies)
                    else:
                        content[attr] = getattr(n, attr)
        elif type(attributes) == str:
            if make_copies:
                content = SmartPointerMultiWrite.copy(getattr(n, attributes), temp_copy=make_copies)
            else:
                content = getattr(n, attributes)
        else:
            raise TypeError("Attributes must be a list, a string, or a sublist instance.")
        return content

    @staticmethod
    def get_count(smart_pointer):
        return 1
        
    @staticmethod
    def __smart_copy_pointer(original):
        if original.head is None:
            return SmartPointerMultiWrite(None)
        lnew, rnew = SmartPointerMultiWrite.copy_raw_address(original.head)
        original.head = rnew
        newPointer = SmartPointerMultiWrite(lnew)
        return newPointer

    @staticmethod
    def copy_raw_address(new_parent):
        lnew = alloc(SmartPointerMultiWrite.pointer_osam, SmartPointerMultiWrite.structure)
        rnew = alloc(SmartPointerMultiWrite.pointer_osam, SmartPointerMultiWrite.structure)
        SmartPointerMultiWrite.link(lnew, rnew, new_parent)
        return lnew, rnew
    
    @staticmethod
    def copy(v1, temp_copy=False):
        if v1 is None:
            return None
        if type(v1) in [float, int, str, bytes, bool, frozenset]:
            return copy.deepcopy(v1)
        elif type(v1) == SmartPointerMultiWrite:
            return SmartPointerMultiWrite.__smart_copy_pointer(v1)
        elif type(v1) == list:
            v0 = []
            for i in range(len(v1)):
                v0.append(SmartPointerMultiWrite.copy(v1[i], temp_copy))
            return v0
        elif type(v1) == tuple:
            v0 = []
            for i in range(len(v1)):
                v0.append(SmartPointerMultiWrite.copy(v1[i], temp_copy))
            return tuple(v0)
        elif type(v1) == dict:
            v0 = dict()
            for key, value in v1.items():
                v0[SmartPointerMultiWrite.copy(key, temp_copy)] = SmartPointerMultiWrite.copy(value, temp_copy)
            return v0
        else:
            return v1.smart_copy(v1, temp_copy)

    @staticmethod
    def smart_copy(smart_pointer):
        return SmartPointerMultiWrite.copy(smart_pointer)
        
    @staticmethod
    def new(value = None):
        root = alloc(SmartPointerMultiWrite.pointer_osam, SmartPointerMultiWrite.structure)
        write(root, ["ROOT", None, value], SmartPointerMultiWrite.structure)
        return root
    
    @staticmethod 
    def get_and_copy(smart_pointer):
        n = SmartPointerMultiWrite.get(smart_pointer)
        nCopy = SmartPointerMultiWrite.copy(n)
        return nCopy # newPointer
    
    @staticmethod
    def get(smart_pointer):
        new_address, root, value = SmartPointerMultiWrite.deref(smart_pointer.head)
        assert (value[1] is None)
        smart_pointer.head = new_address
        write(root, ["ROOT", None, value[2]], SmartPointerMultiWrite.structure)
        return value[2]

    @staticmethod
    def deref(p):
        [t, p_val, s] = read(p, SmartPointerMultiWrite.structure)
        p_new = alloc(SmartPointerMultiWrite.pointer_osam, SmartPointerMultiWrite.structure)
        if t == "ROOT":
            return (p_new, p_new, (t, p_val, s))
        elif t == "TREE":
            new_root, value = SmartPointerMultiWrite.splay(p_new, s, p_val)
            return (p_new, new_root, value)
        else:
            raise ValueError("Invalid address dereference")
        
    
    
    @staticmethod
    def delete(smart_pointer):
        if type(smart_pointer) in [str, float, int, bytearray, bool]:
            return 
        elif type(smart_pointer) in [list, tuple, set, frozenset]:
            for p in smart_pointer:
                SmartPointerMultiWrite.delete(p)
            return
        elif hasattr(smart_pointer, "smart_delete"):
            smart_pointer.smart_delete()
            return 
        elif smart_pointer is None:
            return 

        assert type(smart_pointer) is SmartPointerMultiWrite
        if smart_pointer.head is None:
            return
        t, p, s = read(smart_pointer.head, SmartPointerMultiWrite.structure)
        if t == "TREE":
            t, p2, s2 = read(p, SmartPointerMultiWrite.structure)  
            if t == "ROOT":
                write(s, [t, p2, s2], SmartPointerMultiWrite.structure)
            else:
                SmartPointerMultiWrite.link(s, s2, p2)
        smart_pointer.head = None
   
    @staticmethod
    def splay(a, b, x):
        while True:
            tx, y, c = read(x, SmartPointerMultiWrite.structure)
            if tx == "ROOT":
                x_ = alloc(SmartPointerMultiWrite.pointer_osam, SmartPointerMultiWrite.structure)
                SmartPointerMultiWrite.link(a, b, x_)
                return (x_, (tx, y, c))
            elif tx == "TREE":
                ty, z, d = read(y, SmartPointerMultiWrite.structure)
                if ty == "ROOT":
                    x_ = alloc(SmartPointerMultiWrite.pointer_osam, SmartPointerMultiWrite.structure)
                    y_ = alloc(SmartPointerMultiWrite.pointer_osam, SmartPointerMultiWrite.structure)
                    SmartPointerMultiWrite.link(b, c, y_)
                    SmartPointerMultiWrite.link(a, y_, x_)
                    return (x_, (ty, z, d))
                elif ty == "TREE":
                    x_ = z
                    y_ = alloc(SmartPointerMultiWrite.pointer_osam, SmartPointerMultiWrite.structure)
                    z_ = alloc(SmartPointerMultiWrite.pointer_osam, SmartPointerMultiWrite.structure)
                    SmartPointerMultiWrite.link(a, b, y_)
                    SmartPointerMultiWrite.link(c, d, z_)
                    a = y_
                    b = z_
                    x = x_
                else:
                    raise ValueError("Invalid address dereference")
            else:
                raise ValueError("Invalid address dereference")

    @staticmethod
    def put(smart_pointer, value, delete_old=True):
        p, root, sp_value = SmartPointerMultiWrite.deref(smart_pointer.head)
        assert(sp_value[1] is None)
        smart_pointer.head = p

        # save a copy with put
        write(root, ["ROOT", None, SmartPointerMultiWrite.copy(value)], SmartPointerMultiWrite.structure)
        
        if delete_old:
            SmartPointerMultiWrite.delete(sp_value[2])

def smart_pointer_test_1():
    num_ops = get_global_counter()

    p1 = SmartPointerMultiWrite(SmartPointerMultiWrite.new("cat"))
    assert(SmartPointerMultiWrite.is_single_reference(p1)==True)
    p2 = SmartPointerMultiWrite.copy(p1)
    print("First copy,", get_global_counter())
    num_ops = get_global_counter()
    # print("Post copy values ",p1.head, p2.head, p3.head)
    assert(SmartPointerMultiWrite.get(p1)=="cat")
    assert(SmartPointerMultiWrite.is_single_reference(p1)==False)
    print("First get,", get_global_counter())
    #Check if you can get the same value twice
    assert(SmartPointerMultiWrite.get(p1)=="cat")
    print("Second get,", get_global_counter())
    num_ops = get_global_counter()
    p3 = SmartPointerMultiWrite.copy(p1)
    assert(SmartPointerMultiWrite.is_single_reference(p1)==False)
    print("Second copy,", get_global_counter())
    num_ops = get_global_counter()
    assert(SmartPointerMultiWrite.get(p1)=="cat")
    print("Third get,", get_global_counter())
    num_ops = get_global_counter()
    SmartPointerMultiWrite.put(p1, "dog")
    print("Put,", get_global_counter())
    num_ops = get_global_counter()
    for i in range(10):
        assert(SmartPointerMultiWrite.get(p1)=="dog")
        print("Get,", get_global_counter())
        num_ops = get_global_counter()
        assert(SmartPointerMultiWrite.get(p2)=="dog")
        print("Get,", get_global_counter())
        num_ops = get_global_counter()
    
    p4 = SmartPointerMultiWrite(SmartPointerMultiWrite.new(p1.get(p2)))
    assert(SmartPointerMultiWrite.is_single_reference(p4)==True)
    assert(SmartPointerMultiWrite.get(p4)=="dog")
    print("Get three copies,", get_global_counter())
    SmartPointerMultiWrite.put(p1, "horse")
    assert(SmartPointerMultiWrite.get(p2)=="horse")
    print("Get three copies,", get_global_counter())
    assert(SmartPointerMultiWrite.get(p4)=="dog")
    print("Get p4 unrelated,", get_global_counter())
    SmartPointerMultiWrite.delete(p3)
    print("Get post delete p3,", get_global_counter())
    assert(SmartPointerMultiWrite.get(p2)=="horse")
    print("Get two copies,", get_global_counter())
    SmartPointerMultiWrite.delete(p2)
    print("Get post delete p2,", get_global_counter())
    assert(SmartPointerMultiWrite.get(p1)=="horse")
    print("Get post delete p2,", get_global_counter())
    assert(SmartPointerMultiWrite.is_single_reference(p1)==True)
    assert(SmartPointerMultiWrite.is_single_reference(p4)==True)
    SmartPointerMultiWrite.delete(p1)
    SmartPointerMultiWrite.delete(p4)
    print("Test 1 completed successfully.")
    
def smart_pointer_test_2():
    for lg_num_pointers in range(1, 5):
        num_pointers = 2**lg_num_pointers
        print("testing with ",num_pointers)

        pointers = [None] * num_pointers
        pointers[0] = SmartPointerMultiWrite.new("cat")
        
        for i in range(1, num_pointers):
            p, q = SmartPointerMultiWrite.copy_raw_address(pointers[i - 1])
            pointers[i] = p
            pointers[i - 1] = q

        # random.shuffle(pointers)

        for i in range(num_pointers):
            new_pointer, root, value = SmartPointerMultiWrite.deref(pointers[i])
            
            pointers[i] = new_pointer
            write(root, ["ROOT", None, value], SmartPointerMultiWrite.structure)

        for i in range(num_pointers):
            new_pointer, root, value = SmartPointerMultiWrite.deref(pointers[i])
            
            pointers[i] = new_pointer
            write(root, ["ROOT", None, value], SmartPointerMultiWrite.structure)

        print("After ",num_pointers, " the global counter is ",get_global_counter())

def smart_pointer_test_3():
    animal_list = ["cow", "cat", "dog", "horse", "sheep", "pig", "goat", "chicken"]
    smart_pointer_list = []
    for animal in animal_list:
        sp = SmartPointerMultiWrite(SmartPointerMultiWrite.new(animal))
        smart_pointer_list.append(sp)
    print("Loaded smart pointers with animals:",get_global_counter())

    pointer_to_pointer = SmartPointerMultiWrite(SmartPointerMultiWrite.new(smart_pointer_list))
    print("Loaded pointer to pointers:",get_global_counter())
    list_of_ptrs= SmartPointerMultiWrite.get(pointer_to_pointer)
    i=0
    for ptr in list_of_ptrs:
        assert(SmartPointerMultiWrite.get(ptr)==animal_list[i])
        i += 1
    list_of_ptrs = SmartPointerMultiWrite.get_and_copy(pointer_to_pointer)
    i=0
    for ptr in list_of_ptrs:
        assert(SmartPointerMultiWrite.get(ptr)==animal_list[i])
        i += 1

    print("Test 3 completed successfully with animals:",get_global_counter())

def smart_pointer_test_4():
    p_original = (SmartPointerMultiWrite(SmartPointerMultiWrite.new("cat")))
    p_copies = []
    num_copies = 100
    for i in range(num_copies):
        p_copies.append(SmartPointerMultiWrite.copy(p_original))

    total_alloc = 0
    for i in range(num_copies):
        pre_alloc = get_global_counter()[0]
        assert(SmartPointerMultiWrite.get(p_copies[i])=="cat")
        total_alloc += get_global_counter()[0] - pre_alloc

    print("Average allocs per indexed get with {} copies: {}".format(num_copies, total_alloc/num_copies))

    total_alloc = 0
    for i in range(num_copies):
        pre_alloc = get_global_counter()[0]
        assert(SmartPointerMultiWrite.get(p_copies[i])=="cat")
        total_alloc += get_global_counter()[0] - pre_alloc

    print("Average allocs per indexed get with {} copies: {}".format(num_copies, total_alloc/num_copies))


    total_alloc = 0
    for i in range(num_copies):
        pre_alloc = get_global_counter()[0]
        assert(SmartPointerMultiWrite.get(p_copies[i])=="cat")
        total_alloc += get_global_counter()[0] - pre_alloc

    print("Average allocs per indexed get with {} copies: {}".format(num_copies, total_alloc/num_copies))

    total_alloc = 0
    for i in range(num_copies):
        j = random.randint(0, num_copies -1)
        pre_alloc = get_global_counter()[0]
        assert(SmartPointerMultiWrite.get(p_copies[j])=="cat")
        total_alloc += get_global_counter()[0] - pre_alloc

    print("Average allocs per random get with {} copies: {}".format(num_copies, total_alloc/num_copies))

    total_alloc = 0
    for i in range(num_copies):
        j = random.randint(0, num_copies -1)
        pre_alloc = get_global_counter()[0]
        assert(SmartPointerMultiWrite.get(p_copies[j])=="cat")
        total_alloc += get_global_counter()[0] - pre_alloc

    print("Average allocs per random get with {} copies: {}".format(num_copies, total_alloc/num_copies))

    total_alloc = 0
    for i in range(num_copies):
        j = random.randint(0, num_copies -1)
        pre_alloc = get_global_counter()[0]
        assert(SmartPointerMultiWrite.get(p_copies[j])=="cat")
        total_alloc += get_global_counter()[0] - pre_alloc

    print("Average allocs per random get with {} copies: {}".format(num_copies, total_alloc/num_copies))


    total_alloc = 0
    for i in range(num_copies):
        pre_alloc = get_global_counter()[0]
        assert(SmartPointerMultiWrite.get(p_copies[i])=="cat")
        total_alloc += get_global_counter()[0] - pre_alloc

    print("Average allocs per indexed get with {} copies: {}".format(num_copies, total_alloc/num_copies))

    total_alloc = 0
    for i in range(num_copies):
        pre_alloc = get_global_counter()[0]
        assert(SmartPointerMultiWrite.get(p_copies[i])=="cat")
        total_alloc += get_global_counter()[0] - pre_alloc

    print("Average allocs per indexed get with {} copies: {}".format(num_copies, total_alloc/num_copies))

    total_alloc = 0
    for i in range(num_copies):
        j = random.randint(0, num_copies -1)
        pre_alloc = get_global_counter()[0]
        assert(SmartPointerMultiWrite.get(p_copies[j])=="cat")
        total_alloc += get_global_counter()[0] - pre_alloc

    print("Average allocs per random get with {} copies: {}".format(num_copies, total_alloc/num_copies))

if __name__ == "__main__":
    print("Allocs, Reads, Read/Rem, Cache Hits, Misses, Writes, Evicts, Flushes, Time")
    set_max_reads(1)
    set_max_writes(maxsize)
    
    smart_pointer_test_1()
    smart_pointer_test_2()
    smart_pointer_test_3()
    smart_pointer_test_4()