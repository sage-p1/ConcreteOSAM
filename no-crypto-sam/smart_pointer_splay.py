from single_access_machine import alloc, read, write, get_global_counter, set_max_reads, set_max_writes
import copy


class PtrInfo:
    def __init__(self, sibling, parent):
        self.sibling = sibling
        self.parent = parent

    def get_sibling(self):
        return self.sibling

    def set_sibling(self, s):
        self.sibling = s

    def get_parent(self):
        return self.parent

    def set_parent(self, p):
        self.parent = p

class SmartPointerSplay:
    label = 'splay'
    pointer_osam = True
    structure = "SmartPointerSplay"
    
    def __init__(self, info):
        if type(info) == int or info is None:
            self.head = info
        elif type(info) == SmartPointerSplay:
            new_pointer = SmartPointerSplay.copy(info)
            self.head = new_pointer.head
        else:
            raise ValueError("Invalid type for SmartPointerSplay initialization: {}".format(type(info)))

    def __str__(self):
        return str(self.head) if self.head is not None else "None"

    @staticmethod
    def new(val):
        p = SmartPointerSplay(None)
        p.point(val)
        return p
    
    def get_info(self):
        return self.head
    

    @staticmethod
    def put_attr(smart_pointer, attributes):
        n = SmartPointerSplay.get(smart_pointer)
        for attr in attributes:
            setattr(n, attr, attributes[attr])
        SmartPointerSplay.put(smart_pointer, n)

    @staticmethod
    def get_attr(smart_pointer, attributes):
        n = SmartPointerSplay.get(smart_pointer)
        if type(attributes) == list:
            content = {}
            for attr in attributes:
                if not hasattr(n, attr):
                    content[attr]= None
                else:
                    content[attr] = SmartPointerSplay.copy(getattr(n, attr))
        elif type(attributes) == str:
            content = SmartPointerSplay.copy(getattr(n, attributes))
        else:
            raise TypeError("Attributes must be a list or a string")
        # SmartPointerMW.put(smart_pointer, n)
        return content
            
    @staticmethod
    def copy_raw_address(new_parent):
        lnew = alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure)
        rnew = alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure)
        SmartPointerSplay.link(lnew, rnew, new_parent)

        return lnew, rnew

    @staticmethod
    def get_sam_info():
        print(get_global_counter())
        
    def point(self, val):
        n_addr = alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure)
        n = Node(val, SmartPointerSplay(None))
        write(n_addr, Data.Node(n), SmartPointerSplay.structure)
        i = PtrInfo(None, n_addr)
        self.head = alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure)
        write(self.head, Data.PtrInfo(i), SmartPointerSplay.structure)

    @staticmethod
    def data_to_info(d):
        if isinstance(d, Data.PtrInfo):
            return d.value
        raise Exception("Unreachable")

    @staticmethod
    def data_to_node(d):
        if isinstance(d, Data.Node):
            return d.value
        raise Exception("Unreachable")

    @staticmethod
    def link(sibling1, sibling2, parent):
        write(sibling1, Data.PtrInfo(PtrInfo(sibling2, parent)), SmartPointerSplay.structure)
        write(sibling2, Data.PtrInfo(PtrInfo(sibling1, parent)), SmartPointerSplay.structure)

    @staticmethod
    def __smart_copy_pointer(v1):
        p_info = SmartPointerSplay.data_to_info(read(v1.head, SmartPointerSplay.structure))
        p_info_addr = alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure)
        q_info_addr = alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure)
        if p_info.get_sibling() != None:
            n_addr = alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure)
            n_parent_addr = alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure)
            n = Node(None, SmartPointerSplay(n_parent_addr))
            write(n_addr, Data.Node(n), SmartPointerSplay.structure)
            SmartPointerSplay.link(p_info.get_sibling(), n_parent_addr, p_info.get_parent())
            p_info.set_parent(n_addr)
        SmartPointerSplay.link(p_info_addr, q_info_addr, p_info.get_parent())
        v1.head = p_info_addr
        q = SmartPointerSplay(q_info_addr)
        return q
    
    @staticmethod
    def copy(v1):
        if v1 is None:
            return None
        if type(v1) in [float, int, str, bytes, bool, frozenset]:
            return copy.deepcopy(v1)
        elif type(v1) == SmartPointerSplay:
            return SmartPointerSplay.__smart_copy_pointer(v1)
        elif type(v1) == list:
            v0 = []
            for i in range(len(v1)):
                v0.append(SmartPointerSplay.copy(v1[i]))
            return v0
        elif type(v1) == tuple:
            v0 = []
            for i in range(len(v1)):
                v0.append(SmartPointerSplay.copy(v1[i]))
            return tuple(v0)
        elif type(v1) == dict:
            v0 = dict()
            for key, value in v1.items():
                v0[SmartPointerSplay.copy(key)] = SmartPointerSplay.copy(value)
            return v0
        else:
            return v1.smart_copy()

    def delete(self):
        p_info = SmartPointerSplay.data_to_info(read(self.head, SmartPointerSplay.structure))
        if p_info.parent == None:
            return
        n = SmartPointerSplay.data_to_node(read(p_info.parent, SmartPointerSplay.structure))
        if n.parent.head != None:
            r_info = SmartPointerSplay.data_to_info(read(n.parent.head, SmartPointerSplay.structure))
            SmartPointerSplay.link(p_info.sibling, r_info.sibling, r_info.parent)
        elif p_info.sibling != None:
            n_addr = alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure)
            write(n_addr, Data.Node(n), SmartPointerSplay.structure)
            write(p_info.sibling, Data.PtrInfo(PtrInfo(None, n_addr)), SmartPointerSplay.structure)

    @staticmethod
    def rotate(z, p, q_info, d):
        if z.parent.head == None:
            raise Exception("Unreachable")
        r_info = SmartPointerSplay.data_to_info(read(z.parent.head, SmartPointerSplay.structure))
        y = SmartPointerSplay.data_to_node(read(r_info.parent, SmartPointerSplay.structure))
        if y.parent.head != None:
            s_info = SmartPointerSplay.data_to_info(read(y.parent.head, SmartPointerSplay.structure))
            x = SmartPointerSplay.data_to_node(read(s_info.parent, SmartPointerSplay.structure))
            x_addr = alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure)
            SmartPointerSplay.link(s_info.sibling, q_info, x_addr)
            y_addr = alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure)
            SmartPointerSplay.link(r_info.sibling, p.head, y_addr)
            z.parent = x.parent
            p = SmartPointerSplay(alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure))
            q = SmartPointerSplay(alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure))
            x.parent = q
            y.parent = p
            write(x_addr, Data.Node(x), SmartPointerSplay.structure)
            write(y_addr, Data.Node(y), SmartPointerSplay.structure)
            if x.data != None:
                z_addr = alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure)
                z.data = x.data
                write(z_addr, Data.Node(z), SmartPointerSplay.structure)
                SmartPointerSplay.link(p.head, q.head, z_addr)
            return z, p, q.head
        else:
            y_addr = alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure)
            if d != None:
                y.data = d
            write(y_addr, Data.Node(y), SmartPointerSplay.structure)
            z_addr = alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure)
            z.parent.head = alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure)
            SmartPointerSplay.link(z.parent.head, r_info.sibling, y_addr)
            write(z_addr, Data.Node(z), SmartPointerSplay.structure)
            SmartPointerSplay.link(p.head, q_info, z_addr)
            return y, p, q_info

    def splay(self, d):
        p = SmartPointerSplay(self.head)
        if p.head == None:
            raise Exception("Unreachable")
        p_info = SmartPointerSplay.data_to_info(read(p.head, SmartPointerSplay.structure))
        z = SmartPointerSplay.data_to_node(read(p_info.parent, SmartPointerSplay.structure))
        q_info = p_info.sibling
        flag = False
        if z.parent.head == None:
            z_addr = alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure)
            write(z_addr, Data.Node(z), SmartPointerSplay.structure)
            self.head = alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure)
            SmartPointerSplay.link(self.head, q_info, z_addr)
        while z.parent.head != None:
            if not flag:
                flag = True
                p.head = alloc(SmartPointerSplay.pointer_osam, SmartPointerSplay.structure)
                self.head = p.head
            z, p, q_info = SmartPointerSplay.rotate(z, p, q_info, d)
        return z.data

    @staticmethod
    def get(smart_pointer):
        new_pointer = SmartPointerSplay.copy(smart_pointer)
        return new_pointer.splay(None)
    
    @staticmethod
    def put(smart_pointer, d):
        new_pointer = SmartPointerSplay.copy(smart_pointer)
        smart_pointer.splay(d)

class Node:
    def __init__(self, data, parent):
        self.data = data
        self.parent = parent

    @staticmethod
    def new(d, p):
        return Node(d, p)

    def get_data(self):
        return self.data

class Data:
    class Node:
        def __init__(self, value):
            self.value = value

    class PtrInfo:
        def __init__(self, value):
            self.value = value

def smart_pointer_test_3():
    animal_list = ["cow", "cat", "dog", "horse", "sheep", "pig", "goat", "chicken"]
    smart_pointer_list = []
    for animal in animal_list:
        sp = SmartPointerSplay.new(animal)
        smart_pointer_list.append(sp)
    print("Loaded smart pointers with animals:", get_global_counter())

    pointer_to_pointer = SmartPointerSplay.new(smart_pointer_list)
    print("Loaded pointer to pointers:", get_global_counter())
    list_of_ptrs = SmartPointerSplay.get(pointer_to_pointer)
    i = 0
    for ptr in list_of_ptrs:
        assert(SmartPointerSplay.get(ptr) == animal_list[i])
        i += 1
    pointer = SmartPointerSplay.copy(pointer_to_pointer)

    list_of_ptrs = SmartPointerSplay.get(pointer)
    i = 0
    for ptr in list_of_ptrs:
        assert(SmartPointerSplay.get(ptr) == animal_list[i])
        i += 1

    print("Test 3 completed successfully with animals:", get_global_counter())


def smart_pointer_test_2():
    for lg_num_pointers in range(1, 5):
        num_pointers = 2 ** lg_num_pointers
        print("testing with ", num_pointers)

        pointers = [None] * num_pointers
        pointers[0] = SmartPointerSplay.new("cat")

        for i in range(1, num_pointers):
            pointers[i] = SmartPointerSplay.copy(pointers[i - 1])

        for i in range(num_pointers):
            val = SmartPointerSplay.get(pointers[i])
            assert(val == "cat")
            SmartPointerSplay.put(pointers[i], "cat")

        for i in range(num_pointers):
            val = SmartPointerSplay.get(pointers[i])
            assert(val == "cat")
            SmartPointerSplay.put(pointers[i], "cat")

        print("After ", num_pointers, " the global counter is ", get_global_counter())


def smart_pointer_test_1():
    num_ops = get_global_counter()

    p1 = SmartPointerSplay(SmartPointerSplay.new("cat"))
    print("Values initial no cp,", p1.head)
    p2 = SmartPointerSplay.copy(p1)
    print("First copy,", get_global_counter())
    num_ops = get_global_counter()
    assert(SmartPointerSplay.get(p1) == "cat")
    print("First get,", get_global_counter())
    assert(SmartPointerSplay.get(p1) == "cat")
    print("Second get,", get_global_counter())
    num_ops = get_global_counter()
    p3 = SmartPointerSplay.copy(p1)
    print("Second copy,", get_global_counter())
    num_ops = get_global_counter()
    assert(SmartPointerSplay.get(p1) == "cat")
    print("Third get,", get_global_counter())
    num_ops = get_global_counter()
    SmartPointerSplay.put(p1, "dog")
    print("Put,", get_global_counter())
    num_ops = get_global_counter()
    for i in range(10):
        assert(SmartPointerSplay.get(p1) == "dog")
        print("Get,", get_global_counter())
        num_ops = get_global_counter()
        assert(SmartPointerSplay.get(p2) == "dog")
        print("Get,", get_global_counter())
        num_ops = get_global_counter()
    p4 = SmartPointerSplay.new(SmartPointerSplay.get(p2))
    assert(SmartPointerSplay.get(p4) == "dog")
    print("Get three copies,", get_global_counter())
    SmartPointerSplay.put(p1, "horse")
    assert(SmartPointerSplay.get(p2) == "horse")
    print("Get three copies,", get_global_counter())
    assert(SmartPointerSplay.get(p4) == "dog")
    print("Get p4 unrelated,", get_global_counter())
    p3.delete()
    print("Get post delete p3,", get_global_counter())
    assert(SmartPointerSplay.get(p2) == "horse")
    print("Get two copies,", get_global_counter())
    p2.delete()
    print("Get post delete p2,", get_global_counter())
    assert(SmartPointerSplay.get(p1) == "horse")
    print("Get post delete p2,", get_global_counter())
    p1.delete()
    p4.delete()
    print("Test 1 completed successfully.")



if __name__ == "__main__":

    print("Allocs, Reads, Read/Rem, Cache Hits, Misses, Writes, Evicts, Flushes, Time")
    
    smart_pointer_test_1()
    smart_pointer_test_2()
    smart_pointer_test_3()
    
    
    