from single_access_machine import alloc, read, write, get_global_counter, set_max_reads, set_max_writes
from address_queue import AddressQueue as queue
import copy
from sublist import sublist

class SmartNode():
    def __init__(self, tailL=None, tailR=None, headL=None, headR=None, headP=None, isRoot=None, content=None):
        self.tailL = tailL
        self.tailR = tailR
        self.headL = headL
        self.headR = headR
        self.isRoot = isRoot
        self.content = content
        self.headP = headP
        self.count = 1

    def __str__(self):
        return "Node Contents: "+str(self.tailR)+", "+str(self.tailL)+ ", "+str(self.headL)+", "+ str(self.headR) +", "+str(self.isRoot) +", "+ str(self.content) +", "+str(self.headP)
        
class SmartPointerOriginal:
    label = 'original'
    pointer_osam = True
    structure = "SmartPointerOriginal"

    def __init__(self, head):
        self.head = head
        
    def __str__(self):
        return str(self.head) if self.head is not None else "None"

    # retrieves the thing at the tail of the queue
    # this should only be an internal method
    @staticmethod
    def __chase(head):
        target, latest, tail = None, None, None
        while (head is not None) and type(head) == int:
            latest = target
            tail = head
            target, head = queue.dequeue(head)
            
        if latest is not None:
            n = read(latest, SmartPointerOriginal.structure)
        else:
            n = None
        if n is not None:
            if n.tailL == tail:
                n.tailL = None
            else:
                n.tailR = None
        return n

    #Have to pass in a SmartNode, checks if the appropriate queues have been
    #created if not initialize the two queues
    @staticmethod
    def __save_node(n: SmartNode):
        a = alloc(SmartPointerOriginal.pointer_osam, SmartPointerOriginal.structure)
        if n is not None:
            if n.tailL is not None:
                n.tailL = queue.enqueue(n.tailL, a)
            if n.tailR is not None:
                n.tailR = queue.enqueue(n.tailR, a)
        write(a, n, SmartPointerOriginal.structure)

    @staticmethod
    def __addTail(n):
        if n is None:
            return None
        head, tail = queue.init_queue()
        if n.tailL is not None and n.tailR is not None:
            raise ValueError("Both tails are already occupied")
        if n.tailL == None:
            n.tailL = tail
        else:
            n.tailR = tail
        return head

    @staticmethod
    def put_attr(p, attributes, delete_old=False):
        n = SmartPointerOriginal.__chase(p.head)
        p.head = SmartPointerOriginal.__addTail(n)
        while n is not None and (not n.isRoot):
            new_n = SmartPointerOriginal.__chase(n.headP)
            n.headP = SmartPointerOriginal.__addTail(new_n)
            SmartPointerOriginal.__save_node(n)
            n = new_n
        for attr in attributes:
            if delete_old and hasattr(n.content, attr):
                SmartPointerOriginal.delete(getattr(n.content, attr))
            setattr(n.content, attr, attributes[attr])
        SmartPointerOriginal.__save_node(n)

    @staticmethod
    def is_single_reference(p):
        n = SmartPointerOriginal.__chase(p.head)
        p.head = SmartPointerOriginal.__addTail(n)
        another_pointer=False
        if n.tailL is not None and n.tailR is not None:
            another_pointer=True
        while n is not None and (not n.isRoot):
            new_n = SmartPointerOriginal.__chase(n.headP)
            n.headP = SmartPointerOriginal.__addTail(new_n)
            SmartPointerOriginal.__save_node(n)
            n = new_n
            if n.tailL is not None and n.tailR is not None:
                another_pointer=True
        SmartPointerOriginal.__save_node(n)
        return not another_pointer
    
    @staticmethod
    def get_attr(p, attributes, make_copies=True):
        n = SmartPointerOriginal.__chase(p.head)
        p.head = SmartPointerOriginal.__addTail(n)
        while n is not None and (not n.isRoot):
            new_n = SmartPointerOriginal.__chase(n.headP)
            n.headP = SmartPointerOriginal.__addTail(new_n)
            SmartPointerOriginal.__save_node(n)
            n = new_n
        if type(attributes) == sublist:
            content = {}
            if not hasattr(n.content, attributes.attribute):
                content[attributes.attribute] = None
            else:
                lst = getattr(n.content, attributes.attribute)
                sublst = []
                for pos in attributes.positions:
                    if make_copies:
                        sublst.append(SmartPointerOriginal.copy(lst[pos], temp_copy=make_copies))
                    else:
                        sublst.append(lst[pos])
                content[attributes.attribute] = sublst
        elif type(attributes) == list:
            content = {}
            for attr in attributes:
                if not hasattr(n.content, attr):
                    content[attr] = None
                else:
                    if make_copies:
                        content[attr] = SmartPointerOriginal.copy(getattr(n.content, attr), temp_copy=make_copies)
                    else:
                        content[attr] = getattr(n.content, attr)
        elif type(attributes) == str:
            if make_copies:
                content = SmartPointerOriginal.copy(getattr(n.content, attributes), temp_copy=make_copies)
            else:
                content = getattr(n.content, attributes)
        else:
            raise TypeError("Attributes must be a list, a sublist, or a string")
        SmartPointerOriginal.__save_node(n)
        return content

    # This reads the value of anything at p
    @staticmethod
    def get(p):
        n = SmartPointerOriginal.__chase(p.head)
        p.head = SmartPointerOriginal.__addTail(n)
        while n is not None and (not n.isRoot):
            new_n = SmartPointerOriginal.__chase(n.headP)
            n.headP = SmartPointerOriginal.__addTail(new_n)
            SmartPointerOriginal.__save_node(n)
            n =new_n
        if n is not None:
            content = n.content
        else:
            content = None
        SmartPointerOriginal.__save_node(n)
        return content

    # This returns the count of the number of smart pointers
    # that point to the same content
    # This is used to determine if the content can be deleted
    @staticmethod
    def get_count(p):
        n = SmartPointerOriginal.__chase(p.head)
        p.head = SmartPointerOriginal.__addTail(n)
        while n is not None and (not n.isRoot):
            new_n = SmartPointerOriginal.__chase(n.headP)
            n.headP = SmartPointerOriginal.__addTail(new_n)
            SmartPointerOriginal.__save_node(n)
            n =new_n
        count = n.count
        SmartPointerOriginal.__save_node(n)
        return count
        
    # This reads the value of anything at p
    # if the value of things at p contains
    # smart pointers they are smart copied
    @staticmethod
    def get_and_copy(p):
        n = SmartPointerOriginal.__chase(p.head)
        p.head = SmartPointerOriginal.__addTail(n)
        while n is not None and (not n.isRoot):
            new_n = SmartPointerOriginal.__chase(n.headP)
            n.headP = SmartPointerOriginal.__addTail(new_n)
            SmartPointerOriginal.__save_node(n)
            n =new_n
        copy_of_data = SmartPointerOriginal.copy(n.content, temp_copy=True)
        SmartPointerOriginal.__save_node(n)
        return copy_of_data

    # This places content in p, smart copying as appropriate
    @staticmethod
    def put(p, c):
        # delete content all the way at end of pointer
        SmartPointerOriginal.delete(SmartPointerOriginal.get(p))

        n = SmartPointerOriginal.__chase(p.head)
        SmartPointerOriginal.delete(n.content)
        p.head = SmartPointerOriginal.__addTail(n)

        while n is not None and (not n.isRoot):
            new_n = SmartPointerOriginal.__chase(n.headP)
            n.headP = SmartPointerOriginal.__addTail(new_n)
            SmartPointerOriginal.__save_node(n)
            n = new_n

        n.content = SmartPointerOriginal.copy(c)
        SmartPointerOriginal.__save_node(n)

    @staticmethod
    def delete(p):
        if type(p) in [str, float, int, bytearray, bool]:
            return 
        elif type(p) in [list, tuple, set, frozenset]:
            for p1 in p:
                SmartPointerOriginal.delete(p1)
            return
        elif hasattr(p, "smart_delete"):
            p.smart_delete()
            return 
        elif p is None:
            return 

        assert type(p) is SmartPointerOriginal
        if p.head is None:
            return 
        n = SmartPointerOriginal.__chase(p.head)
        if n.isRoot:
            if n.tailL is None and n.tailR is None:
                SmartPointerOriginal.delete(n.content)
            else:
                n.count = n.count -1
                SmartPointerOriginal.__save_node(n)
        else:
            if n.tailL:
                tail = n.tailL
            else:
                tail = n.tailR
            n = SmartPointerOriginal.__chase(n.headP)
            n.count = n.count -1
            if n.tailL is None:
                n.tailL = tail
            else:
                n.tailR = tail
            SmartPointerOriginal.__save_node(n)
        p.head = None

    @staticmethod
    def __smart_copy_pointers(p0, p1):
        if p1 is None:
            p0 = None
        n = SmartPointerOriginal.__chase(p1.head)
        if n is not None and type(n) == SmartNode:
            n.count = n.count+1
            if (n.tailL is not None or n.tailR is not None):
                nNew = SmartNode(headP=SmartPointerOriginal.__addTail(n), isRoot=False, content="Branch Node")
                SmartPointerOriginal.__save_node(n)
                n = nNew
        p0.head = SmartPointerOriginal.__addTail(n)
        p1.head = SmartPointerOriginal.__addTail(n)
        SmartPointerOriginal.__save_node(n)
        
    @staticmethod 
    def __smart_copy_smart_node(n0, n1):
        # print("Copying a smart node")
        n0.tailL = SmartPointerOriginal.copy(n1.tailL)
        n0.tailR = SmartPointerOriginal.copy(n1.tailR)
        n0.headL = SmartPointerOriginal.copy(n1.headL)
        n0.headR = SmartPointerOriginal.copy(n1.headR)
        n0.content = SmartPointerOriginal.copy(n1.content)
        n0.isRoot = n1.isRoot
        n0.headP = SmartPointerOriginal.copy(n1.headP)
        # n0.tailP = SmartPointerOriginal.copy(n1.tailP)
        n0.count = n1.count
        
    @staticmethod
    def copy(v1, temp_copy=False):
        if v1 is None:
            return None
        if type(v1) in [float, int, str, bytes, bool, frozenset]:
            return copy.deepcopy(v1)
        elif type(v1) == SmartPointerOriginal:
            v0 = SmartPointerOriginal(None)
            SmartPointerOriginal.__smart_copy_pointers(v0, v1)
            return v0
        elif type(v1) == SmartNode:
            v0 = SmartNode()
            SmartPointerOriginal.__smart_copy_smart_node(v0, v1)
        elif type(v1) == list:
            v0 = []
            for i in range(len(v1)):
                v0.append(SmartPointerOriginal.copy(v1[i], temp_copy))
            return v0
        elif type(v1) == tuple:
            v0 = []
            for i in range(len(v1)):
                v0.append(SmartPointerOriginal.copy(v1[i], temp_copy))
            return tuple(v0)
        elif type(v1) == dict:
            v0 = dict()
            for key, value in v1.items():
                v0[SmartPointerOriginal.copy(key, temp_copy)] = SmartPointerOriginal.copy(value, temp_copy)
            return v0
        else:
            return v1.smart_copy(v1, temp_copy)
        
    @staticmethod
    def smart_copy(p):
        return SmartPointerOriginal.copy(p)

    @staticmethod
    def new(c):
        v = SmartNode(tailL=None, tailR=None, content=c, isRoot=True)
        head = SmartPointerOriginal.__addTail(v)
        SmartPointerOriginal.__save_node(v)
        return head

if __name__ == "__main__":
    set_max_reads(1)
    set_max_writes(1)
    
    print("Initializing pointers:")
    
    # sp = SmartPointerOriginal(alloc(SmartPointerOriginal.pointer_osam, SmartPointerOriginal.structure))
    # sp.save_node(node)

    p1 = SmartPointerOriginal(SmartPointerOriginal.new("cat"))
    print("Values initial no cp,",p1.head)
    assert(SmartPointerOriginal.get_count(p1)==1)
    assert(SmartPointerOriginal.is_single_reference(p1)==True)
    num_ops = get_global_counter()[0]
    p2 = SmartPointerOriginal.copy(p1)
    print("Number allocs first copy,", get_global_counter()[0]-num_ops)
    assert(SmartPointerOriginal.get_count(p2)==2)
    assert(SmartPointerOriginal.is_single_reference(p1)==False)
    assert(SmartPointerOriginal.is_single_reference(p2)==False)
    num_ops = get_global_counter()[0]
    # print("Post copy values ",p1.head, p2.head, p3.head)
    assert(SmartPointerOriginal.get(p1)=="cat")
    print("Number allocs first get,", get_global_counter()[0]-num_ops)
    
    num_ops = get_global_counter()[0]
    #Check if you can get the same value twice
    assert(SmartPointerOriginal.get(p1)=="cat")
    print("Number allocs second get,", get_global_counter()[0]-num_ops)
    num_ops = get_global_counter()[0]
    p3 = SmartPointerOriginal.copy(p1)
    print("Number allocs second copy,", get_global_counter()[0]-num_ops)
    assert(SmartPointerOriginal.get_count(p3)==3)
    num_ops = get_global_counter()[0]
    assert(SmartPointerOriginal.get(p1)=="cat")
    print("Number allocs third get,", get_global_counter()[0]-num_ops)
    num_ops = get_global_counter()[0]
    SmartPointerOriginal.put(p1, "dog")
    print("Number allocs put,", get_global_counter()[0]-num_ops)
    num_ops = get_global_counter()[0]
    for i in range(10):
        assert(SmartPointerOriginal.get(p1)=="dog")
        print("Number allocs get,", get_global_counter()[0]-num_ops)
        num_ops = get_global_counter()[0]
        assert(SmartPointerOriginal.get(p2)=="dog")
        print("Number allocs get,", get_global_counter()[0]-num_ops)
        num_ops = get_global_counter()[0]
    p4 = SmartPointerOriginal(SmartPointerOriginal.new(SmartPointerOriginal.get(p2)))
    assert(SmartPointerOriginal.get(p4)=="dog")
    print("Number allocs get three copies,", get_global_counter()[0]-num_ops)
    assert(SmartPointerOriginal.get_count(p4)==1)
    assert(SmartPointerOriginal.is_single_reference(p4)==True)
    num_ops = get_global_counter()[0]
    SmartPointerOriginal.put(p1, "horse")
    assert(SmartPointerOriginal.get(p2)=="horse")
    print("Number allocs get three copies,", get_global_counter()[0]-num_ops)
    assert(SmartPointerOriginal.is_single_reference(p1)==False)
    num_ops = get_global_counter()[0]
    assert(SmartPointerOriginal.get(p4)=="dog")
    print("Number allocs get p4 unrelated,", get_global_counter()[0]-num_ops)
    num_ops = get_global_counter()[0]
    SmartPointerOriginal.delete(p3)
    assert(SmartPointerOriginal.get_count(p2)==2)
    print("Number allocs get post delete p3,", get_global_counter()[0]-num_ops)
    num_ops = get_global_counter()[0]
    assert(SmartPointerOriginal.is_single_reference(p2)==False)
    assert(SmartPointerOriginal.get(p2)=="horse")
    print("Number allocs get two copies,", get_global_counter()[0]-num_ops)
    num_ops = get_global_counter()[0]
    SmartPointerOriginal.delete(p2)
    assert(SmartPointerOriginal.is_single_reference(p1)==True)
    assert(SmartPointerOriginal.get_count(p1)==1)
    print("Number allocs get post delete,", get_global_counter()[0]-num_ops)
    num_ops = get_global_counter()[0]
    assert(SmartPointerOriginal.get(p1)=="horse")
    print("Number allocs get post delete,", get_global_counter()[0]-num_ops)
    num_ops = get_global_counter()[0]
    SmartPointerOriginal.delete(p1)
    SmartPointerOriginal.delete(p4)
