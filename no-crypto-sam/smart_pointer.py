from smart_pointer_original import SmartPointerOriginal
from smart_pointer_multi_write import SmartPointerMultiWrite
from smart_pointer_splay import SmartPointerSplay
from recursive_pointer import RecursivePointer
from single_access_machine import set_max_reads, set_max_writes, clear_available_addresses
from sys import maxsize

class SmartPointer:
    label = 'multiwrite'
    
    def __init__(self, arg, temp_copy=False):
        self.temp_copy = temp_copy

        match arg:
            case SmartPointerOriginal() | SmartPointerMultiWrite() | SmartPointerSplay() | RecursivePointer():
                self.p = arg
            case _:
                match SmartPointer.label:
                    case 'original': 
                        self.p = SmartPointerOriginal(arg)
                    case 'multiwrite':
                        self.p = SmartPointerMultiWrite(arg)
                    case 'splay':
                        self.p = SmartPointerSplay(arg)
                    case 'recursive':
                        self.p = RecursivePointer(arg)
                    case _:
                        raise RuntimeError(f"Init: Label {SmartPointer.label} is not recognizable.")

    def __str__(self):
        return str(self.p)

    def __eq__(self, other):
        return hasattr(other, 'p') and type(self.p) is type(other.p) and self.p.head == other.p.head

    def get_label(self):
        return self.p.label

    def smart_delete(self):
        return SmartPointer.delete(self.p)

    @staticmethod
    def set_smart_pointer(label, clear=True):
        match label:
            case 'original':
                set_max_reads(1)
                set_max_writes(1)
            case 'multiwrite':
                set_max_reads(1)
                set_max_writes(maxsize)
            case 'splay':
                set_max_reads(1)
                set_max_writes(maxsize)
            case 'recursive':
                set_max_reads(maxsize)
                set_max_writes(maxsize)
            case _:
                raise RuntimeError(f"Set Smart Pointer: Label {label} does not correspond to any pointer type.")
                return

        SmartPointer.label = label

        if clear:
            clear_available_addresses()

    @staticmethod
    def is_temp_copy(smart_pointer):
        if hasattr(smart_pointer, 'temp_copy'):
            return smart_pointer.temp_copy   

    @staticmethod
    def put_attr(smart_pointer, attributes, delete_old=False):
        if hasattr(smart_pointer, 'p'):
            p = smart_pointer.p
        else:
            p = smart_pointer

        # update copy status if item is set as an attribute
        for value in attributes.values():
            if hasattr(value, 'temp_copy'):
                value.temp_copy = False
            elif type(value) == list:
                for item in value:
                    if hasattr(item, 'temp_copy'):
                        item.temp_copy = False

        match p:
            case SmartPointerOriginal():
                SmartPointerOriginal.put_attr(p, attributes, delete_old)
            case SmartPointerMultiWrite():
                SmartPointerMultiWrite.put_attr(p, attributes, delete_old)
            case SmartPointerSplay():
                SmartPointerSplay.put_attr(p, attributes)
            case RecursivePointer():
                RecursivePointer.put_attr(p, attributes, delete_old)
            case _:
                raise RuntimeError(f"Put Attr: Pointer type {type(p)} could not be matched.")        

    @staticmethod
    def get_attr(smart_pointer, attributes, make_copies=True):
        if hasattr(smart_pointer, 'p'):
            p = smart_pointer.p
        else:
            p = smart_pointer
        
        match p:
            case SmartPointerOriginal():
                return SmartPointerOriginal.get_attr(p, attributes, make_copies)
            case SmartPointerMultiWrite():
                return SmartPointerMultiWrite.get_attr(p, attributes, make_copies)
            case SmartPointerSplay():
                return SmartPointerSplay.get_attr(p, attributes)
            case RecursivePointer():
                return RecursivePointer.get_attr(p, attributes, make_copies)
            case _:
                raise RuntimeError(f"Get Attr: Pointer type {type(p)} could not be matched.") 
        
    @staticmethod
    def copy(smart_pointer, temp_copy=False):
        if hasattr(smart_pointer, 'p'):
            p = smart_pointer.p
            match p:
                case SmartPointerOriginal():
                    new_copy = SmartPointerOriginal.copy(p, temp_copy)
                case SmartPointerMultiWrite():
                    new_copy = SmartPointerMultiWrite.copy(p, temp_copy)
                case SmartPointerSplay():
                    new_copy = SmartPointerSplay.copy(p, temp_copy)
                case RecursivePointer():
                    new_copy = RecursivePointer.copy(p, temp_copy)
                case _:
                    raise RuntimeError(f"Copy: Pointer type {type(p)} could not be matched.") 

            match new_copy:
                case SmartPointerOriginal() | SmartPointerMultiWrite() | SmartPointerSplay() | RecursivePointer():
                    return SmartPointer(new_copy, temp_copy)
                case _:
                    return new_copy
        else:
            match SmartPointer.label:
                case 'original':
                    return SmartPointerOriginal.copy(smart_pointer, temp_copy)
                case 'multiwrite':
                    return SmartPointerMultiWrite.copy(smart_pointer, temp_copy)
                case 'splay':
                    return SmartPointerSplay.copy(smart_pointer, temp_copy)
                case 'recursive':
                    return RecursivePointer.copy(smart_pointer, temp_copy)
                case _:
                    raise RuntimeError(f"Copy: Label {label} does not correspond to any pointer type.")

    @staticmethod
    def smart_copy(smart_pointer, temp_copy=False):
        return SmartPointer.copy(smart_pointer, temp_copy)

    @staticmethod
    def new(value = None):
        match SmartPointer.label:
            case 'original': 
                return SmartPointerOriginal.new(value)
            case 'multiwrite':
                return SmartPointerMultiWrite.new(value)
            case 'splay':
                return SmartPointerSplay.new(value)
            case 'recursive':
                return RecursivePointer.new(value)
            case _:
                raise RuntimeError(f"New: Label {SmartPointer.label} is not recognizable.")
    
    @staticmethod 
    def get_and_copy(smart_pointer):
        if hasattr(smart_pointer, 'p'):
            p = smart_pointer.p
        else:
            p = smart_pointer

        match p:
            case SmartPointerOriginal():
                return SmartPointerOriginal.get_and_copy(p)
            case SmartPointerMultiWrite():
                return SmartPointerMultiWrite.get_and_copy(p)
            case SmartPointerSplay():
                return SmartPointerSplay.get_and_copy(p)
            case RecursivePointer():
                return RecursivePointer.get_and_copy(p)
            case _:
                raise RuntimeError(f"Get and Copy: Pointer type {type(p)} could not be matched.") 
    
    @staticmethod
    def get(smart_pointer):
        if hasattr(smart_pointer, 'p'):
            p = smart_pointer.p
        else:
            p = smart_pointer

        match p:
            case SmartPointerOriginal():
                return SmartPointerOriginal.get(p)
            case SmartPointerMultiWrite():
                return SmartPointerMultiWrite.get(p)
            case SmartPointerSplay():
                return SmartPointerSplay.get(p)
            case RecursivePointer():
                return RecursivePointer.get(p)
            case _:
                raise RuntimeError(f"Get: Pointer type {type(p)} could not be matched.") 
            
    @staticmethod
    def get_count(smart_pointer):
        if hasattr(smart_pointer, 'p'):
            p = smart_pointer.p
        else:
            p = smart_pointer

        match p:
            case SmartPointerOriginal():
                return SmartPointerOriginal.get_count(p)
            case SmartPointerMultiWrite():
                return SmartPointerMultiWrite.get_count(p)
            case SmartPointerSplay():
                return 0
            case RecursivePointer():
                return RecursivePointer.get_count(p)
            case _:
                raise RuntimeError(f"Get Count: Pointer type {type(p)} could not be matched.")
    
    @staticmethod
    def delete(smart_pointer):
        if type(smart_pointer) in [str, float, int, bytearray, bool]:
            return 
        elif type(smart_pointer) in [list, tuple, set, frozenset]:
            for p in smart_pointer:
                SmartPointer.delete(p)
            return
        elif smart_pointer is None:
            return
        elif hasattr(smart_pointer, 'smart_delete') and callable(smart_pointer.smart_delete):
            smart_pointer.smart_delete() 
            return
        elif hasattr(smart_pointer, 'p'):
            p = smart_pointer.p
        else:
            p = smart_pointer

        match p:
            case SmartPointerOriginal():
                SmartPointerOriginal.delete(p)
            case SmartPointerMultiWrite():
                SmartPointerMultiWrite.delete(p)
            case SmartPointerSplay():
                SmartPointerSplay.delete(p)
            case RecursivePointer():
                RecursivePointer.delete(p)
            case None:
                return
            case _:
                raise RuntimeError(f"Delete: Pointer type {type(p)} could not be matched.") 

    @staticmethod
    def delete_copy(smart_pointer):
        if SmartPointer is not None and SmartPointer.is_temp_copy(smart_pointer):
            SmartPointer.delete(smart_pointer)
            
    @staticmethod
    def put(smart_pointer, value, delete_old=True):
        if hasattr(smart_pointer, 'p'):
            p = smart_pointer.p
        else:
            p = smart_pointer

        if hasattr(value, 'out_children'):
            for i in value.out_children:
                if hasattr(i, 'temp_copy'):
                    i.temp_copy = False

        if hasattr(value, 'children'):
            for i in value.children:
                if hasattr(i, 'temp_copy'):
                    i.temp_copy = False

        if hasattr(value, 'ogE') and hasattr(value.ogE, 'temp_copy'):
            value.ogE.temp_copy = False

        match p:
            case SmartPointerOriginal():
                SmartPointerOriginal.put(p, value)
            case SmartPointerMultiWrite():
                SmartPointerMultiWrite.put(p, value, delete_old)
            case SmartPointerSplay():
                SmartPointerSplay.put(p, value)
            case RecursivePointer():
                RecursivePointer.put(p, value, delete_old)
            case _:
                raise RuntimeError(f"Put: Pointer type {type(p)} could not be matched.") 

    @staticmethod
    def is_single_reference(smart_pointer):
        if hasattr(smart_pointer, 'p'):
            p = smart_pointer.p
        else:
            p = smart_pointer

        if p is None:
            return

        match p:
            case SmartPointerOriginal():
                return SmartPointerOriginal.is_single_reference(p)
            case SmartPointerMultiWrite():
                return SmartPointerMultiWrite.is_single_reference(p)
            case SmartPointerSplay():
                return SmartPointerSplay.is_single_reference(p)
            case RecursivePointer():
                return RecursivePointer.is_single_reference(p)
            case _:
                raise RuntimeError(f"Is Single Reference: Pointer type {type(p)} could not be matched.") 
        