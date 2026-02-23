from single_access_machine import alloc, read, write, return_available_address

class SmartStack:
    stack_osam = True # decides if OSAM or an insecure machine is used
    structure = "SmartStack"

    @staticmethod
    def init_stack():
        stack_top = None # denotes a stack end
        return stack_top

    @staticmethod
    def push(top, value)->int:
        new_top = alloc(SmartStack.stack_osam, SmartStack.structure)
        write(new_top, [value, top], SmartStack.structure)
        return new_top

    @staticmethod
    def pop(top): 
        if top is None:
            return None, None

        next_address = read(top, SmartStack.structure)
        return_available_address(top)

        if next_address is None:
            return None, None
        elif type(next_address) == list:
            return next_address

if __name__ == "__main__":
    top = SmartStack.init_stack()
    print("Initial Stack:", top)

    top = SmartStack.push(top, "cat")
    print("Push:", top)

    top = SmartStack.push(top,"dog")
    print("Push:",top)

    value, top = SmartStack.pop(top)
    print("Pop:", top, value)

    top = SmartStack.push(top, "horse")
    print("Push:", top)

    value, top  = SmartStack.pop(top)
    print("Pop:", top, value)

    value, top  = SmartStack.pop(top)
    print("Pop:", top, value)

    value, top  = SmartStack.pop(top)
    print("Pop:", top, value)


