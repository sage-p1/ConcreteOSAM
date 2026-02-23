from single_access_machine import alloc, read, write, return_available_address

class AddressQueue:
    queue_osam = True
    structure = "AddressQueue"
    
    @staticmethod
    def init_queue():
        head = alloc(AddressQueue.queue_osam, AddressQueue.structure)
        tail = head
        return head, tail

    @staticmethod
    def enqueue(tail, value)->int:
        new_tail = alloc(AddressQueue.queue_osam, AddressQueue.structure)
        write(tail, [value, new_tail], AddressQueue.structure)
        tail = new_tail
        return tail

    @staticmethod
    def dequeue(head): 
        if head is None:
            return None, None

        next_address = read(head, AddressQueue.structure)
        return_available_address(head)

        if next_address is None:
            return None, None

        if type(next_address) == list:
            return next_address

if __name__ == "__main__":
    head, tail = AddressQueue.init_queue()
    print("Initial Queue:", head, tail)

    tail = AddressQueue.enqueue(tail, "cat")
    print("Enqueue:", head, tail)

    tail = AddressQueue.enqueue(tail,"dog")
    print("Enqueue:",head, tail)

    value, head = AddressQueue.dequeue(head)
    print("Dequeue:", head, tail, value)

    tail = AddressQueue.enqueue(tail, "horse")
    print("Enqueue:", head, tail)

    value, head = AddressQueue.dequeue(head)
    print("Dequeue:", head, tail, value)

    value, head = AddressQueue.dequeue(head)
    print("Dequeue:", head, tail, value)

    value, head = AddressQueue.dequeue(head)
    print("Dequeue:", head, tail, value)