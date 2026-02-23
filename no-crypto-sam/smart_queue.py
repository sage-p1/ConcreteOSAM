from single_access_machine import alloc, read, write, return_available_address

class SmartQueue:
    queue_osam = True
    structure = "SmartQueue"
    
    def __init__(self, head, tail):
        self.head = head
        self.tail = tail

    @staticmethod
    def init_queue():
        head = alloc(SmartQueue.queue_osam, SmartQueue.structure)
        tail = head
        queue = SmartQueue(head=head, tail=tail)
        return queue

    @staticmethod
    def enqueue(queue, value) -> None:
        new_tail = alloc(SmartQueue.queue_osam, SmartQueue.structure)
        write(queue.tail, [new_tail, value], SmartQueue.structure)
        queue.tail = new_tail

    @staticmethod
    def dequeue(queue):
        # if head equals tail, we know the queue is empty without reading head
        if queue.head == queue.tail:
            return_available_address(queue.head)
            queue.head = None
            queue.tail = None
            return None

        if queue.head is None:
            return None
            
        next_entry = read(queue.head, SmartQueue.structure)
        
        if next_entry is None:
            return None
        
        return_available_address(queue.head)

        if type(next_entry) == list:
            value, queue.head = next_entry[1], next_entry[0]
            return value
        else:
            value, queue.head = next_entry, None
            return value
    
if __name__ == "__main__":
    queue = SmartQueue.init_queue()
    print("Initial Queue:", queue.head, queue.tail)

    SmartQueue.enqueue(queue, "cat")
    print("Enqueue:", queue.head, queue.tail)

    SmartQueue.enqueue(queue, "dog")
    print("Enqueue:", queue.head, queue.tail)

    value = SmartQueue.dequeue(queue)
    print("Dequeue:", queue.head, queue.tail)

    SmartQueue.enqueue(queue, "horse")
    print("Enqueue:", queue.head, queue.tail)

    value = SmartQueue.dequeue(queue)
    print("Dequeue:", queue.head, queue.tail, value)

    value = SmartQueue.dequeue(queue)
    print("Dequeue:", queue.head, queue.tail, value)

    value = SmartQueue.dequeue(queue)
    print("Dequeue:", queue.head, queue.tail, value)

    value = SmartQueue.dequeue(queue)
    print("Dequeue:", queue.head, queue.tail, value)
