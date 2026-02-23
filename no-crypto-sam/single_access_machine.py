import random
from sys import maxsize

class SingleAccessMachine:
    def __init__(self, max_reads=1, max_writes=maxsize):
        # request counting
        self.alloc_counter = 0 # also controls the addresses being given out
        self.read_counter = 0
        self.read_and_remove_counter = 0
        self.write_counter = 0
        self.write_batches = 0
        self.max_write_batches = 0

        # basic storage
        self.position_map = dict() # key: address, value: [current reads, current writes]
        self.storage = dict() # key: address, value: value
        self.max_reads = max_reads 
        self.max_writes = max_writes
        self.available_addresses = [] # reuse allocated addresses during multi-read
       
        # plaintext storage - simulate storage that does not use OSAM
        self.plaintext_counter = -1
        self.plaintext_storage = dict()
        self.plaintext_available_addresses = []

        # flag for printing traces
        self.traces = False 

        # high level operation capture
        self.high_level_structures = set()
        self.high_level_allocs = dict()
        self.high_level_reads = dict()
        self.high_level_read_and_removes = dict()
        self.high_level_writes = dict()
        
    def multiple_reads_enabled(self):
        return self.max_reads > 1

    def alloc(self, use_osam=True, structure=None):
        # simulate plaintext / non-oblivious storage if OSAM is disabled
        if not use_osam:
            if self.plaintext_available_addresses:
                address = self.plaintext_available_addresses.pop()
            else:
                address = self.plaintext_counter
                self.plaintext_counter -= 1
            
            return address

        # recycle used addresses in multi-read settings
        elif self.multiple_reads_enabled() and self.available_addresses:
            return self.available_addresses.pop()

        # assign new address
        address = self.alloc_counter
        self.position_map[address] = [0, 0]
        self.alloc_counter += 1

        # track high level allocs 
        if structure is not None:
            self.high_level_structures.add(structure)
            if structure in self.high_level_allocs:
                self.high_level_allocs[structure] += 1
            else:
                self.high_level_allocs[structure] = 1
        
        if self.traces: 
            print("alloc counter:", self.alloc_counter)
    
        return address

    def write(self, address, value, structure=None):
        # a negative address indicates an plaintext operation
        if address < 0:
            self.plaintext_storage[address] = value
            return

        # ensure address exists
        try:
            stats = self.position_map[address]
        except:
            raise KeyError(f"No address {address}!")
        
        # ensure write limit is not exceeded
        if stats[1] >= self.max_writes:
            raise RuntimeError(f"Address {address} cannot be written to more than {self.max_writes} time(s)!")
        else:
            stats[1] += 1
       
        # store value at address
        self.storage[address] = value

        # increment counter
        self.write_counter += 1

        # track high level writes 
        if structure is not None:
            self.high_level_structures.add(structure)
            if structure in self.high_level_writes:
                self.high_level_writes[structure] += 1
            else:
                self.high_level_writes[structure] = 1

        if self.traces:
            print("write counter:", self.write_counter)

        self.write_batches += 1
        if self.write_batches > self.max_write_batches:
            self.max_write_batches = self.write_batches

    def read(self, address, structure=None):
        # a negative address indicates an plaintext operation
        if address < 0:
            try:
                value = self.plaintext_storage[address]
            except:
                if address > self.plaintext_counter:
                    value = None
                else:
                    raise KeyError(f"Address {address} not found in plaintext storage!")
            return value

        value = self.read_and_remove(address, structure)

        # increment counter
        self.read_counter += 1

        # track high level reads 
        if structure is not None:
            self.high_level_structures.add(structure)
            if structure in self.high_level_reads:
                self.high_level_reads[structure] += 1
            else:
                self.high_level_reads[structure] = 1

        if self.traces:
            print("read counter:", self.read_counter)

        if self.write_batches > 0:
            self.write_batches -= 1

        return value

    def read_and_remove(self, address, structure=None):
        # ensure address exists
        try:
            stats = self.position_map[address]
        except:
            raise KeyError(f"No address {address}!")
            
        # ensure address is not written to more than the max
        if stats[0] >= self.max_reads:
            raise RuntimeError(f"Address {address} cannot be read from more than {self.max_reads} time(s)!")
        else:
            stats[0] += 1

        value = None
        storage = None
        
        # retrieve value from storage
        try:
            value = self.storage[address]
            storage = self.storage
        except:
            pass

        # increment counter
        self.read_and_remove_counter += 1

        # track high level read_and_removes 
        if structure is not None:
            self.high_level_structures.add(structure)
            if structure in self.high_level_read_and_removes:
                self.high_level_read_and_removes[structure] += 1
            else:
                self.high_level_read_and_removes[structure] = 1

        # only delete entry if it cannot be read from again
        if storage is not None and stats[0] >= self.max_reads:
            del storage[address]

        if self.traces:
            print("read and remove counter:", self.read_and_remove_counter)
        
        return value

    def return_available_address(self, address):
        # a negative address indicates an plaintext operation
        if address is not None:
            if address < 0 and address not in self.plaintext_available_addresses:
                self.plaintext_available_addresses.append(address)

            elif self.multiple_reads_enabled() and address not in self.available_addresses:
                self.available_addresses.append(address)

    def reset(self):
        self.alloc_counter = 0
        self.read_counter = 0
        self.read_and_remove_counter = 0
        self.write_counter = 0
        self.position_map.clear()
        self.storage.clear()
        self.available_addresses.clear()
        self.plaintext_counter = -1
        self.plaintext_storage.clear()
        self.plaintext_available_addresses.clear()
        self.high_level_allocs.clear()
        self.high_level_reads.clear()
        self.high_level_read_and_removes.clear()
        self.high_level_writes.clear()
        self.write_batches = 0
        self.max_write_batches = 0

    def set_max_reads(self, max_reads):
        assert max_reads > 0
        self.max_reads = max_reads
        
    def set_max_writes(self, max_writes):
        assert max_writes > 0
        self.max_writes = max_writes

    def get_global_counter(self):
        return self.alloc_counter, self.read_counter, self.read_and_remove_counter, self.write_counter

    def print_high_level_operations(self, flush=True):
        if not self.high_level_allocs:
            return 
            
        for structure in sorted(list(self.high_level_structures)):
            try:
                a = self.high_level_allocs[structure]
            except:
                a = 0

            try:
                r = self.high_level_reads[structure]
            except:
                r = 0

            try:
                rr = self.high_level_read_and_removes[structure]
            except:
                rr = 0

            try:
                w = self.high_level_writes[structure]
            except:
                w = 0

            print(f"High level operations: {structure} ({a}, {r}, {rr}, {w})", flush=flush)

    def print_max_write_batches(self, flush=True):    
        print(f"\nMax write batches: {self.max_write_batches}", flush=flush)

    def reset_write_batches(self):    
        self.write_batches = 0
        self.max_write_batches = 0

    def clear_available_addresses(self):
        self.available_addresses.clear()
        self.plaintext_available_addresses.clear()

# global SAM
sam = SingleAccessMachine()

def multiple_reads_enabled():
    return sam.multiple_reads_enabled()
    
def get_global_counter():
    return sam.get_global_counter()

def print_high_level_operations(flush=True):
    sam.print_high_level_operations(flush)

def print_max_write_batches(flush=True):
    sam.print_max_write_batches(flush)

def reset_write_batches():
    sam.reset_write_batches()

def clear_available_addresses():
    sam.clear_available_addresses()

def alloc(use_osam=True, structure=None):
    address = sam.alloc(use_osam, structure)
    if sam.traces:
        print("alloc output: ", address)
    
    return address

def return_available_address(address):
    sam.return_available_address(address)
    
def write(address, value, structure=None):
    if sam.traces:
        print("write input: ", address, value)

    sam.write(address, value, structure)

def read(address, structure=None):
    value = sam.read(address, structure)
    if sam.traces:
        print("read input: ", address, "  value :", value)

    return value

def read_and_remove(address, structure=None):
    value = sam.read_and_remove(address, structure)
    if sam.traces:
        print("read and remove input: ", address, "  value :", value)

    return value

def reset():
    if sam.traces:
        print("sam reset")

    sam.reset()

def set_max_reads(max_reads):
    sam.set_max_reads(max_reads)

def set_max_writes(max_writes):
    sam.set_max_writes(max_writes)

def set_traces(boolean):
    sam.traces = boolean