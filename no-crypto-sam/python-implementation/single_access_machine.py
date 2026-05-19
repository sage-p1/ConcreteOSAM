import random
from sys import maxsize
from typing import List, Tuple, Dict, Set, Any

"""
SingleAccessMachine - manages address allocating, reading, and writing
"""

class SingleAccessMachine:
    def __init__(self, max_reads: int = 1, max_writes: int = maxsize) -> None:
        """Initialize OSAM stats"""
        # counts SAM operations
        self.alloc_counter = 0 # also controls the addresses being given out
        self.read_counter = 0
        self.write_counter = 0

        # writes count - reads count
        self.write_batches = 0

        # the largest number of writes made at one point
        self.max_write_batches = 0

        # basic address storage
        self.position_map: Dict[int, List[int]] = dict() # key: address, value: [current reads, current writes]
        self.storage: Dict[int, Any] = dict() # key: address, value: value

        # reuse previously allocated addresses no longer being used during multi-read
        self.available_addresses: List[int] = []

        # storage limits
        self.max_reads = max_reads 
        self.max_writes = max_writes
       
        # plaintext storage - simulate storage that does not use OSAM
        self.plaintext_counter = -1
        self.plaintext_storage: Dict[int, Any] = dict() # key: address, value: value

        # reuse previously allocated plaintext addresses no longer being used
        self.plaintext_available_addresses: List[int] = []

        # flag for printing traces
        self.traces = False 

        # map SAM-based structure to its number of SAM operations
        self.structures: Set = set()
        self.structure_allocs: Dict[str, int] = dict()
        self.structure_reads: Dict[str, int] = dict()
        self.structure_writes: Dict[str, int] = dict()
        
    def multiple_reads_enabled(self) -> bool:
        """Output boolean if multiple reads are enabled"""
        return self.max_reads > 1

    def alloc(self, use_osam: bool = True, structure: str | None = None) -> int:
        """Generate an fresh address to be written to and read from"""
        # simulate plaintext / non-oblivious storage if OSAM is disabled
        if not use_osam:
            # reuse previously assigned address that is not in current use
            if self.plaintext_available_addresses:
                address = self.plaintext_available_addresses.pop()

            # assign new plaintext address
            else:
                address = self.plaintext_counter
                self.plaintext_counter -= 1
            
            return address

        # recycle used addresses in multi-read settings
        elif self.multiple_reads_enabled() and self.available_addresses:
            address = self.available_addresses.pop()
            return address

        # assign new address
        address = self.alloc_counter
        self.position_map[address] = [0, 0] # current reads and writes
        self.alloc_counter += 1

        # track allocs by structure
        if structure is not None:
            self.structures.add(structure)
            if structure in self.structure_allocs:
                self.structure_allocs[structure] += 1
            else:
                self.structure_allocs[structure] = 1
        
        # print traces
        if self.traces: 
            print("alloc counter:", self.alloc_counter)
    
        return address

    def write(self, address: int, value: Any, structure: str | None = None):
        """Writes value to address in storage"""
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

        # track writes by structure
        if structure is not None:
            self.structures.add(structure)
            if structure in self.structure_writes:
                self.structure_writes[structure] += 1
            else:
                self.structure_writes[structure] = 1

        # print traces
        if self.traces:
            print("write counter:", self.write_counter)

        # track max write batches
        self.write_batches += 1
        if self.write_batches > self.max_write_batches:
            self.max_write_batches = self.write_batches

    def read(self, address: int , structure: str | None = None) -> Any:
        """Get value associated with address"""
        # a negative address indicates an plaintext operation
        if address < 0:
            try:
                value = self.plaintext_storage[address]
            except:
                raise KeyError(f"Address {address} not found in plaintext storage!")
            return value

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
            assert address in self.position_map

        # only delete entry if it cannot be read from again
        if storage is not None and stats[0] >= self.max_reads:
            del storage[address]

        # increment counter
        self.read_counter += 1

        # track reads by structure
        if structure is not None:
            self.structures.add(structure)
            if structure in self.structure_reads:
                self.structure_reads[structure] += 1
            else:
                self.structure_reads[structure] = 1

        # print traces
        if self.traces:
            print("read counter:", self.read_counter)

        # decrement write batches
        if self.write_batches > 0:
            self.write_batches -= 1

        return value

    def return_available_address(self, address: int | None) -> None:
        """
        Return previously allocated address not in use to be allocated again later
        Trusts the caller the given address is not currently in use
        """
        if address is not None:
            # a negative address indicates an plaintext operation
            if address < 0 and address not in self.plaintext_available_addresses:
                self.plaintext_available_addresses.append(address)

            # can only return addresses in the multi-read setting (simulate recursive pointer)
            elif self.multiple_reads_enabled() and address not in self.available_addresses:
                self.available_addresses.append(address)

    def reset(self) -> None:
        """Reset all SAM counters / stats / storage"""
        self.alloc_counter = 0
        self.read_counter = 0
        self.write_counter = 0
        self.position_map.clear()
        self.storage.clear()
        self.available_addresses.clear()
        self.plaintext_counter = -1
        self.plaintext_storage.clear()
        self.plaintext_available_addresses.clear()
        self.structure_allocs.clear()
        self.structure_reads.clear()
        self.structure_writes.clear()
        self.write_batches = 0
        self.max_write_batches = 0

    def set_max_reads(self, max_reads: int) -> None:
        """Set max reads"""
        assert max_reads > 0
        self.max_reads = max_reads
        
    def set_max_writes(self, max_writes: int) -> None:
        """Set max writes"""
        assert max_writes > 0
        self.max_writes = max_writes

    def get_global_counter(self) -> Tuple[int, int, int]:
        """Output SAM stats (allocs, reads, writes)"""
        return self.alloc_counter, self.read_counter, self.write_counter

    def print_stats_by_structure(self, flush: bool = True) -> None:
        """Print each SAM-based structure to its number of SAM operations"""
        if not self.structure_allocs:
            return 
            
        for structure in sorted(list(self.structures)):
            try:
                a = self.structure_allocs[structure]
            except:
                a = 0

            try:
                r = self.structure_reads[structure]
            except:
                r = 0

            try:
                w = self.structure_writes[structure]
            except:
                w = 0

            print(f"Structure stats: {structure} ({a}, {r}, {w})", flush=flush)

    def print_max_write_batches(self, flush: bool = True) -> None:    
        print(f"\nMax write batches: {self.max_write_batches}", flush=flush)

    def reset_write_batches(self) -> None:    
        """Reset write batches outside of reset()"""
        self.write_batches = 0
        self.max_write_batches = 0

    def clear_available_addresses(self) -> None:
        """Clear saved addresses outside of reset()"""
        self.available_addresses.clear()
        self.plaintext_available_addresses.clear()


"""
Global SAM with global functions to be imported
"""

# global SAM
sam = SingleAccessMachine()

def multiple_reads_enabled() -> int:
    return sam.multiple_reads_enabled()
    
def get_global_counter() -> Tuple[int, int, int]:
    return sam.get_global_counter()

def print_stats_by_structure(flush: bool = True) -> None:
    sam.print_stats_by_structure(flush)

def print_max_write_batches(flush: bool = True) -> None: 
    sam.print_max_write_batches(flush)

def reset_write_batches() -> None:
    sam.reset_write_batches()

def clear_available_addresses() -> None:
    sam.clear_available_addresses()

def alloc(use_osam: bool = True, structure: str | None = None) -> int:
    address = sam.alloc(use_osam, structure)
    if sam.traces:
        print("alloc output: ", address)
    
    return address

def return_available_address(address: int | None) -> None:
    sam.return_available_address(address)
    
def write(address: int, value: Any, structure: str | None) -> None:
    if sam.traces:
        print("write input: ", address, value)

    sam.write(address, value, structure)

def read(address: int, structure: str | None = None) -> Any:
    value = sam.read(address, structure)
    if sam.traces:
        print("read input: ", address, "  value:", value)

    return value

def reset() -> None:
    if sam.traces:
        print("sam reset")

    sam.reset()

def set_max_reads(max_reads: int) -> None:
    sam.set_max_reads(max_reads)

def set_max_writes(max_writes: int) -> None:
    sam.set_max_writes(max_writes)

def set_traces(on: bool) -> None:
    sam.traces = on
