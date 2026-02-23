import random
import pickle
import time
import statistics
import struct

from sys import maxsize
from osam_rust_backend import RustBackend

RUST_BLOCK_SIZE = 4096
ITEMS_PER_BLOCK_4096 = 32

if RUST_BLOCK_SIZE == 4096:
    LOGICAL_SLOT_SIZE = RUST_BLOCK_SIZE
    CHUNKS_PER_OBJECT = 1
    IS_PACKED_MODE = True
else:
    LOGICAL_SLOT_SIZE = 128
    CHUNKS_PER_OBJECT = (LOGICAL_SLOT_SIZE + RUST_BLOCK_SIZE - 1) // RUST_BLOCK_SIZE
    IS_PACKED_MODE = False


class SingleAccessMachine:
    def __init__(self, max_reads=1, max_writes=maxsize):
        # request counting
        self.alloc_counter = 0
        self.read_counter = 0
        self.read_and_remove_counter = 0
        self.write_counter = 0
        self.write_batches = 0
        self.max_write_batches = 0

        self.write_times = []
        self.read_times = []

        print(
            f"--- Rust ORAM Backend Initialized (Phys Block: {RUST_BLOCK_SIZE}, Logical Slot: {LOGICAL_SLOT_SIZE}, Chunks/Obj: {CHUNKS_PER_OBJECT}, Packed: {IS_PACKED_MODE}) ---"
        )
        self.backend = RustBackend()

        self.position_map = dict()
        self.max_reads = max_reads
        self.max_writes = max_writes
        self.available_addresses = []

        # plaintext storage for non-ORAM simulation (stash/recursion)
        self.plaintext_counter = -1
        self.plaintext_storage = dict()
        self.plaintext_available_addresses = []

        # flags and stats
        self.traces = False
        self.high_level_structures = set()
        self.high_level_allocs = dict()
        self.high_level_reads = dict()
        self.high_level_read_and_removes = dict()
        self.high_level_writes = dict()

    def multiple_reads_enabled(self):
        return self.max_reads > 1

    def alloc(self, use_osam=True, structure=None):
        # simulate plaintext / non-oblivious storage if OSAM is disabled (for Stash/Recursion)
        if not use_osam:
            if self.plaintext_available_addresses:
                address = self.plaintext_available_addresses.pop()
            else:
                address = self.plaintext_counter
                self.plaintext_counter -= 1
            return address

        elif self.multiple_reads_enabled() and self.available_addresses:
            return self.available_addresses.pop()

        address = self.alloc_counter
        self.position_map[address] = [0, 0]
        self.alloc_counter += 1

        # stats
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
        if address < 0:
            self.plaintext_storage[address] = value
            return

        try:
            stats = self.position_map[address]
        except KeyError:
            raise KeyError(f"No address {address}!")

        stats[1] += 1

        t0 = time.perf_counter()

        if IS_PACKED_MODE:
            phys_block_id = address // ITEMS_PER_BLOCK_4096
            sub_index = address % ITEMS_PER_BLOCK_4096

            raw_data = self.backend.read(phys_block_id)
            if not raw_data:
                raw_data = b"\0" * RUST_BLOCK_SIZE

            if not any(b != 0 for b in raw_data):
                slots = [None] * ITEMS_PER_BLOCK_4096
            else:
                try:
                    clean_data = raw_data.rstrip(b"\0")
                    slots = pickle.loads(clean_data)
                except Exception:
                    slots = [None] * ITEMS_PER_BLOCK_4096

            if len(slots) != ITEMS_PER_BLOCK_4096:
                slots = [None] * ITEMS_PER_BLOCK_4096

            slots[sub_index] = value

            packed_data = pickle.dumps(slots)
            if len(packed_data) > RUST_BLOCK_SIZE:
                raise RuntimeError(
                    f"Block Overflow! Data size {len(packed_data)} > {RUST_BLOCK_SIZE}. "
                )

            chunk = packed_data.ljust(RUST_BLOCK_SIZE, b"\0")
            self.backend.write(phys_block_id, chunk)

        else:
            try:
                data = pickle.dumps(value)
            except Exception as e:
                raise ValueError(f"Failed to pickle object at address {address}: {e}")

            if len(data) > LOGICAL_SLOT_SIZE:
                raise RuntimeError(
                    f"Logical Slot size exceeded! Object size is {len(data)} bytes, "
                    f"but limit is {LOGICAL_SLOT_SIZE}. Increase LOGICAL_SLOT_SIZE in single_access_machine.py."
                )

            padded_data = data.ljust(LOGICAL_SLOT_SIZE, b"\0")
            base_phys_addr = address * CHUNKS_PER_OBJECT

            for i in range(CHUNKS_PER_OBJECT):
                chunk = padded_data[i * RUST_BLOCK_SIZE : (i + 1) * RUST_BLOCK_SIZE]
                chunk = chunk.ljust(RUST_BLOCK_SIZE, b"\0")
                self.backend.write(base_phys_addr + i, chunk)

        t1 = time.perf_counter()
        self.write_times.append(t1 - t0)

        self.position_map[address] = [0, 0]

        self.write_counter += 1
        if structure is not None:
            self.high_level_structures.add(structure)
            self.high_level_writes[structure] = (
                self.high_level_writes.get(structure, 0) + 1
            )

        if self.traces:
            print(f"write input: {address} value: {value}")

        self.write_batches += 1
        if self.write_batches > self.max_write_batches:
            self.max_write_batches = self.write_batches

    def read(self, address, structure=None):
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
        self.read_counter += 1

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
        try:
            stats = self.position_map[address]
        except KeyError:
            raise KeyError(f"No address {address}!")

        stats[0] += 1

        value = None

        t0 = time.perf_counter()

        if IS_PACKED_MODE:
            phys_block_id = address // ITEMS_PER_BLOCK_4096
            sub_index = address % ITEMS_PER_BLOCK_4096

            raw_data = self.backend.read(phys_block_id)

            if raw_data and any(b != 0 for b in raw_data):
                try:
                    clean_data = raw_data.rstrip(b"\0")
                    slots = pickle.loads(clean_data)
                    if slots and len(slots) > sub_index:
                        value = slots[sub_index]
                except Exception:
                    value = None
        else:
            base_phys_addr = address * CHUNKS_PER_OBJECT
            full_data = bytearray()

            for i in range(CHUNKS_PER_OBJECT):
                chunk = self.backend.read(base_phys_addr + i)
                if chunk:
                    full_data.extend(chunk)
                else:
                    full_data.extend(b"\0" * RUST_BLOCK_SIZE)

            if any(b != 0 for b in full_data):
                try:
                    clean_data = full_data.rstrip(b"\0")
                    value = pickle.loads(clean_data)
                except Exception as e:
                    value = None
            else:
                stats = self.position_map.get(address, [0, 0])
                if stats[1] > 0:
                    print(
                        f"[CRITICAL] Data Loss at Address {address} (Was written {stats[1]} times!)"
                    )
                value = None

        t1 = time.perf_counter()
        self.read_times.append(t1 - t0)

        self.position_map[address] = [0, 0]

        self.read_and_remove_counter += 1

        if structure is not None:
            self.high_level_structures.add(structure)
            self.high_level_read_and_removes[structure] = (
                self.high_level_read_and_removes.get(structure, 0) + 1
            )

        if self.traces:
            print(f"read and remove input: {address} value: {value}")

        return value

    def return_available_address(self, address):
        if address is not None:
            if address < 0 and address not in self.plaintext_available_addresses:
                self.plaintext_available_addresses.append(address)
            elif (
                self.multiple_reads_enabled()
                and address not in self.available_addresses
            ):
                self.available_addresses.append(address)

    def reset(self):
        self.alloc_counter = 0
        self.read_counter = 0
        self.read_and_remove_counter = 0
        self.write_counter = 0
        self.position_map.clear()

        # Re-init backend
        self.backend = RustBackend()

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

        self.write_times = []
        self.read_times = []

    def print_timing_stats(self, flush=True):
        print("\n--- ORAM Access Timing Statistics (Seconds) ---", flush=flush)

        def print_stat(name, times):
            if not times:
                print(f"{name}: No operations recorded.", flush=flush)
                return
            avg_t = statistics.mean(times)
            min_t = min(times)
            max_t = max(times)
            med_t = statistics.median(times)
            print(
                f"{name} ({len(times)} ops): Avg={avg_t:.6f}, Min={min_t:.6f}, Med={med_t:.6f}, Max={max_t:.6f}",
                flush=flush,
            )

        print_stat("Reads ", self.read_times)
        print_stat("Writes", self.write_times)
        print("-----------------------------------------------\n", flush=flush)

    def set_max_reads(self, max_reads):
        assert max_reads > 0
        self.max_reads = max_reads

    def set_max_writes(self, max_writes):
        assert max_writes > 0
        self.max_writes = max_writes

    def get_global_counter(self):
        return (
            self.alloc_counter,
            self.read_counter,
            self.read_and_remove_counter,
            self.write_counter,
        )

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

            print(
                f"High level operations: {structure} ({a}, {r}, {rr}, {w})", flush=flush
            )

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


def print_timing_stats(flush=True):
    sam.print_timing_stats(flush)
