
# Cryptographic OSAM
A bridge between a stripped-down fork of facebook/oram-rs and a Python implementation of OSAM. The fork keeps only the cryptographic position map logic, discarding everything else. The Python side handles OSAM operations without its own crypto. Together, they let you run OSAM operations in Python backed by a fast Rust cryptographic implementation.

All standard ORAM features are disabled since the goal here is purely to evaluate and time OSAM operations.

⚠️ **Benchmarking Only:** This project exists purely for timing cryptographic operations. It has not been audited for security and should not be used in any real-world or production setting. For research purposes, you don't need this repo as the timing operations for read and write will stay consistent. Just use the no-crypto-sam version in the root directory.


## Directory Structure

| Directory | Description |
|-----------|-------------|
| `pyosam` | A modified cryptographic OSAM implementation with Rust bindings via maturin. |
| `osam-rs` | Rust bindings to oram-rs, compiled into a Python-importable module. |
| `rust-oram` | Fork of facebook's recursive ORAM implementation w/ determinstic eviction. |

---

## Setup

First, make sure you have [Rust](https://rustup.rs/) and Python 3 installed.

Create a virtual environment and install the Python dependencies:

```bash
cd pyosam

python3 -m venv venv
source venv/bin/activate

pip3 install -r requirements.txt
```

Keep this virtual environment around. Activate it each time you want to run the benchmark.
```bash
source venv/bin/activate
```

## Building the `osam-rs` Module

With your same virtual environment as above active, run:

```bash
cd osam-rs
maturin develop --release
```

Maturin handles the Rust compilation and installs the module into your virtual environment.

## Running the Benchmark

With your virtual environment active, run:

```bash
python3 run_single_test.py -- <args here>
```

**Note**: Only block sizes 64 and 4096 are supported. Since this repo only exists to benchmark, we've kept a lot of code non-integrated to save on development time. You may need to manually move the variations of oblivious_graph files to run the two block sizes due to different packing strategies.
