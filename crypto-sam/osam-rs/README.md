# OSAM Wrapper
This is a wrapper for a custom fork of ORAM with various changes to remove things not needed by OSAM that acts as the backend for pyosam. This repository also contains benchmarking tools to calculate read and write timings of the backend.

There is an overhead of 2.1x for bs=64 for both reads and writes, and an overhead of 2.1x for writes for bs=4096 between the benchmark criterion here and running it through pyosam.

## ⚠️ Important Warning
This implementation is intended to be used to benchmark our paper. Do not use this under any other circumstances. The code has NOT been vetted by security experts. Therefore, no part of this code should be used in any real-world or production setting.

## License
This project is provided under GPL. The criterion benchmark is modified from the ORAM implementation and has its own license which must be respected.
