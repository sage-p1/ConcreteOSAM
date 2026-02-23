# Concrete Oblivious Single Access Machines

This is the codebase used to generate the results found in the paper *Oblivious Single Access Machines are Concretely Efficient*.

## How to use

This repo contains two contributions: non-cryptographic and cryptographic implementations of the tools described in the above paper. The non-cryptographiuc interface is built in Python only. The crptographic version creates bindings between the non-cryptographic Python implementation and an Path ORAM implementation in Rust. Further details of each construction can be found in their respective folders.
