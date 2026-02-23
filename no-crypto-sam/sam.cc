#include "sam.h"
#include <cstdlib>
#include <algorithm>
#include <iostream>

int size;
int next;
block* sam;
bool* used;

int reads = 0;
int writes = 0;
int allocs = 0;

void init(int n) {
  size = n;
  reads = 0;
  writes = 0;
  allocs = 0;

  next = 0;
  sam = new block[n];
  used = new bool[n];
  memset(used, 0, n*sizeof(bool));
}

void clear() {
  size = 0;
  delete[] sam;
  delete[] used;
}

void debug_header() {
  std::cout << "READS"
    << "\t" << "WRITES"
    << "\t" << "ALLOCS"
    << '\n';
}

void debug() {
  std::cout << reads
    << "\t" << writes 
    << "\t" << allocs
    << '\n';
}

void debug_reset() {
  reads = 0;
  writes = 0;
  allocs = 0;
}

void write(int i, block x) {
  ++writes;
  sam[i] = x;
}

block read(int i) {
  ++reads;
  if (used[i]) {
    int x = -1;
    std::cerr << "ADDRESS " << i << " ALREADY READ!\n";
    std::cerr << "SAM SIZE " << size << '\n';
    std::cerr << "ALLOCATIONS " << allocs << '\n';
    std::exit(1);
  } else {
    used[i] = true;
    return sam[i];
  }
}

int alloc() {
  ++allocs;
  return next++;
}



void link(int l, int r, int p) {
  write(l, { (int)block_type::INNER, p, r });
  write(r, { (int)block_type::INNER, p, l });
}


std::pair<int, int> copy(int p) {
  int l = alloc();
  int r = alloc();
  link(l, r, p);
  return { l, r };
}

void destroy(int x) {
  const auto bl = read(x);
  if ((block_type)bl[0] == block_type::DATA) {
    // DO NOTHING
  } else {
    const auto p = bl[1];
    const auto s = bl[2];
    const auto bl2 = read(p);
    if ((block_type)bl2[0] == block_type::DATA) {
      write(s, bl2);
    } else {
      const auto p_ = bl2[1];
      const auto s_ = bl2[2];
      link(s, s_, p_);
    }
  }
}

/**
 * Chase pointer x, splaying the tree as it goes.
 * As input take (1) a pointer x and (2) two elements that should point to the
 * new version of x.
 *
 * Return (1) the pointer where pointed-to data should be written back and (2)
 * the two fields of the pointee.
 */

int counter = 0;
std::pair<int, block> splay(int a, int b, int x) {
  while (true) {
    ++counter;

    const auto bl = read(x);
    const auto tx = (block_type)bl[0];
    const auto y = bl[1];
    const auto c = bl[2];

    if (tx == block_type::DATA) {
      int x_ = alloc();
      link(a, b, x_);
      return { x_, bl };
    }

    const auto bl2 = read(y);
    const auto ty = (block_type)bl2[0];
    const auto z = bl2[1];
    const auto d = bl2[2];
    if (ty == block_type::DATA) {
      /**
       * zig splay:
       *
       *     y            x
       *    / \          / \
       *   x   c  ==>   a   y
       *  / \              / \
       * a   b            b   c
       */

      int x_ = alloc();
      int y_ = alloc();

      link(b, c, y_);
      link(a, y_, x_);

      return { x_, bl2 };
    }

    /**
     * zig-zag splay:
     *       z               x
     *      / \             / \
     *     y   d           /   \
     *    / \      ==>    y     z
     *   c   x           / \   / \
     *      / \         a   b c   d
     *     a   b
     */

    int x_ = z;
    int y_ = alloc();
    int z_ = alloc();

    link(a, b, y_);
    link(c, d, z_);

    a = y_;
    b = z_;
    x = x_;
  }
}

std::tuple<int, int, block> deref(int x) {
  const auto bl = read(x);
  const auto t = (block_type)bl[0];
  const auto p = bl[1];
  const auto s = bl[2];

  const auto x_ = alloc();
  if (t == block_type::DATA) {
    return { x_, x_, bl };
  } else {
    const auto [where, b] = splay(x_, s, p);
    return { x_, where, b };
  }
}

