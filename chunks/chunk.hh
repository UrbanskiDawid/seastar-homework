#pragma once

#include <algorithm>
#include <ostream>

constexpr size_t chunk_size = 1024; //1k bytes, as requested in task desc

struct Chunk
{
    char d[chunk_size];
    const char& operator[](unsigned pos) const {return d[pos];}
          char& operator[](unsigned pos)       {return d[pos];}
};
static_assert(sizeof(Chunk)==chunk_size);

std::ostream& operator<<(std::ostream& os, const Chunk& dc);

bool chunk_less(const Chunk& l, const Chunk& r);
