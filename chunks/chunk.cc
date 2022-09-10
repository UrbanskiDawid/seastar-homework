//chunk
#include "chunk.hh"

std::ostream& operator<<(std::ostream& os, const Chunk& dc) {
    os << "Chunk:[";
    for (size_t i=0;i<chunk_size;i++)
        os << (int)(dc[i]) << ' ';
    os << "]";
    return os;
}

bool chunk_less(const Chunk& l, const Chunk& r)
{
  return std::lexicographical_compare(
      std::begin(l.d), std::end(l.d),
      std::begin(r.d), std::end(r.d)
  );
}


