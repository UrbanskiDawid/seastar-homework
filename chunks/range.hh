#pragma once

typedef uint64_t ChunkOffset;
uint64_t toBytes(ChunkOffset c);

struct Range
{
    ChunkOffset first,last;
};

//split 'numOfChunks' into equaly sized ranges
//note: last range can be shorter
//'offset' - move first/all in all returned ranges
std::vector<Range> splitIntoRangs(ChunkOffset numOfChunks, ChunkOffset maxRangeLen, ChunkOffset offset=0);