#include "chunk.hh"
#include "range.hh"
#include <iostream>
uint64_t toBytes(ChunkOffset c)
{
    return c * chunk_size;
}

std::vector<Range> splitIntoRangs(ChunkOffset numOfChunks, ChunkOffset maxRangeLen, ChunkOffset offset)
{
    std::vector<Range> ranges;

    if(numOfChunks <= maxRangeLen)
    {
        ranges.push_back( Range{offset + 0,
                                offset + numOfChunks} );
        return ranges;
    }
    const unsigned numOfGroups          = (numOfChunks/maxRangeLen);
    const unsigned numOfGroupsRemainder = (numOfChunks%maxRangeLen);
    ranges.reserve(numOfGroups + (numOfGroupsRemainder?1:0));

    for(unsigned i=0; i<numOfGroups; i++)
        ranges.push_back( Range{offset + ((i+0) * maxRangeLen),
                                offset + ((i+1) * maxRangeLen)} );

    if(numOfGroupsRemainder )
        ranges.push_back( Range{offset + maxRangeLen * numOfGroups,
                                offset + numOfChunks} );

    return ranges;
}