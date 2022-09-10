#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include "chunks_futures.hh"

using namespace seastar;

future<Chunk> chunks_read(file f, ChunkOffset pos)
{
    uint64_t aligned_pos = toBytes(pos);

    return f.dma_read_exactly<char>(aligned_pos, chunk_size).then([](temporary_buffer<char> buf) {
        auto chunks = reinterpret_cast<Chunk*>(buf.get_write());
        Chunk chunk;
        memcpy(chunk.d, chunks[0].d, chunk_size);
        return make_ready_future<Chunk>( std::move(chunk) );
    });
}

future<> chunks_read(temporary_buffer<char>& buf, file fIn, ChunkOffset pos, ChunkOffset len)
{
    uint64_t aligned_pos = toBytes(pos);
    uint64_t aligned_len = toBytes(len);

    assert(aligned_len <= buf.size());
    return fIn.dma_read(aligned_pos, buf.get_write(), aligned_len).then([&buf, aligned_len, aligned_pos] (size_t count)mutable {
        assert(count == aligned_len);
        return make_ready_future<>();
    });
}

future<> chunks_write(Chunk& chunk, file f, ChunkOffset pos)
{
    uint64_t aligned_pos = toBytes(pos);

    return f.dma_write<char>(aligned_pos, reinterpret_cast<const char*>(&chunk), chunk_size).then([aligned_pos](size_t countW) mutable{
        assert(countW == chunk_size);
        return make_ready_future<>();
    });
}

future<> chunks_write(temporary_buffer<char>& rbuf, file fOut, ChunkOffset pos, ChunkOffset len)
{
    uint64_t aligned_pos = toBytes(pos);
    uint64_t aligned_len = toBytes(len);

    assert(aligned_len <= rbuf.size());

    return fOut.dma_write(aligned_pos, rbuf.get(), aligned_len).then([aligned_pos, aligned_len](size_t countW) mutable{
        assert(countW == aligned_len);
        return make_ready_future<>();
    });
}

future<> chunks_sort(temporary_buffer<char> & buf, ChunkOffset num)
{
    assert( toBytes(num) <= buf.size());
    Chunk* chunks = reinterpret_cast<Chunk*>(buf.get_write());

    //obligatory bubble ;)
    // for (unsigned i = 0; i < num - 1; i++)
    //     for (unsigned j = 0; j < num - i - 1; j++)
    //         if ( chunk_less( chunks[j], chunks[j + 1]))
    //             swap(chunks[j], chunks[j + 1]);

    std::sort( chunks, chunks+num, chunk_less );
    return make_ready_future<>();
}

future<> chunk_sort_ranges(
    std::vector<FileWithSortedChunks> sortedFiles,
    file fWrite)
{
    return seastar::async( [=]() mutable {

        unsigned num_writen=0;
        while(true)
        {
            Chunk   selectedChunk;
            FileWithSortedChunks * selected = nullptr;
            for(auto& sf: sortedFiles)
            {
                if(sf.range.first >= sf.range.last) continue;
                Chunk chunk = chunks_read(sf.f, sf.range.first).get0();
                if(selected==nullptr or chunk_less(chunk, selectedChunk))
                {
                    selectedChunk =  chunk;
                    selected = &sf;
                }
            }
            if(selected==nullptr) break;//no more ranges

            chunks_write(selectedChunk, fWrite, num_writen).wait();//todo: buffer this write
            selected->range.first++;
            num_writen++;
            thread::maybe_yield();
        }
    });
}
