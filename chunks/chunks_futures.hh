#pragma once
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/file.hh>
#include "chunk.hh"
#include "range.hh"
#include "FileWithSortedChunks.hh"

using namespace seastar;

future<Chunk> chunks_read(file f, ChunkOffset pos);
future<>      chunks_read(temporary_buffer<char>& buf, file fIn, ChunkOffset pos, ChunkOffset len);

future<> chunks_write(Chunk& chunk, file f, ChunkOffset pos);
future<> chunks_write(temporary_buffer<char>& rbuf, file fOut, ChunkOffset pos, ChunkOffset len);

future<> chunks_sort(temporary_buffer<char> & buf, ChunkOffset num);

future<> chunk_sort_ranges(std::vector<FileWithSortedChunks> sortedFiles, file fWrite);
