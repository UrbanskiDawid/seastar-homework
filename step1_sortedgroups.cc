#include <seastar/core/with_scheduling_group.hh>
#include <seastar/core/memory.hh>
#include "chunks/scoped_timer.hh"

//This module creates sorted file
//
// while sorting this file is seperated into two parts:
// X - is for fully sorted data       (first  half of file) - output of this step
// Y - is for temporary sorted blocks (second half of file) - contains group fof sorted chunks
// XXXX
// XXXX
// YYYY
// YYYY
using namespace seastar;

#include "config.hh"

logger step1log("step1");

// this function generates (sorted groups of chunks) in temporary part of file
//
// fIn - input file
// range - range to sort in fIn
// fOut - where to save data
future<> groupsort_chunks(file fIn, file fOut, Range range, std::vector<Range> &groupRanges, unsigned numOfChunks)
{
    return max_concurrent_for_each(
        groupRanges,
        max_concurent,
        [=] (Range subRange) {
            return seastar::async([=] {

                ChunkOffset len = (subRange.last-subRange.first);
                ChunkOffset relPos = (subRange.first-range.first);

                ChunkOffset fOffsetWrite =
                  numOfChunks+//start in second half of file
                  relPos;

                auto rbuf = temporary_buffer<char>::aligned(chunk_size, toBytes(len));
                chunks_read(rbuf, fIn, subRange.first, len).wait();
                thread::maybe_yield();

                chunks_sort(rbuf, len).wait();
                thread::maybe_yield();

                chunks_write(rbuf, fOut, fOffsetWrite, len).wait();
            });
        });
}

// this function generates sorted chunks in first part of FILE (based on temporary part of file)
//
// f - file with (groups of chunks)
// this will append 'f' with sorted list of chunks build from (groups of chunks)
future<> groupsort_merge_small(file f, Range range, std::vector<Range> &groupRanges, unsigned numOfChunks)
{
    //there is only one group - move it to  begining of file
    return do_with( temporary_buffer<char>::aligned(chunk_size, toBytes(numOfChunks) ),//rbuf
                    [f,numOfChunks] (auto& rbuf) mutable
    {
        return chunks_read(rbuf, f, numOfChunks, numOfChunks).then([f, numOfChunks, &rbuf] () mutable {
            return chunks_write(rbuf, f, 0, numOfChunks).then([](){
                return make_ready_future<>();
            });
        });
    });
}

// this function generates sorted chunks in first part of FILE (based on temporary part of file)
//
// f - file with (groups of chunks)
// this will append 'f' with sorted list of chunks build from (groups of chunks)
future<> groupsort_merge_big(file f, Range range, std::vector<Range> &groupRanges, unsigned numOfChunks)
{
    std::vector<FileWithSortedChunks> fileWithRanges;
    fileWithRanges.reserve(groupRanges.size());
    //convert ranges to FileWithSortedChunks
    for(auto& range: groupRanges)
        fileWithRanges.push_back(
            FileWithSortedChunks{
                "",//filename
                range,
                f
            }
        );

    //must merge groups into one
    return chunk_sort_ranges(fileWithRanges, f);
}

//discard temporaty part of file
future<> groupsort_cleanup(file f, unsigned numOfChunks)
{
    return f.truncate( toBytes(numOfChunks) );
}

//entry point for this step
future<FileWithSortedChunks> sort_part_of_input_file(std::string inputFilename, unsigned workerId, unsigned workersNum)
{
    std::string outFileName = "shard."+std::to_string(workerId)+".sorted";
    std::string workerName = "Step1worker"+std::to_string(workerId);
    const size_t maxMemPerWorker = seastar::memory::free_memory()*max_memory_percentage/workersNum;

    return with_file(open_file_dma(outFileName, open_flags::rw | open_flags::create | open_flags::truncate),
                     [=] (file& fOut) mutable {
        return with_file(open_file_dma(inputFilename, open_flags::ro),
                         [=] (file& fIn) mutable {
            return fIn.stat().then([=](struct stat s) mutable {

                return seastar::async( [=] {

                    const uint64_t numOfChunksInFile = s.st_size / chunk_size;
                    Range range{ numOfChunksInFile / workersNum *  workerId,
                                 numOfChunksInFile / workersNum * (workerId+1) };
                    if(workerId == workersNum-1) //last worker can have more chunks
                        range.last += numOfChunksInFile % workersNum;

                    unsigned numOfChunks = (range.last-range.first);
                    step1log.info("{} - will work on "
                                "{:010}...{:010} {}chunks", workerName,
                                range.first, range.last,
                                numOfChunks);

                    const size_t maxChunksInGroupBasedOnFreeMem = maxMemPerWorker/chunk_size * chunk_size / max_concurent;
                    std::vector<Range> groupsRanges = splitIntoRangs(numOfChunks, maxChunksInGroupBasedOnFreeMem, range.first);

                    for(auto& r: groupsRanges)
                        step1log.info("{} - "
                                    "group {:010}...{:010} {}chunks", workerName, r.first, r.last, (r.last-r.first));

                    {
                    ScopedTime t(workerName+" - "
                                 "groupsort_chunks");
                    groupsort_chunks(fIn, fOut, range, groupsRanges, numOfChunks).wait();
                    }

                    {
                        if(groupsRanges.size()==1)
                        {
                            ScopedTime t(workerName+" - "
                                        "merging groups (small)");
                            groupsort_merge_small(fOut, range, groupsRanges, numOfChunks).wait();
                        }else{
                            ScopedTime t(workerName+" - "
                                        "merging groups (big)");
                            groupsort_merge_big(fOut, range, groupsRanges, numOfChunks).wait();
                        }
                    }

                    {
                    ScopedTime t(workerName+" - "
                                 "cleanup");
                    groupsort_cleanup(fOut, numOfChunks).wait();
                    }
                    return numOfChunks;

                }).then( [outFileName](unsigned numOfChunks){
                    return make_ready_future<FileWithSortedChunks>(FileWithSortedChunks{
                        outFileName,
                        Range{0, numOfChunks}
                    });
                });
            });
        });
    });
}
