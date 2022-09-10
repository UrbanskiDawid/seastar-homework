#include <seastar/core/with_scheduling_group.hh>

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


const ChunkOffset maxChunksInGroup = 1000;
const float scheduling_group_shares = 100.0f;

// this function generates (sorted groups of chunks) in temporary part of file
//
// fIn - input file
// range - range to sort in fIn
// fOut - where to save data
future<> groupsort_chunks(file fIn, Range range, file fOut, seastar::scheduling_group sg)
{
    auto attr = seastar::thread_attributes();
    attr.sched_group = sg;

    ChunkOffset numOfChunks  = range.last-range.first;

    return parallel_for_each(
        splitIntoRangs(numOfChunks, maxChunksInGroup, range.first),
        [=] (Range subRange) {
        return seastar::with_scheduling_group(sg, [=] {
            return seastar::async([=] {
                ChunkOffset len = (subRange.last-subRange.first);
                ChunkOffset relPos = (subRange.first-range.first);

                ChunkOffset fOffsetWrite =
                  numOfChunks+//start in second half of file
                  relPos;

                auto rbuf = temporary_buffer<char>::aligned(chunk_size, toBytes(len));
                chunks_read(rbuf, fIn, subRange.first, len).wait();
                chunks_sort(rbuf, len).wait();
                chunks_write(rbuf, fOut, fOffsetWrite, len).wait();
            });
        });
    });
}

// this function generates sorted chunks in first part of FILE (based on temporary part of file)
//
// f - file with (groups of chunks)
// this will append 'f' with sorted list of chunks build from (groups of chunks)
future<> groupsort_merge(file f, Range range)
{
    ChunkOffset numOfChunks = range.last-range.first;

    //there is only one group - move it to  begining of file
    if(numOfChunks <= maxChunksInGroup)
    {
        applog.info("[merge] - "
                    "moving tmp to begining");
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

    //must merge groups into one
    return seastar::async( [f, numOfChunks]() mutable {

        std::vector<Range> ranges = splitIntoRangs(numOfChunks, maxChunksInGroup, numOfChunks);//all blocks start from middle of this file
        applog.info("[merge] - "
                    "merging groups:");
        for(auto& r: ranges)
            applog.info("[merge] - "
                        "group {:010}...{:010} {}chunks", r.first, r.last, (r.last-r.first));

        std::vector<FileWithSortedChunks> fileWithRanges;
        fileWithRanges.reserve(ranges.size());
        //convert ranges to FileWithSortedChunks
        {
            for(auto& range: ranges)
                fileWithRanges.push_back(
                    FileWithSortedChunks{
                        "",//filename
                        range,
                        f
                    }
                );

            ranges.clear();
        }

        //sort
        chunk_sort_ranges(fileWithRanges, f).wait();
    });
}

//discard temporaty part of file
future<> groupsort_cleanup(file f, Range range)
{
    const unsigned numOfChunks = range.last-range.first;
    return f.truncate( toBytes(numOfChunks) );
}

//entry point for this step
future<FileWithSortedChunks> sort_part_of_input_file(std::string inputFilename, unsigned workerId, unsigned workersNum)
{
    std::string outFileName = "shard."+std::to_string(workerId)+".sorted";

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
                    applog.info("[sort-shard] - "
                                "{:010}...{:010} {}chunks",
                                range.first, range.last,
                                numOfChunks);

                    applog.info("[sort-shard] - "
                                "generate groups");

                    auto sheduleGroup = seastar::create_scheduling_group("sg100"+std::to_string(workerId), scheduling_group_shares).get0();
                    groupsort_chunks(fIn, range, fOut, sheduleGroup).wait();
                    seastar::destroy_scheduling_group(sheduleGroup).get();

                    applog.info("[sort-shard] - "
                                "merging groups");
                    groupsort_merge(fOut, range).wait();

                    applog.info("[sort-shard] - "
                                "cleanup");
                    groupsort_cleanup(fOut, range).wait();
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
