/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright 2020 ScyllaDB
 */

#include <cstring>
#include <limits>
#include <random>

#include <seastar/core/app-template.hh>

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/io_intent.hh>
#include <seastar/util/log.hh>
#include <seastar/util/tmp_file.hh>

using namespace seastar;

namespace {

    //input file
    //to generate it run:
    //$ dd if=/dev/urandom of=largefile.bin bs=1000 count=9999
    logger applog(__FILE__);
};

#include "chunks/range.hh"
#include "chunks/chunks_futures.hh"
#include "chunks/FileWithSortedChunks.hh"

//to lazy to create header files
#include "step1_sortedgroups.cc"
#include "step2_mergefiles.cc"


// print info about input file
future<bool> log_info_about_input_file(std::string inputFileName)
{
    return with_file(open_file_dma(inputFileName, open_flags::ro), [=] (file& fIn) mutable {
        return fIn.stat().then([=](struct stat s) {
            const uint64_t fSize = s.st_size;
            const bool fSizeIsAligned = (fSize % chunk_size == 0);
            if(not fSizeIsAligned)
                applog.error("[main] - "
                             "input file '{}' size of {}bytes is not aligned to chunk_size of {}bytes", inputFileName, fSize, std::to_string(chunk_size));
            else
                applog.info("[main] - "
                            "input file '{}' {}bytes {}chunks", inputFileName, fSize, ((double)fSize/chunk_size));
            return make_ready_future<bool>(fSizeIsAligned);
        });
    });
}

int main(int ac, char** av) {
    app_template app;

    app.add_options()
        ("input_file",  boost::program_options::value<std::string>(), "e.g. --input_file /home/dave/largefile.bin");
    app.add_options()
        ("output_file", boost::program_options::value<std::string>(), "e.g. --output_file /home/dave/largefile.bin");

    app.run(ac, av, [&app] {

        auto&& config = app.configuration();
        auto inputFileName  = config["input_file"].as<std::string>();
        auto outputFileName = config["output_file"].as<std::string>();

        applog.info("[main] - "
                    "config chunkSize:{}bytes",chunk_size);
        applog.info("[main] - "
                    "config maxBlockSize:{}bytes ({}chunks)", toBytes(maxChunksInGroup), maxChunksInGroup);
        applog.info("[main] - "
                    "output file '{}'", outputFileName);
        applog.info("[main] - "
                    "cores '{}'", seastar::smp::count);
        return seastar::async( [inputFileName,outputFileName]{

            if( not log_info_about_input_file(inputFileName).get0() )
            {
                return;
            }

            std::vector<FileWithSortedChunks> sortedFiles;

            seastar::parallel_for_each(boost::irange<unsigned>(0, seastar::smp::count),[inputFileName,&sortedFiles] (unsigned cpu) {
                return seastar::smp::submit_to(cpu, [=, &sortedFiles](){
                    return sort_part_of_input_file(inputFileName, cpu, seastar::smp::count).then([&sortedFiles](const FileWithSortedChunks& fileName){
                        sortedFiles.push_back(fileName);
                        return make_ready_future<>();
                    });
                });
            }).wait();

            for(auto &i: sortedFiles)
                applog.info("OUT file:{} {}chunks",i.fileName, (i.range.last-i.range.first) );

            applog.info("[main] - "
                        "merging sorted {}files", sortedFiles.size());
            sorted_files_open (sortedFiles).wait();
            sorted_files_merge(sortedFiles, outputFileName).wait();
            sorted_files_close_and_remove(sortedFiles).wait();
        });
    });
}