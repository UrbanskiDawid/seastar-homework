#pragma once

#include "range.hh"
#include <string>
#include <seastar/core/file.hh>

struct FileWithSortedChunks
{
    std::string fileName;
    Range range;
    seastar::file f;
};