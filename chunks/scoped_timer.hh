#pragma once
#include <seastar/util/log.hh>
#include <chrono>
#include <string>

struct ScopedTime
{
    using T = std::chrono::time_point<std::chrono::high_resolution_clock>;

    ScopedTime(std::string name);
    ~ScopedTime();
    static T now();
private:
    T start;
    std::string name;
    seastar::logger logger;
};