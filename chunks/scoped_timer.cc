
#include "scoped_timer.hh"


ScopedTime::ScopedTime(std::string name)
    :start( now() )
    ,name(name)
    ,logger(name)
{
    logger.info("start");
}

ScopedTime::~ScopedTime()
{
    std::chrono::duration<double> delta = now()-start;
    logger.info("end, took {:.1f}sec", delta.count());
}

ScopedTime::T ScopedTime::now()
{ 
    return std::chrono::high_resolution_clock::now(); 
}