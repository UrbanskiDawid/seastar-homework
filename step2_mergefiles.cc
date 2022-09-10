using namespace seastar;

future<> sorted_files_open(std::vector<FileWithSortedChunks>& sortedFiles)
{
    return seastar::do_for_each(sortedFiles, [] (FileWithSortedChunks& n) {
        return open_file_dma(n.fileName, open_flags::ro).then([&n ](file f){
            n.f = f;
            return make_ready_future<>();
        });
    });
}

future<> sorted_files_close_and_remove(std::vector<FileWithSortedChunks>& sortedFiles)
{
    return seastar::do_for_each(sortedFiles, [] (FileWithSortedChunks& n) {
        return n.f.close().then( [ path = n.fileName]{
            //return remove_file(path);//TODO uncomment
            return make_ready_future<>();
        });
    });
}

future<> sorted_files_merge(std::vector<FileWithSortedChunks>& sortedFiles, std::string outputFilename)
{
    return with_file(open_file_dma(outputFilename, open_flags::rw | open_flags::create | open_flags::truncate),
                     [sortedFiles] (file& f) mutable {
        return chunk_sort_ranges(sortedFiles,f);
    });
}