#pragma once

#include <vector>
#include <fstream>
#include "mapreduce_spec.h"
#include <math.h>


/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileInfo {
     unsigned int offset_start;
     unsigned int offset_end;
     std::string filename;
};

struct FileShard {
     std::vector<FileInfo> files;
     int id;
};


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
	
	std::vector<std::string> input_files = mr_spec.input_files;
	int max_shard_size = mr_spec.map_kilobytes * 1000;

	std::vector<int> file_sizes;
	std::vector<int> bytes_remaining;

	double total_size = 0.0;
	for (auto filename : input_files) {
		std::ifstream file(filename, std::ios::in | std::ios::ate);
		total_size += (double)file.tellg();
		file_sizes.push_back(file.tellg());
		bytes_remaining.push_back(file.tellg());
		file.close();
	}

	int num_shards = ceil(total_size/max_shard_size);

	int shard_idx = 0;
	int file_idx = 0;
	int start_offset = 0;
	while (shard_idx < num_shards) {
		FileShard shard;
		int current_shard_size = 0.0;

		// put the remaining contents of a file in the current shard if possible
		while (file_idx < input_files.size() && current_shard_size + (double)bytes_remaining[file_idx] <= max_shard_size) {
			current_shard_size += (double)bytes_remaining[file_idx];

			FileInfo fileInfo;
			fileInfo.filename = input_files[file_idx];
			fileInfo.offset_start = start_offset;
			fileInfo.offset_end = file_sizes[file_idx];
			shard.files.push_back(fileInfo);
			
			file_idx++;
			start_offset = 0;
		}

		if(current_shard_size <= max_shard_size && file_idx < input_files.size()) {
			
			std::ifstream file(input_files[file_idx], std::ios::in|std::ios::binary);

			int additional_chars = 0;
			int remaining = max_shard_size - current_shard_size;
			file.seekg(remaining, file.beg);
			char* c = (char *)malloc(1);
			while (!file.eof() && *c == '\n') {
				file.read(c, 1);
				additional_chars++;
			}
			int end_offset = remaining + additional_chars;

			FileInfo fileInfo;
			fileInfo.filename = input_files[file_idx];
			fileInfo.offset_start = start_offset;
			fileInfo.offset_end = end_offset;
			shard.files.push_back(fileInfo); 

			current_shard_size = 0.0;
			bytes_remaining[file_idx] = bytes_remaining[file_idx] - end_offset;
			start_offset = end_offset + 1;
		} 

		fileShards.push_back(shard);
		shard_idx++;
	}

	std::cout << "Shard count " << num_shards << std::endl;

	return true;
}
