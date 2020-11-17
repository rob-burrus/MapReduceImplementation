#pragma once

#include <string>
#include <iostream>
#include <map>
#include <set>
#include <mutex>
#include <fstream>
#include <string>
#include <vector>

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {
	
	/* DON'T change this function's signature */
	BaseMapperInternal();

	/* DON'T change this function's signature */
	void emit(const std::string& key, const std::string& val);

	std::vector<std::string> get_temp_files();
	std::mutex file_lock;
	int n_output_files;
	std::string output_directory;
	std::set<std::string> temp_files_;

};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {
}

/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
	
	file_lock.lock();
	std::string temp_file = output_directory + "/temp" + std::to_string(std::hash<std::string>{}(key) % n_output_files) +".txt";

	std::ofstream file(temp_file, std::ios::app);
	if (file.is_open()) {
		file << key << " " << val << std::endl;
		file.close();
	}
	temp_files_.insert(temp_file);
	file_lock.unlock();
}

inline std::vector<std::string> BaseMapperInternal::get_temp_files() {
	std::vector<std::string> file_paths;
	for (auto entry : temp_files_) {
		file_paths.push_back(entry);
	}

	return file_paths;
}

/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

	/* DON'T change this function's signature */
	BaseReducerInternal();

	/* DON'T change this function's signature */
	void emit(const std::string& key, const std::string& val);

	/* NOW you can add below, data members and member functions as per the need of your implementation*/
	std::string output_directory;
	std::string result_num;
	std::mutex file_lock;
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}

/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	file_lock.lock();
	std::ofstream file(output_directory + "/result" + result_num, std::fstream::app);
	if (file.is_open()) {
		file << key << " " << val << "\n";
		file.flush();
		file.close();
	}
	
	file_lock.unlock();
}
