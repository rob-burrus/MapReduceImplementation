#pragma once

#include <string>
#include <vector>
#include <assert.h>
#include <iostream>
#include <sstream>
#include <fstream>

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
	int n_workers;
	std::vector<std::string> worker_ipaddr_ports;
	std::vector<std::string> input_files;
	std::string output_dir;
	int n_output_files;
	int map_kilobytes;
	std::string user_id;
};

// Reference: https://www.fluentcpp.com/2017/04/21/how-to-split-a-string-in-c/
inline std::vector<std::string> split_string(const std::string& s, char delimeter) {
	std::vector<std::string> result;
	std::istringstream ss(s);
	std::string item;
	while(getline(ss, item, delimeter)) {
    //std::cout << "Split item: " << item << std::endl;
		result.push_back(item);
	}
	return result;
}

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
	std::ifstream config_file(config_filename);
  
	if (config_file.is_open()) {
		std::string line;
		while(getline(config_file, line)) {
			std::vector<std::string> line_split = split_string(line, '=');
      
			std::string key = line_split.front();
			std::string value = line_split.back();
			if (key.compare("n_workers") == 0) {
				mr_spec.n_workers = stoi(value);
				
			} else if (key.compare("worker_ipaddr_ports") == 0) {
				mr_spec.worker_ipaddr_ports = split_string(value, ',');
				
			} else if (key.compare("input_files") == 0) {
				mr_spec.input_files = split_string(value, ',');
				
				
			} else if (key.compare("output_dir") == 0) {
				mr_spec.output_dir = value;
				
			} else if (key.compare("n_output_files") == 0) {
				mr_spec.n_output_files = stoi(value);
				
			} else if (key.compare("map_kilobytes") == 0) {
				mr_spec.map_kilobytes = stoi(value);
				
			} else if (key.compare("user_id") == 0) {
				mr_spec.user_id = value;
				
			}
		}

		/*
		std::cout << "n_workers: " << mr_spec.n_workers << std::endl;
		for (auto port : mr_spec.worker_ipaddr_ports) {
			std::cout << "worker_ipaddr_port: " << port << std::endl;
		}
		
		for (auto file : mr_spec.input_files) {
			std::cout << "input_file: " << file << std::endl;
		}
		std::cout << "output_dir: " << mr_spec.output_dir << std::endl;
		std::cout << "n_output_files: " << mr_spec.n_output_files << std::endl;
		std::cout << "map_kilobytes: " << mr_spec.map_kilobytes << std::endl;
		std::cout << "user_id: " << mr_spec.user_id << std::endl;
		*/

		config_file.close();
		return true;
	} else {
		return false;
	}
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
	if (mr_spec.n_workers <= 0 || mr_spec.n_workers != mr_spec.worker_ipaddr_ports.size()) {
		std::cout << "Number of workers should match number of worker IP addr ports" << std::endl;
		exit(-1);
	}
	if (mr_spec.n_output_files <= 0) {
		std::cout << "Number of output files should be more than 0" << std::endl;
		exit(-1);
	}
	if (mr_spec.map_kilobytes <= 0) {
		std::cout << "Map kilobytes should be more than 0" << std::endl;
		exit(-1);
	}
	if (mr_spec.output_dir.c_str() == nullptr) {
		std::cout << "Output directory not provided" << std::endl;
		exit(-1);
	}


	if(mr_spec.user_id.c_str() == nullptr) {
		std::cout << "No user_id provided" << std::endl;
		exit(-1);
	}

	for (auto& input_file : mr_spec.input_files)
	{
		std::ifstream file(input_file);
		if (!file.is_open()) {
			std::cout << "Unable to open input file" << std::endl;
			exit(-1);
		} else {
			file.close();
		}
		
	}
	

	return true;
}
