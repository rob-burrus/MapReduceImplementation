#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"
#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"
#include <grpcpp/grpcpp.h>
#include <mutex>
#include <chrono>
#include <thread>
#include <sys/stat.h>

using masterworker::MapReduceMaster;
using masterworker::ShardFile;
using masterworker::MapReduceQuery;
using masterworker::MapReduceReply;

using grpc::Channel;
using grpc::CompletionQueue;
using grpc::ClientContext;
using grpc::ClientAsyncResponseReader;
using grpc::Status;
using grpc::StatusCode;

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:
		enum TaskStatus { UNASSIGNED = 0, IN_PROGRESS = 1, COMPLETE = 2 };
		enum TaskType { MAP, REDUCE };
		struct TaskInfo {
			TaskStatus status;
			TaskType task_type;
			FileShard shard;
			int temp_file;
			std::chrono::system_clock::time_point started;
		};

		
		struct WorkerInfo {
			int retries;
			std::string ip_addr;
			int id;
		};

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		MapReduceSpec mr_spec_;
		std::vector<std::thread> threads_;
		std::vector<FileShard> file_shards_;
		std::vector<WorkerInfo> workers_;
		std::vector<TaskInfo> tasks_;
		
		std::map<int, std::vector<std::string>> temp_files;
		std::mutex task_lock;
		std::mutex temp_file_lock;

		void run_tasks(WorkerInfo worker);
		bool mapping = true;
};

void Master::run_tasks(WorkerInfo worker){
	
	while (1) {
		if (worker.retries == 0) { return; }

		int task = -1;
		bool complete = true;
		task_lock.lock();
		
		for (int i = 0; i < tasks_.size(); i++) {

			if(tasks_[i].status == UNASSIGNED){
				task = i;
				tasks_[i].status = IN_PROGRESS;
				tasks_[i].started = std::chrono::system_clock::now() + std::chrono::seconds(3);
				complete = false;
				break;
			}
			if(tasks_[i].status == IN_PROGRESS) {
				complete = false;
				if (tasks_[i].started < std::chrono::system_clock::now()) {
					tasks_[i].status = UNASSIGNED;
				}
			}
		}
		
		task_lock.unlock();
		if(complete) {
			return;
		} else if(task !=- 1){
			//std::cout << "Task: " << task << std::endl;
			MapReduceQuery query;
			query.set_user_id(mr_spec_.user_id);

			if (mapping) {
				for (auto file : tasks_[task].shard.files) {
					ShardFile* shard_file = query.add_shard();
					shard_file->set_filename(file.filename);
					shard_file->set_offset_start(file.offset_start);
					shard_file->set_offset_end(file.offset_end);
				}
				query.set_mapping(1);
				
				std::string output_dir = mr_spec_.output_dir + "/temp" + std::to_string(worker.id);
				mkdir(output_dir.c_str(), 0777);
				query.set_output_directory(output_dir);
				query.set_n_output_files(mr_spec_.n_output_files);
			} else {
				std::vector<std::string> file_paths = temp_files[tasks_[task].temp_file];
				query.set_temp_file(std::to_string(tasks_[task].temp_file));
				for(int i=0; i< file_paths.size(); i++){
					ShardFile* file = query.add_shard();
					file->set_filename(file_paths[i]);
				}

				query.set_mapping(0);
				query.set_output_directory(mr_spec_.output_dir);
			}

			ClientContext ctx;
			CompletionQueue cq;
			Status status;
			MapReduceReply reply;
			std::unique_ptr<MapReduceMaster::Stub> stub = MapReduceMaster::NewStub(grpc::CreateChannel(worker.ip_addr, grpc::InsecureChannelCredentials()));
			std::unique_ptr<ClientAsyncResponseReader<MapReduceReply>> reader(stub->PrepareAsyncMapReduceTask(&ctx, query, &cq));
			reader->StartCall();

			reader->Finish(&reply, &status, (void *)1);

			void* got_tag;
			bool ok = false;
			cq.Next(&got_tag, &ok);
			if (status.ok() && reply.complete() == 1) {
				if (mapping) {
					for (ShardFile output_file : reply.output_files()) {
					
						std::string file_path = output_file.filename();
						int index = file_path.find('.');
						int file_num = std::stoi(file_path.substr(index-1, index));

						temp_file_lock.lock();
						temp_files[file_num].push_back(file_path);
						temp_file_lock.unlock();
					}
				}
				task_lock.lock();
				tasks_[task].status = COMPLETE;
				task_lock.unlock();
				
			} else {
				task_lock.lock();
				tasks_[task].status = UNASSIGNED;
				task_lock.unlock();
				worker.retries--;
				
			}
		} else {
			std::this_thread::yield();
		}

	}
}

/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {

	mr_spec_ = mr_spec;
	file_shards_ = file_shards;
	
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {

	// Create worker objects for bookkeeping about about the real Workers
	int id = 0;
	for(auto worker_ipaddr : mr_spec_.worker_ipaddr_ports) {
		WorkerInfo worker;
		worker.ip_addr = worker_ipaddr;
		worker.retries = 5;
		worker.id = id++;
		workers_.push_back(worker);
	}

	// Create mapping tasks to eb executed by the workers
	for (auto shard: file_shards_) {
		TaskInfo task_info;
		task_info.status = UNASSIGNED;
		task_info.task_type = MAP;
		task_info.shard = shard;
		tasks_.push_back(task_info);
	}


	for(auto worker : workers_) {
		std::thread t(&Master::run_tasks, this, worker);
		threads_.push_back(std::move(t));
	}

	for (auto& th : threads_) {
		th.join();
	}
	threads_.clear();
	tasks_.clear();
	mapping = false;

	// Create reduce tasks to be executede by the workers
	for (int i = 0; i < mr_spec_.n_output_files; i++) {
		TaskInfo task_info;
		task_info.status = UNASSIGNED;
		task_info.task_type = REDUCE;
		task_info.temp_file = i;
		tasks_.push_back(task_info);
	}


	for(auto worker : workers_) {
		
		std::thread t(&Master::run_tasks, this, worker);
		threads_.push_back(std::move(t));
	}

	// Wait for reduce to complete
	for (auto& th : threads_) {
		th.join();
	}

	return true;
}