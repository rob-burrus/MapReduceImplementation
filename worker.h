#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"
#include "masterworker.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>
#include <vector>
#include <string>
#include <sstream>
#include <map>
#include <fstream>


using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;

using masterworker::MapReduceMaster;
using masterworker::MapReduceQuery;
using masterworker::ShardFile;
using masterworker::MapReduceReply;


extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		class CallData {
	   	public:
			CallData(MapReduceMaster::AsyncService* service, ServerCompletionQueue* cq) : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
				proceed();
				//std::cout << "Version 6" << std::endl;
			}

			void proceed() {
				if (status_ == CREATE) {
					status_ = PROCESS;
					service_->RequestMapReduceTask(&ctx_, &query_, &responder_, cq_, cq_, this);
				} else if (status_ == PROCESS) {

					new CallData(service_, cq_);

					if (query_.mapping() == 1) {
						
						auto mapper = get_mapper_from_task_factory(query_.user_id());
						mapper->impl_->output_directory = query_.output_directory();
						mapper->impl_->n_output_files = query_.n_output_files();
						

						for(auto shard : query_.shard()) {
							std::string file_abs_path = shard.filename();
							
							int start_offset = shard.offset_start();
							int end_offset = shard.offset_end();
							
							std::ifstream file(file_abs_path, std::ios::in);
							if(file.is_open()) {
								std::string line;
								file.seekg (start_offset, file.beg);

								while(file.tellg() <= end_offset && file.tellg() != -1) {
									std::getline(file, line);
									mapper->map(line);
								}
							}

							file.close();
						}

						std::vector<std::string> file_paths = mapper->impl_->get_temp_files();
						for(auto filepath : file_paths) {
							ShardFile *output_file = reply_.add_output_files();
							output_file->set_filename(filepath);
						}
					} else {
						
						
						auto reducer = get_reducer_from_task_factory(query_.user_id());
						reducer->impl_->output_directory = query_.output_directory();
						reducer->impl_->result_num = query_.temp_file();
						
						std::map<std::string, std::vector<std::string>> word_map;

						for(auto shard: query_.shard()) {
							std::string file_path = shard.filename();
							std::ifstream file(file_path, std::ios::in);
							std::string line;
							if(file.is_open()) {
								while(getline(file, line)) {
									std::vector<std::string> line_split = split_string(line, ' ');
									std::string key = line_split.front();
									std::string value = line_split.back();
									word_map[key].push_back(value);
								}
							}
						}
						for(auto word: word_map) {
							reducer->reduce(word.first, word.second);
						}
						
					}
					
					reply_.set_complete(1);
					responder_.Finish(reply_, Status::OK, this);
					status_ = FINISH;
				} else {
					GPR_ASSERT(status_ == FINISH);
					delete this;
				}

			};

	   	private:
			MapReduceMaster::AsyncService* service_;
			ServerCompletionQueue* cq_;
			ServerContext ctx_;
			MapReduceQuery query_;
			MapReduceReply reply_;
			int request_type_;

			ServerAsyncResponseWriter<MapReduceReply> responder_;
			bool finish;
			enum CallStatus { CREATE, PROCESS, FINISH };
			CallStatus status_; 

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
	  };

		std::string ip_addr_port;
		MapReduceMaster::AsyncService service_;
		std::unique_ptr<Server>  server_;
		std::unique_ptr<ServerCompletionQueue> cq_;
		void handleRpcs();
};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
	ip_addr_port = ip_addr_port;
	ServerBuilder builder;
	builder.AddListeningPort(ip_addr_port, grpc::InsecureServerCredentials());
	builder.RegisterService(&service_);
	cq_ = builder.AddCompletionQueue();
	server_ = builder.BuildAndStart();
}

void Worker::handleRpcs() {
	new CallData(&service_, cq_.get());
    void* tag;
    bool ok;
    while (true) {
      GPR_ASSERT(cq_->Next(&tag, &ok));
      GPR_ASSERT(ok);
      static_cast<CallData*>(tag)->proceed();
    }
  }

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and
	BaseReduer's member BaseReducerInternal impl_ directly,
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	handleRpcs();
	return true;
}
