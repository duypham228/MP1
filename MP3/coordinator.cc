// Source Code to start Coordinator Server
// Similar structure as Server but only need to handle Login Command

#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include "sns.grpc.pb.h"
#include "coord.grpc.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;

// Namespace for Coordinator rpc
using csce438::coordMessage;
using csce438::coordRequest;
using csce438::coordReply;

using csce438::clientInfo;
using csce438::followerSyncInfo;
using csce438::followerSyncRTInfo;
using csce438::syncIdInfo;
using csce438::clusterInfo;

using csce438::masterReply;
using csce438::masterRequest;
using csce438::SNSCoordinator;


// Data Structure to manage Coordinator and its routing table metadata
std::vector<std::string> client_db_1;
std::vector<std::string> client_db_2;
std::vector<std::string> client_db_3;

std::unordered_map<std::string, int> routingTable;
std::string Master_RT[3][3];
std::string Slave_RT[3][3];
std::string Follower_RT[3][3];

class SNSCoordinatorImpl final : public SNSCoordinator::Service {
    Status coordLogin(ServerContext* context, const coordRequest* request, coordReply* reply) override {
        int client_id = std::stoi(request->client_id());
        
        // Update that client to cluster client database
        if (((client_id % 3) - 1) == 0) {
        	client_db_1.push_back(request->client_id());
        } else if (((client_id % 3) - 1) == 1) {
        	client_db_2.push_back(request->client_id());
        } else if (((client_id % 3) - 1) == -1) {
        	client_db_3.push_back(request->client_id());
        }

        // Get Available Server
        std::vector<std::string> RT = getServer(client_id);
        
        // Reply Available back to Client to Connect To
        reply->set_server_ip(RT[0]);
        reply->set_server_port(RT[1]);
        
        return Status::OK;
    }

    Status Heartbeat(ServerContext* context, ServerReaderWriter<coordMessage, coordMessage>* stream) override {
        coordMessage message;
        std::string server_port;
        bool initialHB = false;
        while (1) {
            if (stream->Read(&message) && initialHB == false) {
                initialHB = true;
                std::string server_ip = message.server_ip();
                server_port = message.server_port();
                std::string server_type = message.server_type();
                std::string server_address = server_ip + ":" + server_port;
                if (routingTable.find(server_address) == routingTable.end()) {
                    routingTable[server_address] = 1;
                    // Populate Routing Table
                    if (server_type == "master") {
                        int index = -1;
                        for (int i = 0; i < 3; i++) {
                            if (Master_RT[i][0] == "") {
                                index = i;
                                break;
                            }
                        }
                        if (index != -1) { 
                            Master_RT[index][0] = server_port;
                            Master_RT[index][1] = "Active";
                            Master_RT[index][2] = server_ip;
                        }
                    } else {
                        int index = -1;
                        for (int i = 0; i < 3; i++) {
                            if (Slave_RT[i][0] == "") {
                                index = i;
                                break;
                            }
                        }
                        if (index != -1) { 
                            Slave_RT[index][0] = server_port;
                            Slave_RT[index][1] = "Active";
                            Slave_RT[index][2] = server_ip;
                        }
                    }
                }            
            }
            if (!(stream->Read(&message))) {
                // Update Routing Table
                for (int i = 0; i < 3; i++) {
                    if (Master_RT[i][0] == server_port) {
                        Master_RT[i][1] = "Inactive";
                        break;
                    }
                }
            }
            sleep(20);
        }
        return Status::OK;
    }

    Status GetSlaveInfo (ServerContext* context, const masterRequest* request, masterReply* reply) {
    	reply->set_slave_ip(Slave_RT[std::stoi(request->server_id()) - 1][2]);
    	reply->set_slave_port(Slave_RT[std::stoi(request->server_id()) - 1][0]);
    	return Status::OK;
    }

    Status FillFollowerSyncRT (ServerContext* context, const followerSyncRTInfo* request, syncIdInfo* reply) {
    	int index = std::stoi(request->sync_id()) - 1;
    	Follower_RT[index][0] = request->sync_port();
    	Follower_RT[index][1] = "Active";
    	Follower_RT[index][2] = request->sync_ip();
    	reply->set_sync_id(request->sync_id());
    	return Status::OK;
    }

    Status GetFollowerSyncInfo (ServerContext* context, const clientInfo* clientInfo, followerSyncInfo* followerSyncInfo) {
    	int client_id = std::stoi(clientInfo->client_id());
    	int follower_sync_id = (client_id % 3) - 1;
    	if (follower_sync_id == -1) {
    		follower_sync_id = 2;
    	}
    	followerSyncInfo->set_sync_ip(Follower_RT[follower_sync_id][2]);
    	followerSyncInfo->set_sync_port(Follower_RT[follower_sync_id][0]);
    	followerSyncInfo->set_sync_id(std::to_string(follower_sync_id + 1));
    	
    	return Status::OK;
    }

    Status GetClusterInfo (ServerContext* context, const syncIdInfo* syncinfo, clusterInfo* clusterinfo) {
    	int cluster_id = ((std::stoi(syncinfo->sync_id())) % 3) - 1;
    	if (cluster_id == 0) {
    		for (std::string client : client_db_1) {
    			clusterinfo->add_cluster_clients(client);
    		}
    	} else if (cluster_id == 1) {
    		for (std::string client : client_db_2) {
    			clusterinfo->add_cluster_clients(client);
    		}
    	} else if (cluster_id == -1) {
    		for (std::string client : client_db_3) {
    			clusterinfo->add_cluster_clients(client);
    		}
    	}
    	return Status::OK;
    	
    }

    // Get Available Server for Client based on getServer Algorithms in Handout
    std::vector<std::string> getServer(int client_id){
        std::vector<std::string> server_address(2);
        int server_id = (client_id % 3) - 1;
        if (server_id == -1) {
        	server_id = 2;
        }
        if (Master_RT[server_id][1] == "Active") {
            server_address[0]=Master_RT[server_id][2];
            server_address[1]=Master_RT[server_id][0];
            return server_address;
        } else{
            server_address[0]=Slave_RT[server_id][2];
            server_address[1]=Slave_RT[server_id][0];
            return server_address;
        }
    }
};




void RunServer(std::string port_no) {
  std::string server_address = "0.0.0.0:"+port_no;
  SNSCoordinatorImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  server->Wait();
}

int main(int argc, char** argv) {
  
  std::string port = "3010";
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1){
    switch(opt) {
      case 'p':
          port = optarg;break;
      default:
	  std::cerr << "Invalid Command Line Argument\n";
    }
  }
  RunServer(port);

  return 0;
}



