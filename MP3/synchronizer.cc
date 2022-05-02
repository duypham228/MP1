#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include "sns.grpc.pb.h"
#include "sync.grpc.pb.h"
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
using grpc::ClientContext; 
using grpc::ClientReader; 
using grpc::ClientReaderWriter;
using grpc::ClientWriter;

// Namespace for Synchronizer grpc
using csce438::SNSSynchronizer;
using csce438::followRequest;
using csce438::followReply;
using csce438::syncMessage;
using csce438::clientRequest;
using csce438::clientReply;

// Namespace for Service grpc
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;

// Namespace for Coordinator grpc
using csce438::SNSCoordinator;
using csce438::clientInfo;
using csce438::followerSyncInfo;
using csce438::followerSyncRTInfo;
using csce438::syncIdInfo;
using csce438::clusterInfo;

// Each F process need to communicate with other two F processes (3 server clusters total)
std::unique_ptr<SNSSynchronizer::Stub> stubSync1_;
std::unique_ptr<SNSSynchronizer::Stub> stubSync2_;

//Struct that stores relevant client information about their timeline files
struct Client {
  std::string username;
  int following_file_size;
  time_t last_mod_time;
  std::vector<std::string> newPosts;
  std::vector<std::string> followers;
  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }
};

//Struct that stores relevant client information about their following files
struct Client2 {
  std::string username;
  int following_file_lines;
  time_t last_mod_time;
  std::vector<std::string> newFollowing;
};

//Vector that stores every client that has been created for timeline checks
std::vector<Client> client_db;

//Vector that stores every client that has been created for following checks
std::vector<Client2> client_db_2;

//Helper function used to find a Client object given its username
int find_user(std::string username){
  int index = 0;
  for(Client c : client_db){
    if(c.username == username)
      return index;
    index++;
  }
  return -1;
}

//Helper function used to find a Client2 object given its username
int find_user_2(std::string username){
  int index = 0;
  for(Client2 c : client_db_2){
    if(c.username == username)
      return index;
    index++;
  }
  return -1;
}

class SNSSynchronizerImpl final : public SNSSynchronizer::Service {
	Status InformFollow(ServerContext* context, const followRequest* request, followReply* reply) override {
		std::string user = request->username();
		std::string follower = request->follower();
		std::ofstream masterfile("master" + user + "followerlist.txt", std::ios::app|std::ios::out|std::ios::in);
		std::string fileinput = follower + "\n";
		masterfile << fileinput;
		masterfile.close();
		std::ofstream slavefile("slave" + user + "followerlist.txt", std::ios::app|std::ios::out|std::ios::in);
		slavefile << fileinput;
		slavefile.close();
		
		return Status::OK;
	}
	
	Status InformTimeline(ServerContext* context, ServerReaderWriter<syncMessage, syncMessage>* stream) override {
		syncMessage message; 
		while (stream->Read(&message)) {
			for (int i = 0; i < message.followers().size(); i++) {
				std::ofstream outputfile3("master" + message.followers(i) + "following.txt", std::ios::app|std::ios::out|std::ios::in);
				outputfile3 << message.msg() << "\n";
				outputfile3.close();
				
				std::ofstream outputfile4("slave" + message.followers(i) + "following.txt", std::ios::app|std::ios::out|std::ios::in);
				outputfile4 << message.msg() << "\n";
				outputfile4.close();
				
				std::ofstream outputfile5("master" + message.followers(i) + "actualtimeline.txt", std::ios::app|std::ios::out|std::ios::in);
				outputfile5 << message.msg() << "\n";
				outputfile5.close();
				
				std::ofstream outputfile6("slave" + message.followers(i) + "actualtimeline.txt", std::ios::app|std::ios::out|std::ios::in);
				outputfile6 << message.msg() << "\n";
				outputfile6.close();
				
				int user_index = find_user(message.followers(i));
				if (user_index < 0) {
					std::cout << "Error InformTimeline" << std::endl;
				}
			}
		}
		return Status::OK;
	}
	
	Status AskForClients(ServerContext* context, const clientRequest* request, clientReply* reply) override {
		std::ifstream in("slave" + request->server_reply_id() + "clusterusers.txt");
		std::string line;
		while (getline(in, line)) {
			reply->add_clients(line);
		}
		
		return Status::OK;
	}
};

void RunServer(std::string hostname, std::string cport, std::string port, std::string server_id) {
  std::string server_address = "0.0.0.0:"+port;
  SNSSynchronizerImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  
  std::string login_info = hostname + ":" + cport;
  std::unique_ptr<SNSCoordinator::Stub> stubCoord_ = std::unique_ptr<SNSCoordinator::Stub>(SNSCoordinator::NewStub(
    grpc::CreateChannel(
      login_info, grpc::InsecureChannelCredentials())));
  
  ClientContext context1;  
  followerSyncRTInfo rtInfo;
  syncIdInfo idInfo;
  rtInfo.set_sync_ip("localhost");
  rtInfo.set_sync_port(port);
  rtInfo.set_sync_id(server_id);
  stubCoord_->FillFollowerSyncRT(&context1, rtInfo, &idInfo);
  
  clientInfo message;
  followerSyncInfo syncInfo;
  std::string sync_one;
  std::string sync_two;
  
  if (server_id == "1") {
  	sync_one = "2";
  	sync_two = "3";
  } else if (server_id == "2") {
  	sync_one = "1";
  	sync_two = "3";
  } else if (server_id == "3") {
  	sync_one = "1";
  	sync_two = "2";
  }
ClientContext context2; 
message.set_client_id(sync_one);
stubCoord_->GetFollowerSyncInfo(&context2, message, &syncInfo);
while ((syncInfo.sync_ip() == "") && (syncInfo.sync_port() == "")) {
    ClientContext context3;
    stubCoord_->GetFollowerSyncInfo(&context3, message, &syncInfo);
}
login_info = syncInfo.sync_ip() + ":" + syncInfo.sync_port();

stubSync1_ = std::unique_ptr<SNSSynchronizer::Stub>(SNSSynchronizer::NewStub(
grpc::CreateChannel(
    login_info, grpc::InsecureChannelCredentials())));

ClientContext context4;
message.set_client_id(sync_two);
stubCoord_->GetFollowerSyncInfo(&context4, message, &syncInfo);
while ((syncInfo.sync_ip() == "") && (syncInfo.sync_port() == "")) {
    ClientContext context5;
    stubCoord_->GetFollowerSyncInfo(&context5, message, &syncInfo);
}
login_info = syncInfo.sync_ip() + ":" + syncInfo.sync_port();

stubSync2_ = std::unique_ptr<SNSSynchronizer::Stub>(SNSSynchronizer::NewStub(
grpc::CreateChannel(
    login_info, grpc::InsecureChannelCredentials())));
      
  server->Wait();
}

void AddToClientDB(std::string username) {
	Client c;
	c.username = username;
  	std::string pathname = "./master" + username + ".txt";
  	struct stat sb;
 	if (stat(pathname.c_str(), &sb) != -1) {
  		c.last_mod_time = sb.st_mtime;
  		std::ifstream in("master" + username + ".txt");
  		int count = 0;
  		std::string line;
  		while (getline(in, line)) {
  			count++;
 		} // getline
  		c.following_file_size = count;
  	} else {
  		c.following_file_size = 0;
  	}
  	client_db.push_back(c);
}

void PeriodicFollowingCheck(std::string hostname, std::string cport, std::string server_id) {
  sleep(30);
        
  std::string login_info = hostname + ":" + cport;
  std::unique_ptr<SNSCoordinator::Stub> stubCoord_ = std::unique_ptr<SNSCoordinator::Stub>(SNSCoordinator::NewStub(
    grpc::CreateChannel(
      login_info, grpc::InsecureChannelCredentials())));
  
  int iterationCount = 0;
  while (true) {
  iterationCount++;
  syncIdInfo syncinfo;
  ClientContext context1;
  clusterInfo clusterinfo;
  syncinfo.set_sync_id(server_id);
  stubCoord_->GetClusterInfo(&context1, syncinfo, &clusterinfo);
  
  for (int i = 0; i < clusterinfo.cluster_clients().size(); i++) {
  	std::string username = clusterinfo.cluster_clients(i);
  	
  	int user_index = find_user_2(username);
  	if (user_index < 0) {
  		Client2 c;
  		c.username = username;
  		std::string pathname = "./slave" + username + "followinglist.txt";
  		struct stat sb;
  		if (stat(pathname.c_str(), &sb) != -1) {
  			c.last_mod_time = sb.st_mtime;
  			std::ifstream in("slave" + username + "followinglist.txt");
  			int count = 0;
  			std::string line;
  			while (getline(in, line)) {
  				c.newFollowing.push_back(line);
  				count++;
  			} // getline
  			c.following_file_lines = count;
  		} else {
  			c.following_file_lines = 0;
  		}
  		client_db_2.push_back(c);
  	} else {
  		Client2 *user = &client_db_2[user_index];
  		std::string pathname = "./slave" + user->username + "followinglist.txt";
  		struct stat sb;
  		if (stat(pathname.c_str(), &sb) != -1) {
  			if (sb.st_mtime != user->last_mod_time) {
  				user->last_mod_time = sb.st_mtime;
  				std::ifstream in("slave" + user->username + "followinglist.txt");
  				int count = 0;
  				std::string line;
  				while (getline(in, line)) {
  					count++;
  					if (count > user->following_file_lines) {
  						user->newFollowing.push_back(line);
  					}
  				}
  				user->following_file_lines = count;
  			}
  		}
  	}
  }
  
  for (int i = 0; i < clusterinfo.cluster_clients().size(); i++)  {
  	std::string username = clusterinfo.cluster_clients(i);
  	int user_index = find_user_2(username);
  	Client2 *user = &client_db_2[user_index];
  	
  	for (std::string following : user->newFollowing) {
  		ClientContext context2;
	  	clientInfo message;
	  	message.set_client_id(following);
	  	followerSyncInfo fsyncInfo;
		stubCoord_->GetFollowerSyncInfo(&context2, message, &fsyncInfo);
		
	  	if (server_id == fsyncInfo.sync_id()) {
	  		std::ofstream outputfile1("master" + following + "followerlist.txt", std::ios::app|std::ios::out|std::ios::in);
			outputfile1 << user->username << "\n";
			outputfile1.close();
			
			std::ofstream outputfile2("slave" + following + "followerlist.txt", std::ios::app|std::ios::out|std::ios::in);
			outputfile2 << user->username << "\n";
			outputfile2.close();
			
			
			continue;
	  	} // if
	  	ClientContext context3;
	  	followRequest req;
	  	req.set_username(following);
	  	req.set_follower(user->username);
	  	followReply rep;
	  	if (server_id == "1") {
			if (fsyncInfo.sync_id() == "2") {
				stubSync1_->InformFollow(&context3, req, &rep);
			}
			if (fsyncInfo.sync_id() == "3") {
				stubSync2_->InformFollow(&context3, req, &rep);
			}
		} else if (server_id == "2") {
			if (fsyncInfo.sync_id() == "1") {
				stubSync1_->InformFollow(&context3, req, &rep);
			}
			if (fsyncInfo.sync_id() == "3") {
				stubSync2_->InformFollow(&context3, req, &rep);
			}
		} else if (server_id == "3") {
			if (fsyncInfo.sync_id() == "1") {
				stubSync1_->InformFollow(&context3, req, &rep);
			}
			if (fsyncInfo.sync_id() == "2") {
				stubSync2_->InformFollow(&context3, req, &rep);
			}
		}
  	}
  	user->newFollowing.clear();
  } 
  sleep(30);
  } 
}

void PeriodicTimelineCheck(std::string hostname, std::string cport, std::string server_id) {
  sleep(50);
  ClientContext context1;
  std::shared_ptr<ClientReaderWriter<syncMessage, syncMessage>> syncOne_stream(
        stubSync1_->InformTimeline(&context1));
  ClientContext context2;
  std::shared_ptr<ClientReaderWriter<syncMessage, syncMessage>> syncTwo_stream(
        stubSync2_->InformTimeline(&context2));
        
  std::string login_info = hostname + ":" + cport;
  std::unique_ptr<SNSCoordinator::Stub> stubCoord_ = std::unique_ptr<SNSCoordinator::Stub>(SNSCoordinator::NewStub(
    grpc::CreateChannel(
      login_info, grpc::InsecureChannelCredentials())));
  //Might call infinite while loop after these stub initializations
  
  int iterationCount = 0;
  while (true) {
  iterationCount++;
  syncIdInfo syncinfo;
  ClientContext context3;
  clusterInfo clusterinfo;
  syncinfo.set_sync_id(server_id);
  stubCoord_->GetClusterInfo(&context3, syncinfo, &clusterinfo);	
  
  for (int i = 0; i < clusterinfo.cluster_clients().size(); i++) {
  	std::string username = clusterinfo.cluster_clients(i);
  	
  	int user_index = find_user(username);
  	if (user_index < 0) {
  		Client c;
  		c.username = username;
  		std::string pathname = "./slave" + username + ".txt";
  		struct stat sb;
  		if (stat(pathname.c_str(), &sb) != -1) {
  			c.last_mod_time = sb.st_mtime;
  			std::ifstream in("slave" + username + ".txt");
  			int count = 0;
  			std::string line;
  			while (getline(in, line)) {
  				count++;
  			} // getline
  			c.following_file_size = count;
  		} else {
  			c.following_file_size = 0;
  		} 
  		client_db.push_back(c);
  	} else {
  		Client *user = &client_db[user_index];
  		std::string pathname = "./slave" + user->username + ".txt";
  		struct stat sb;
  		if (stat(pathname.c_str(), &sb) != -1) {
  			if (sb.st_mtime != user->last_mod_time) {
  				user->last_mod_time = sb.st_mtime;
  				/*
  				std::vector<std::string> newTimelinePosts;
  				std::vector<std::string> clientFollowers;
  				*/
  				
  				std::ifstream in("slave" + user->username + ".txt");
  				int count = 0;
  				std::string line;
  				while (getline(in, line)) {
  					count++;
  					if (count > user->following_file_size) {
  						user->newPosts.push_back(line);
  					}
  				}
  				user->following_file_size = count;
  				
  				std::ifstream in2("slave" + user->username + "followerlist.txt");
  				while (getline(in2, line)) {
  					user->followers.push_back(line);
  				}
  				
  				
  			}
  		}
  	}
  }
  
  for (int i = 0; i < clusterinfo.cluster_clients().size(); i++) {
  	std::string username = clusterinfo.cluster_clients(i);
  	int user_index = find_user(username);
  	Client *user = &client_db[user_index];
	
	for (std::string follower : user->followers) {
		ClientContext context4;
		clientInfo message;
		message.set_client_id(follower);
		followerSyncInfo syncInfo;
		stubCoord_->GetFollowerSyncInfo(&context4, message, &syncInfo);
		for (std::string post : user->newPosts) {
			syncMessage m;
			m.set_username(user->username);
			m.set_msg(post);
			m.add_followers(follower);
			// Might need to update following_file_size of clients
			if (server_id == syncInfo.sync_id()) {
				if (find_user(follower) < 0) {
					AddToClientDB(follower);
				}
				std::ofstream outputfile3("master" + follower + "following.txt", std::ios::app|std::ios::out|std::ios::in);
				outputfile3 << post << "\n";
				outputfile3.close();
				
				std::ofstream outputfile4("slave" + follower + "following.txt", std::ios::app|std::ios::out|std::ios::in);
				outputfile4 << post << "\n";
				outputfile4.close();
				
				std::ofstream outputfile5("master" + follower + "actualtimeline.txt", std::ios::app|std::ios::out|std::ios::in);
				outputfile5 << post << "\n";
				outputfile5.close();
				
				std::ofstream outputfile6("slave" + follower + "actualtimeline.txt", std::ios::app|std::ios::out|std::ios::in);
				outputfile6 << post << "\n";
				outputfile6.close();
				
				continue;
			}
			if (server_id == "1") {
				if (syncInfo.sync_id() == "2") {
					syncOne_stream->Write(m);
				}
				if (syncInfo.sync_id() == "3") {
					syncTwo_stream->Write(m);
				}
			} else if (server_id == "2") {
				if (syncInfo.sync_id() == "1") {
					syncOne_stream->Write(m);
				}
				if (syncInfo.sync_id() == "3") {
					syncTwo_stream->Write(m);
				}
			} else if (server_id == "3") {
				if (syncInfo.sync_id() == "1") {
					syncOne_stream->Write(m);
				}
				if (syncInfo.sync_id() == "2") {
					syncTwo_stream->Write(m);
				}
			}
		}
	}
	user->newPosts.clear();
	user->followers.clear();
  }
  sleep(30);
  }
}

void PeriodicUsersCheck(std::string server_id) {
  std::ofstream initialuser_file("slave" + server_id + "users.txt", std::ios::app|std::ios::out|std::ios::in);
  initialuser_file.close();
  std::ofstream initialuser_file2("master" + server_id + "users.txt", std::ios::app|std::ios::out|std::ios::in);
  initialuser_file2.close();
  sleep(30);
  while (true) {
    std::ofstream user_file;
    user_file.open("slave" + server_id + "users.txt",std::ofstream::out|std::ofstream::trunc);
    user_file.close();
    std::ofstream user_file3;
    user_file3.open("master" + server_id + "users.txt",std::ofstream::out|std::ofstream::trunc);
    user_file.close();
    std::ofstream user_file2("slave" + server_id + "users.txt",std::ios::app|std::ios::out|std::ios::in);
    std::ifstream in("slave" + server_id + "clusterusers.txt");
    std::string line;
    while (getline(in, line)) {
        user_file2 << line << std::endl;
        user_file3 << line << std::endl;
    }
    
    ClientContext context;
    clientRequest req;
    req.set_server_request_id(server_id);
    clientReply rep;
	  	
    if (server_id == "1") {
            ClientContext context1;
            clientRequest req1;
            clientReply rep1;
            req1.set_server_request_id(server_id);
            req1.set_server_reply_id("2");
            stubSync1_->AskForClients(&context1, req1, &rep1);
            for (int i = 0; i < rep1.clients().size(); i++) {
                user_file2 << rep1.clients(i) << std::endl;
                user_file3 << rep1.clients(i) << std::endl;
            }
            ClientContext context2;
            clientRequest req2;
            clientReply rep2;
            req2.set_server_request_id(server_id);
            req2.set_server_reply_id("3");
            stubSync1_->AskForClients(&context2, req2, &rep2);
            for (int i = 0; i < rep2.clients().size(); i++) {
                user_file2 << rep2.clients(i) << std::endl;
                user_file3 << rep2.clients(i) << std::endl;
            }
    } else if (server_id == "2") {
            ClientContext context1;
            clientRequest req1;
            clientReply rep1;
            req1.set_server_request_id(server_id);
            req1.set_server_reply_id("1");
            stubSync1_->AskForClients(&context1, req1, &rep1);
            for (int i = 0; i < rep1.clients().size(); i++) {
                user_file2 << rep1.clients(i) << std::endl;
                user_file3 << rep1.clients(i) << std::endl;
            }
            ClientContext context2;
            clientRequest req2;
            clientReply rep2;
            req2.set_server_request_id(server_id);
            req2.set_server_reply_id("3");
            stubSync1_->AskForClients(&context2, req2, &rep2);
            for (int i = 0; i < rep2.clients().size(); i++) {
                user_file2 << rep2.clients(i) << std::endl;
                user_file3 << rep2.clients(i) << std::endl;
            }

    } else if (server_id == "3") {
            ClientContext context1;
            clientRequest req1;
            clientReply rep1;
            req1.set_server_request_id(server_id);
            req1.set_server_reply_id("1");
            stubSync1_->AskForClients(&context1, req1, &rep1);
            for (int i = 0; i < rep1.clients().size(); i++) {
                user_file2 << rep1.clients(i) << std::endl;
                user_file3 << rep1.clients(i) << std::endl;
            }
            ClientContext context2;
            clientRequest req2;
            clientReply rep2;
            req2.set_server_request_id(server_id);
            req2.set_server_reply_id("2");
            stubSync1_->AskForClients(&context2, req2, &rep2);
            for (int i = 0; i < rep2.clients().size(); i++) {
                user_file2 << rep2.clients(i) << std::endl;
                user_file3 << rep2.clients(i) << std::endl;
            }
    } 
    sleep(30);
  }
}

int main(int argc, char** argv) {
    std::string hostname = "localhost";
    std::string port = "3010";
    std::string server_id = "0";
    std::string cport = "5050";

    int opt = 0;
    while ((opt = getopt(argc, argv, "h:c:p:i:")) != -1){
        switch(opt) {
            case 'h': //Coordinator IP
                hostname = optarg;break;
            case 'c': //Coordinator Port
                cport = optarg;break;
            case 'p': //Port
                port = optarg;break;
            case 'i':
                server_id = optarg;break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }
    
    std::thread thread1(RunServer, hostname, cport, port, server_id);
    std::thread thread2(PeriodicTimelineCheck, hostname, cport, server_id);
    std::thread thread3(PeriodicFollowingCheck, hostname, cport, server_id);
    std::thread thread4(PeriodicUsersCheck, server_id);
    
    thread1.join();
    thread2.join();
    thread3.join();
    thread4.join();

}
