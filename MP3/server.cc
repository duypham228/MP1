/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <chrono>
#include <thread>
#include <sys/types.h>
#include <sys/stat.h>
#include <map>
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

// Namespace for Coordinator grpc
using csce438::coordRequest;
using csce438::coordReply;
using csce438::coordMessage;
using csce438::masterRequest;
using csce438::masterReply;
using csce438::syncIdInfo;
using csce438::clusterInfo;
using csce438::SNSCoordinator;

// Server act both as "a server" to client and as "a client" to Coordinator
using grpc::ClientContext;
using grpc::ClientReader; 
using grpc::ClientReaderWriter;
using grpc::ClientWriter;

std::string t = "master";
std::string identifier;

struct Client {
  std::string username;
  bool connected = true;
  int following_file_size = 0;
  time_t last_mod_time;
  int timeline_file_size = 0;
  std::vector<Client*> client_followers;
  std::vector<Client*> client_following;
  ServerReaderWriter<Message, Message>* stream = 0;
  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }
};

//Vector that stores every client that has been created
std::vector<Client> client_db;

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

class SNSServiceImpl final : public SNSService::Service {
// Each Master Server need a stub to communicate with the slave, since Client "ONLY" interact with master
public:
std::unique_ptr<SNSService::Stub> stubSlave_;

private:
  Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
    if (t == "master") {
      ClientContext mastercontext;
      Request masterrequest;
      masterrequest.set_username(request->username());
      ListReply masterreply;
      stubSlave_->List(&mastercontext, masterrequest, &masterreply);
    }
    
    std::string userLine;
    std::ifstream users(t + identifier + "users.txt");
    while (getline(users, userLine)) {
    	list_reply->add_all_users(userLine);
    }
    
    std::ifstream follower_file(t + request->username() + "followerlist.txt");
    std::string followerLine;
    while (getline(follower_file, followerLine)) {
      list_reply->add_followers(followerLine);
    }
    return Status::OK;
  }

  Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
    if (t == "master") {
      ClientContext mastercontext;
      Reply masterreply;
      stubSlave_->Follow(&mastercontext, *request, &masterreply);
    }
    
    std::string username1 = request->username();
    std::string username2 = request->arguments(0);
    
    if(username1 == username2) {
      reply->set_msg("Join Failed -- Invalid Username");
    } else{
      std::ifstream in(t + username1 + "followinglist.txt");
      std::string line;
      while (getline(in, line)) {
      	if (line == username2) {
      		reply->set_msg("Join Failed -- Already Following User");
      		return Status::OK;
      	}
      }
      
      std::ofstream following_file(t + username1 + "followinglist.txt", std::ios::app|std::ios::out|std::ios::in);
      std::string fileinput = username2 + "\n";
      following_file << fileinput;
      following_file.close();

      reply->set_msg("Successfully Joined");
    }

    return Status::OK; 
  }

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {
    if (t == "master") {
    ClientContext mastercontext;
    Reply masterreply;
      stubSlave_->UnFollow(&mastercontext, *request, &masterreply);
    }
    
    std::string username1 = request->username();
    std::string username2 = request->arguments(0);
    int leave_index = find_user(username2);
    if(leave_index < 0 || username1 == username2)
      reply->set_msg("unknow follower username");
    else{
      Client *user1 = &client_db[find_user(username1)];
      Client *user2 = &client_db[leave_index];
      if(std::find(user1->client_following.begin(), user1->client_following.end(), user2) == user1->client_following.end()){
	      reply->set_msg("you are not follower");
        return Status::OK;
      }
      user1->client_following.erase(find(user1->client_following.begin(), user1->client_following.end(), user2)); 
      user2->client_followers.erase(find(user2->client_followers.begin(), user2->client_followers.end(), user1));
      reply->set_msg("UnFollow Successful");
    }
    return Status::OK;
  }
  
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {
    if (t == "master") {
      ClientContext mastercontext;
      Reply masterreply;
      stubSlave_->Login(&mastercontext, *request, &masterreply);
    }
    
    Client c;
    std::string username = request->username();
    int user_index = find_user(username);
    if(user_index < 0){
      c.username = username;
      client_db.push_back(c);
      reply->set_msg("Login Successful!");
      std::ofstream cluster_file(t + identifier + "clusterusers.txt",std::ios::app|std::ios::out|std::ios::in);
      cluster_file << username << std::endl;
    } else { 
      Client *user = &client_db[user_index];
      if(user->connected)
        reply->set_msg("Invalid Username");
      else{
        std::string msg = "Welcome Back " + user->username;
	      reply->set_msg(msg);
        user->connected = true;
        std::ofstream cluster_file(t + identifier + "clusterusers.txt",std::ios::app|std::ios::out|std::ios::in);
        cluster_file << username << std::endl;
      }
    }
    return Status::OK;
  }

  Status Timeline(ServerContext* context, 
		ServerReaderWriter<Message, Message>* stream) override {
    std::cout << "Timeline t is: " << t << std::endl;
    ClientContext mastercontext;
    
    std::shared_ptr<ClientReaderWriter<Message, Message>> streamSlave; 
    if (t == "master") {
    	streamSlave = std::shared_ptr<ClientReaderWriter<Message, Message>> (stubSlave_->SlaveTimeline(&mastercontext));
    }
	
    Message message;
    Client *c;
    while(stream->Read(&message)) {
      if (t == "master") {
        Message messageClone;
        messageClone.set_username(message.username());
        messageClone.set_msg(message.msg());
        //set timestamp
        google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
        timestamp->set_seconds(message.timestamp().seconds());
        timestamp->set_nanos(0);
        messageClone.set_allocated_timestamp(timestamp);
      	streamSlave->Write(messageClone);
      }
      std::string username = message.username();
      int user_index = find_user(username);
      c = &client_db[user_index];
 
      //Write the current message to "username.txt"
      std::string filename = t + username+".txt";
      std::ofstream user_file(filename,std::ios::app|std::ios::out|std::ios::in);
      std::ofstream actualtimeline_file(t + username+"actualtimeline.txt",std::ios::app|std::ios::out|std::ios::in);
      google::protobuf::Timestamp temptime = message.timestamp();
      std::string time = google::protobuf::util::TimeUtil::ToString(temptime);
      std::string fileinput = time+" :: "+message.username()+":"+message.msg()+"\n";
      if(message.msg() != "Set Stream") {
        user_file << fileinput;
        actualtimeline_file << fileinput;
        std::string pathname = "./" + t + username + ".txt"; 
      } else{
        if(c->stream==0)
      	  c->stream = stream;
        
        std::string line;
        std::vector<std::string> newest_twenty;
        std::ifstream in(t + username+"actualtimeline.txt");
        int count = 0;
        
        while(getline(in, line)){
          count++;
        }
        in.close();
        
        std::ifstream in2(t + username+"actualtimeline.txt");
        int count2 = 0;
        while(getline(in2, line)){
          if(count > 20){
	          if(count2 < count-20){
              count2++;
	            continue;
            }
          }
          newest_twenty.push_back(line);
        }
        
        Message new_msg; 
        for(int i = 0; i<newest_twenty.size(); i++){
          new_msg.set_msg(newest_twenty[i]);
          stream->Write(new_msg);
        }    
        continue;
      } 
    }
    // Client Disconnected
    c->connected = false;
    std::ofstream cluster_file;
    cluster_file.open(t + identifier + "clusterusers.txt",std::ofstream::out|std::ofstream::trunc);
    cluster_file.close();
    std::ofstream cluster_file2(t + identifier + "clusterusers.txt",std::ios::app|std::ios::out|std::ios::in);
    for (Client c : client_db) {
    	if (c.connected == true) {
    		cluster_file2 << c.username << std::endl; 
    	}
    }
    return Status::OK;
  }

  Status SlaveTimeline(ServerContext* context, 
		ServerReaderWriter<Message, Message>* stream) override {
    Message message;
    Client *c;
    while(stream->Read(&message)) {
      std::string username = message.username();
      int user_index = find_user(username);
      c = &client_db[user_index];
 
      //Write the current message to "username.txt"
      std::string filename = t + username+".txt";
      std::ofstream user_file(filename,std::ios::app|std::ios::out|std::ios::in);
      std::ofstream actualtimeline_file(t + username+"actualtimeline.txt",std::ios::app|std::ios::out|std::ios::in);
      google::protobuf::Timestamp temptime = message.timestamp();
      std::string time = google::protobuf::util::TimeUtil::ToString(temptime);
      std::string fileinput = time+" :: "+message.username()+":"+message.msg()+"\n";
      if(message.msg() != "Set Stream") {
        user_file << fileinput;
        actualtimeline_file << fileinput;
        std::string pathname = "./" + t + username + ".txt";
  
        std::cout << "At " << pathname << " timeline, the server wrote in: " << fileinput << std::endl; 
      //If message = "Set Stream", print the first 20 chats from the people you follow
      } else{
        if(c->stream==0)
      	  c->stream = stream;
      	std::string pathname = "./" + t + username + ".txt";
      
        std::string line;
        std::vector<std::string> newest_twenty;
        std::ifstream in(t + username+"actualtimeline.txt");
        int count = 0;
  
        while(getline(in, line)){
          count++;
        } // while
        in.close();
        
        std::ifstream in2(t + username+"actualtimeline.txt");
        int count2 = 0;
        while(getline(in2, line)) {
          if(count > 20){
            if(count2 < count-20){
              count2++;
              continue;
            }
          }
          newest_twenty.push_back(line);
        }
        
        Message new_msg; 

        for(int i = 0; i<newest_twenty.size(); i++){
          new_msg.set_msg(newest_twenty[i]);
          stream->Write(new_msg);
        }    
        continue;
      }
      
    } 
    c->connected = false;
    std::ofstream cluster_file;
    cluster_file.open(t + identifier + "clusterusers.txt",std::ofstream::out|std::ofstream::trunc);
    cluster_file.close();
    std::ofstream cluster_file2(t + identifier + "clusterusers.txt",std::ios::app|std::ios::out|std::ios::in);
    for (Client c : client_db) {
    	if (c.connected == true) {
    		cluster_file2 << c.username << std::endl; 
    	}
    }
    
    return Status::OK;
  }

};

void RunServer(std::string hostname, std::string cport, std::string port_no, std::string server_id) {
  std::string server_address = "localhost:"+port_no;
  SNSServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  if (t == "master") {
    std::string login_info = hostname + ":" + cport;
    std::unique_ptr<SNSCoordinator::Stub> stubCoord_ = std::unique_ptr<SNSCoordinator::Stub>(SNSCoordinator::NewStub(
      grpc::CreateChannel(
        login_info, grpc::InsecureChannelCredentials())));
    ClientContext context;
    masterRequest request;
    request.set_server_id(server_id);
    masterReply reply;
    stubCoord_->GetSlaveInfo(&context, request, &reply);
    while (reply.slave_ip() == "") {
      ClientContext context2;
      stubCoord_->GetSlaveInfo(&context2, request, &reply);
    }
    
    login_info = reply.slave_ip() + ":" + reply.slave_port();
    service.stubSlave_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(
      grpc::CreateChannel(
        login_info, grpc::InsecureChannelCredentials())));
  }

  server->Wait();
}

void StreamHeartbeat(std::string hostname, std::string cport, std::string type, std::string port){
  std::string login_info = hostname + ":" + cport;
  std::unique_ptr<SNSCoordinator::Stub> stubCoord_ = std::unique_ptr<SNSCoordinator::Stub>(SNSCoordinator::NewStub(
    grpc::CreateChannel(
      login_info, grpc::InsecureChannelCredentials())));
  
  ClientContext context;
  std::shared_ptr<ClientReaderWriter<coordMessage, coordMessage>> stream(stubCoord_->Heartbeat(&context));
  while (true) {
    coordMessage m;
    m.set_server_ip("localhost");
    m.set_server_port(port);
    m.set_status("Active");
    m.set_server_type(type);
    stream->Write(m);
    sleep(10);
  }
}

void PeriodicCheck(std::string hostname, std::string cport, std::string server_id, std::string type) {
  std::string login_info = hostname + ":" + cport;
  std::unique_ptr<SNSCoordinator::Stub> stubCoord_ = std::unique_ptr<SNSCoordinator::Stub>(SNSCoordinator::NewStub(
    grpc::CreateChannel(
      login_info, grpc::InsecureChannelCredentials())));
  while (true) {
    sleep(30);
    syncIdInfo syncinfo;
    ClientContext context1;
    clusterInfo clusterinfo;
    syncinfo.set_sync_id(server_id);
    stubCoord_->GetClusterInfo(&context1, syncinfo, &clusterinfo);
    
    for (int i = 0; i < clusterinfo.cluster_clients().size(); i++) {
      std::string username = clusterinfo.cluster_clients(i);
      int user_index = find_user(username);
      if (user_index < 0) {
        std::cout << "Periodic Checking Error" << std::endl;
      } else {
        Client *user = &client_db[user_index];
        std::string pathname = "./" + type + username + "following.txt";
        struct stat sb;
        if (stat(pathname.c_str(), &sb) != -1) {
          if (sb.st_mtime != user->last_mod_time) {
            user->last_mod_time = sb.st_mtime;
            std::ifstream in(type + username + "following.txt");
            int count = 0;
            std::string line;
            while (getline(in, line)) {
              count++;
              if (count > user->following_file_size) {
                if (user->stream != 0) {
                  Message message;
                  message.set_msg(line);
                  user->stream->Write(message);
                }
              } 
            }
            user->following_file_size = count;
          } 
        } 
      } 
    } 
  }
}

int main(int argc, char** argv) {
  
  std::string hostname = "localhost";
  std::string port = "3010";
  std::string server_id = "0";
  std::string cport = "5050";

  int opt = 0;
  // getopt only work with single char => h=cip, c=cp, p=p,i=server_id, type=t
  while ((opt = getopt(argc, argv, "h:c:p:i:t:")) != -1){
      switch(opt) {
          case 'h':
              hostname = optarg;break;
          case 'c':
              cport = optarg;break;
          case 'p':
              port = optarg;break;
          case 'i':
              server_id = optarg;break;
          case 't':
              t = optarg;break;
          default:
              std::cerr << "Invalid Command Line Argument\n";
      }
  }
  
  identifier = server_id;
  
  //thread 1
  std::thread thread1(StreamHeartbeat, hostname, cport, t, port);
  
  //thread 2
  std::thread thread2(RunServer, hostname, cport, port, server_id);
    
  //thread 3
  std::thread thread3(PeriodicCheck, hostname, cport, server_id, t);
  
  
  thread1.join();
  thread2.join();
  thread3.join();

  return 0;
}
