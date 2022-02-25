#include <arpa/inet.h>

#include <netdb.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/time.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <signal.h>
#include <pthread.h>
#include <string>
#include <sstream>
#include <algorithm>
#include "interface.h"

using namespace std;

const int MAX_CLIENTS = 128;  // Maximum number of connected clients
const int MAX_CHATROOMS = 16; // Maximum number of active chatrooms

// Basic chatroom struct with name, thread ID, and port
struct chatroom 
{
    string name = "";
    pthread_t tid = 0;
    int port = 0;
};

struct chatroom CHATROOMS[MAX_CHATROOMS]; // Array to store chatroom information
int CLIENTS[MAX_CLIENTS];                 // Array to store client sockets
pthread_t CLIENT_CHATROOMS[MAX_CLIENTS];  // Parallel array to CLIENTS that stores connected chatroom for each client
int CURRENT_PORT = 6000; // First port to try when creating a new chatroom
pthread_mutex_t LOCK = PTHREAD_MUTEX_INITIALIZER; // Lock to avoid race conditions in chatrooms

// Simple function that prints a message to stderr and exits with status 1
void error(const char* message)
{
    fprintf(stderr, "ERROR: ");
    fputs(message, stderr);
    exit(EXIT_FAILURE);
}

// Creates and returns a new listening socket bound to the lowest available port >= the supplied argument port.
int createListeningSocket(int& port)
{
    // Set up server address and protocol
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(port);

    // Create socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);

    // Attempt to bind socket, incrementing through available port numbers until bind succeeds or available ports are exhausted
    while (bind(sockfd, (struct sockaddr*) &server_addr, sizeof(server_addr)) < 0 && port < 49152)
    {
        printf("Could not bind socket on port %d, trying port %d\n", port, port+1);
        port++;
        server_addr.sin_port = htons(port);
    }

    // If available ports are exhausted, display an error and terminate
    if (port == 49152)
        error("bind failed");

    printf("Successfully bound to port %d\n", port);

    // Start listening on the socket
    if (listen(sockfd, 10) < 0)
        error("listen failed");

    return sockfd;
}

// Function to accept client connection request on a socket and store client information, making sure to avoid race conditions.
void acceptClientConnection(int server_socket, pthread_t tid)
{
    struct sockaddr_in client_addr;
    socklen_t addrlen = sizeof(client_addr);

    // Accept connection request and store socket information in newsockfd
    int newsockfd = -1;
    if ((newsockfd = accept(server_socket, (struct sockaddr*) &client_addr, &addrlen)) < 0) 
        error("could not accept new connection request");

    // search for a space in the CLIENTS array (avoiding race conditions) and fill with new client information
    int found_space = false;
    pthread_mutex_lock(&LOCK);
    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (CLIENTS[i] == 0)
        {
            CLIENTS[i] = newsockfd;
            CLIENT_CHATROOMS[i] = tid;
            found_space = true;
            break;
        }
    }
    pthread_mutex_unlock(&LOCK);
    
    // If the CLIENTS array is full (maximum number of connected clients reached), close the socket
    if (found_space == false)
    {
        printf("WARNING: could not accept connection request due to maximum number of connected clients");                
        close(newsockfd);
    }
}

// This function will call FD_ZERO on the current fd_set and FD_SET in order to reattach the master and slave sockets to the fd_set for subsequent select() calls
// It will also return the largest file descriptor for use with select() 
int resetReadFDs(int server_socket, fd_set &readfds, pthread_t tid)
{
    // clear the current fd_set
    FD_ZERO(&readfds);

    // attach the master socket
    FD_SET(server_socket, &readfds);

    // attach all active and connected clients, keeping track of the max file descriptor
    int max_fd = server_socket;
    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        // if the client is connected to the current chatroom (or the main server), attach the fd
        if (CLIENTS[i] > 0 && CLIENT_CHATROOMS[i] == tid)
        {
            FD_SET(CLIENTS[i], &readfds);
            if (CLIENTS[i] > max_fd )
                max_fd = CLIENTS[i];
        }
    }

    return max_fd;
}

// This function will count the number of currently connected clients to a particular chatroom
int countChatroomClients(pthread_t tid)
{
    int count = 0;
    for (int k = 0; k < MAX_CLIENTS; k++)
    {
        // If the client is connected to the chatroom, record the client
        if (CLIENT_CHATROOMS[k] == tid)
            count++;
    }

    // return the number of clients
    return count;
}

// When given the SIGINT interrupt signal, the chat room will send a message to all connected clients and shut down through this handler
void chatroomInterruptHandler(int sig_num)
{
    pthread_t tid = pthread_self();

    // Send shutdown message to all connected clients
    char buf[MAX_DATA] = "WARNING: chatroom shutting down.";
    for (int i = 0; i < MAX_CLIENTS; i++)
    {   
        // If the client is connected to the current chatroom
        if (CLIENT_CHATROOMS[i] == tid)
        {
            // Send the shutdown message, then close and reset the client connection
            send(CLIENTS[i], (char*) &buf, MAX_DATA, 0);
            close(CLIENTS[i]);
            CLIENTS[i] = 0;
            CLIENT_CHATROOMS[i] = 0;
        }
    }

    // Then remove the chatroom from the list of chatrooms by replacing it with a newly constructed default chatroom object
    for (int j = 0; j < MAX_CHATROOMS; j++)
    {
        if (CHATROOMS[j].tid == tid)
        {
            struct chatroom room;
            CHATROOMS[j] = room;
            break;
        }
    }

    // Then terminate the thread, shutting down the master socket and freeing resources
    pthread_cancel(tid);
}

// This is the main chatroom server thread and will create a new master socket and listen for connection requests. When a connected client sends a message to the chatroom
// server, the server will replay that message to all connected clients
void* runChatroomThread(void* args)
{
    // Set signal handler for clean termination on interrupt signal
    signal(SIGINT, chatroomInterruptHandler);

    // Avoid race conditions on update of current port when creating a new chatroom master socket
    pthread_mutex_lock(&LOCK);
    int sockfd = createListeningSocket(CURRENT_PORT);

    // Find the chatroom object representing the current chatroom and update the port number
    for (int i = 0; i < MAX_CHATROOMS; i++)
    {
        if (CHATROOMS[i].tid == pthread_self())
        {
            CHATROOMS[i].port = CURRENT_PORT;
            break;
        }
    }
    printf("Chatroom running on port %d...\n", CURRENT_PORT);
    pthread_mutex_unlock(&LOCK);
    
    char buf[MAX_DATA]; 
    memset(buf, 0, sizeof(buf));

    fd_set readfds;
    while(true)
    {
        // Call FD_ZERO and FD_SET on master and slave sockets
        int maxfd = resetReadFDs(sockfd, readfds, pthread_self());

        if (select( maxfd + 1, &readfds, NULL, NULL, NULL) < 0) 
            error("select failed");

        // Accept client connection requests on master socket
        if (FD_ISSET(sockfd, &readfds))
            acceptClientConnection(sockfd, pthread_self());

        // Handle requests on slave sockets
        for (int i = 0; i < MAX_CLIENTS; i++)
        {
            if (FD_ISSET(CLIENTS[i], &readfds))
            {
                int status = read(CLIENTS[i], buf, MAX_DATA);
                if (status <= 0)
                {
                    // If the message was empty or could not be read, disconnect the client
                    printf("Client disconnected\n");
                    close(CLIENTS[i]);
                    CLIENTS[i] = 0;
                    CLIENT_CHATROOMS[i] = 0;
                    break;
                }

                // If the string was nonempty, relay the message to all connected clients
                string command(buf);
                if (command != "")
                {
                    for(int j = 0; j < MAX_CLIENTS; j++)
                    {
                        if (CLIENTS[j] != 0 && CLIENTS[i] != CLIENTS[j] && CLIENT_CHATROOMS[j] == pthread_self())
                            send(CLIENTS[j], (char*) &buf, MAX_DATA, 0);
                    }
                }
            }
        }
    }
}

// Main server function that handles CREATE, JOIN, LIST, and DELETE functions and manages chatrooms
int main(int argc, char *argv[])
{
    if (argc != 2) 
    {
		fprintf(stderr, "usage: enter port number\n");
		exit(1);
	}

    // Create a listening socket on the nearest available port to the one requested
    int port = atoi(argv[1]);
    int sockfd = createListeningSocket(port);

    char buf[MAX_DATA]; 
    memset(buf, 0, sizeof(buf));

    // Reset all client and chatroom information
    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        CLIENTS[i] = 0;
        CLIENT_CHATROOMS[i] = 0;
    }

    fd_set readfds;
    printf("Server running on port %d...\n", port);
    while(true)
    {
        // Call FD_ZERO and FD_SET on master and slave sockets
        int maxfd = resetReadFDs(sockfd, readfds, 0);

        if (select(maxfd + 1, &readfds, NULL, NULL, NULL) < 0) 
            error("select failed");

        // Accept new connection request on master socket
        if (FD_ISSET(sockfd, &readfds))
            acceptClientConnection(sockfd, 0);

        // Handle requests on slave sockets
        for (int i = 0; i < MAX_CLIENTS; i++)
        {
            if (FD_ISSET(CLIENTS[i], &readfds))
            {
                int status = read(CLIENTS[i], buf, MAX_DATA);
                if (status <= 0)
                {
                    // If the message was empty or could not be read, disconnect the client
                    printf("Client disconnected\n");
                    close(CLIENTS[i]);
                    CLIENTS[i] = 0;
                    break;
                }

                stringstream ss(buf);
                string command;
                ss >> command;
                transform(command.begin(), command.end(), command.begin(), ::toupper); 

                Reply reply;

                // If the command was 'CREATE <chatroom name>'
                if (command == "CREATE")
                {
                    string chatroom_name;
                    ss >> chatroom_name;

                    // First, check if the chatroom name exists in the CHATROOMS array and return the error reply if it does
                    for (int j = 0; j < MAX_CHATROOMS; j++)
                    {
                        if (CHATROOMS[j].name == chatroom_name)
                        {
                            reply.status = FAILURE_ALREADY_EXISTS;
                            break;
                        }   
                    }

                    // Initialize a chatroom object in the CHATROOMS array and start the runChatroomThread() function as a new thread, passing CURRENT_PORT
                    if (reply.status != FAILURE_ALREADY_EXISTS)
                    {
                        for (int j = 0; j < MAX_CHATROOMS; j++)
                        {
                            if (CHATROOMS[j].name == "")
                            {
                                pthread_t new_thread;
                                pthread_create(&new_thread, NULL, runChatroomThread, NULL);
                                CHATROOMS[j].name = chatroom_name;
                                CHATROOMS[j].tid = new_thread;
                                reply.status = SUCCESS;
                                break;
                            }   
                        }
                    }
                }

                // If the command was 'DELETE <chatroom name>'
                if (command == "DELETE")
                {
                    string chatroom_name;
                    ss >> chatroom_name;

                    reply.status = FAILURE_NOT_EXISTS;

                    for (int j = 0; j < MAX_CHATROOMS; j++)
                    {
                        // If chatroom is found with correct name, send sigint and let handler shut down chatroom
                        if (CHATROOMS[j].name == chatroom_name)
                        {
                            printf("Sending interrupt signal to chatroom\n");
                            pthread_kill(CHATROOMS[j].tid, SIGINT);
                            reply.status = SUCCESS;
                            break;
                        }   
                    }
                }                                       

                // If the command was 'JOIN <chatroom name>'
                if (command == "JOIN")
                {
                    string chatroom_name;
                    ss >> chatroom_name;

                    reply.status = FAILURE_NOT_EXISTS;

                    for (int j = 0; j < MAX_CHATROOMS; j++)
                    {
                        // If chatroom with correct name is found, store port and number of clients in reply
                        if (CHATROOMS[j].name == chatroom_name)
                        {
                            reply.port = CHATROOMS[j].port;
                            reply.num_member = countChatroomClients(CHATROOMS[j].tid);
                            reply.status = SUCCESS;
                            break;
                        }
                    }                    
                }

                // If the command was 'LIST'
                if (command == "LIST")
                {
                    // Populate reply_string with comma seperated list of all active chatrooms
                    string reply_string = "";
                    bool comma = false;
                    for (int j = 0; j < MAX_CHATROOMS; j++)
                    {
                        if (CHATROOMS[j].name != "")
                        {
                            if (comma) 
                                reply_string += ", ";
                            reply_string += CHATROOMS[j].name;
                            comma = true;
                        }
                    }
                    if (reply_string == "")
                        reply_string = "empty";

                    reply.status = SUCCESS;
                    strcpy(reply.list_room, reply_string.c_str());
                }

                // Send reply and close connection
                send(CLIENTS[i], (char*) &reply, sizeof(reply), 0);
                close(CLIENTS[i]);
                CLIENTS[i] = 0;
            }
        }
    }
}