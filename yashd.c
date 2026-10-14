
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdbool.h>

#define PORT 3820

int main(void) {

	int server_fd = socket(AF_INET, SOCK_STREAM, 0); //server_fd contains socket file descriptor & socket creates the IPv4 TCP socket
	
	if(server_fd == -1) {
		perror("socket");
		exit(EXIT_FAILURE);
	}
	//socket creation phase				
	struct sockaddr_in server_addr; //holds server's local address, IPv4 specific address
	server_addr = (struct sockaddr_in){0}; //IPv4 address structure
	server_addr.sin_family = AF_INET; //Which address family that is used
	server_addr.sin_port = htons(PORT);
	server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

	//binding: assigning local IP address and port
	if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
		perror("bind");
		close(server_fd);
		exit(EXIT_FAILURE);
	}
	//listening: creating the queue
	if (listen(server_fd, SOMAXCONN) == -1) {
		perror("listen");
		close(server_fd);
		exit(EXIT_FAILURE);
	}
	
	//client address
	struct sockaddr_in client_addr; //holds client's remote IP address
	socklen_t client_len;
	int client_fd;
	while (1) {
		client_len = sizeof(client_addr);
		client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);

	if (client_fd == -1) {
		perror("accept");
		close(server_fd);
		exit(EXIT_FAILURE);
	}

	char buffer[1024];
	ssize_t bytes_received;
	char command[1024];
	size_t command_len = 0;
	bool discarding = false;

	while (1) {
	
		bytes_received = recv(client_fd, buffer, sizeof(buffer)-1, 0);
	
		ssize_t bytes_sent;

		if (bytes_received > 0) {
			for (size_t i=0; i< (size_t)bytes_received; i++) {
				char ch = buffer[i];
				if (ch == '\n') {
					if (discarding) {
					 discarding = false;
					 command_len = 0;
					 continue;
					}

					command[command_len] = '\0';

					printf ("Complete command: %s\n", command);
					size_t total_sent = 0;

					command[command_len] = '\n';
					size_t response_len = command_len + 1;

					while (total_sent < response_len) {
						bytes_sent = send(client_fd, command + total_sent, response_len - total_sent, MSG_NOSIGNAL);

						if (bytes_sent == -1) {
							if (errno == EINTR) {
								continue;
							}
							perror("send");
							goto client_cleanup;
						}
						if (bytes_sent == 0) {
							goto client_cleanup;
						}

						total_sent += (size_t)bytes_sent;
					}
					command_len = 0;
				} else {
					if (discarding) {
						continue;
					}
					if (command_len >= sizeof(command) - 1) {
						fprintf(stderr, "Command too long \n");
						discarding = true;
						command_len = 0;
						continue;
					}

					command[command_len] = ch;
					command_len++;
					
				}
			}
			
		} else if (bytes_received == 0 ) {
			printf("Client disconneted \n");
			break;
		} else {
			if (errno == EINTR) {
				continue;
			}

			perror("recv");
			break;

		}
	}
	client_cleanup:
		close(client_fd);
	}
		close(server_fd);
	


}
