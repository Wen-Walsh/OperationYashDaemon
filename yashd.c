#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdbool.h>
#include <string.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <errno.h>

#define PORT 3820

//send every byte of the message, even send() writes only part
static int send_all(int fd, const char *data, size_t length) {
	size_t sent = 0;
	while (sent < length) {
		ssize_t n = send(fd, data + sent, length - sent, MSG_NOSIGNAL);
		if (n > 0) {
			sent += (size_t)n;
		}
		else if (n == -1 && errno == EINTR) {
			continue;
		}
		else {
			return -1;
		}
	}
	return 0;
}

static int send_prompt(int fd) {
	return send_all(fd, "\n#", 2);
}

static void wait_for_child(pid_t pid) {
	while (waitpid(pid, NULL, 0) == -1 && errno == EINTR) {
	}
}
void *handle_client(void *arg) {
	int client_fd = *(int *)arg;
	free(arg);
	char buffer[1024];
	ssize_t bytes_received;
	char command[1024];
	size_t command_len = 0;
	bool discarding = false;
	
	//forcing prompt to start at server on first connection
	if (send_prompt(client_fd) == -1) goto client_cleanup;
	
	while (1) {
	
		bytes_received = recv(client_fd, buffer, sizeof(buffer)-1, 0);
	
		if (bytes_received > 0) {
			for (size_t i=0; i< (size_t)bytes_received; i++) {
				char ch = buffer[i];
				if (ch == '\n') {
					if (discarding) {
					 discarding = false;
					 command_len = 0;
					 if (send_prompt(client_fd) == -1) goto client_cleanup;
					 continue;
					}

					command[command_len] = '\0';

					//save the received line before strtok_r edits buffer
					printf("Complete command: %s\n", command);

					//Adding logic to check for CMD based on the phase 2 specification
					if (strncmp(command, "CMD ", 4) == 0) {
						char *actual_command = command + 4;

						char *argv[100];
						int argc = 0;
						char *saveptr = NULL;
						char *token = strtok_r(actual_command, " ", &saveptr); //multiple threads now

						while (token != NULL && argc < 99) {
							argv[argc] = token;
							argc++;
							token = strtok_r(NULL, " ", &saveptr);
						}

						argv[argc] = NULL;

						//Redirection parser similar to phase 1
						char *left_output_file = NULL;
						bool invalid_redirection = false;
						char *left_input_file = NULL;

						char *right_output_file = NULL;
						char *right_input_file = NULL;

						int left_count = 0;
						int right_count = 0;

						char *left_args[100];
						char *right_args[100];

						int pipe_index = -1;
						char **right_argv = right_args;
						bool invalid_pipe = false;

						//First check: where is da pipe?? (do not allow for multiple pipes)
						for (int i=0; i < argc; i++) {
							if (strcmp(argv[i], "|") == 0) {
								if (pipe_index != -1) invalid_pipe=true;
								pipe_index = i;
							}
						}
						//Second check, collect command args and associate filename with the command on its side of the pipe
						for (int i= 0; i<argc && !invalid_pipe; i++) {
							if (i == pipe_index) continue;
							bool right_side = (pipe_index != -1 && i > pipe_index);
							if (strcmp(argv[i], ">") == 0 || strcmp(argv[i], "<") == 0) {
								bool output = (argv[i][0] == '>');
								if (i + 1 >= argc || i + 1 == pipe_index || strcmp(argv[i + 1], "<") == 0 || strcmp(argv[i+1], ">") == 0 || strcmp(argv[i+1], "|") == 0) {
									invalid_redirection = true;
									break;
								}
								char **slot = output ? (right_side ? &right_output_file : &left_output_file) : (right_side ? &right_input_file : &left_input_file);
								if (*slot != NULL) {
									invalid_redirection = true;
									break;
								}
								*slot = argv[++i];
							}
							else if (right_side) {
								right_args[right_count++] = argv[i];
							} 
							else {
								left_args[left_count++] = argv[i];
							}
						}
						left_args[left_count] = NULL;
						right_args[right_count] = NULL;

						//Pipe validation 
						if (pipe_index != -1 && (left_count == 0 || right_count == 0)) {
							invalid_pipe = true;
						}
						if (invalid_pipe && send_all(client_fd, "Invalid pipe syntax \n", strlen("Invalid pipe syntax\n")) == -1) goto client_cleanup;
						if (invalid_redirection && send_all(client_fd, "Invalid redirection syntax\n", strlen("Invalid redirection syntax\n")) == - 1) goto client_cleanup;

						//Only nonempty commands should enter this logic
						//Commands with pipes
						if (left_count > 0 && !invalid_redirection && !invalid_pipe && pipe_index != -1) {
							int pipefd[2];

							if (pipe(pipefd) == -1) {
								perror("pipe");
							}
							else {
								pid_t left_pid = fork();

								if (left_pid == -1) {
									perror("fork");
									close(pipefd[0]);
									close(pipefd[1]);
								}
								else if (left_pid == 0) {
									if (dup2(pipefd[1],STDOUT_FILENO) == -1) {
										perror("dup2");
										_exit(1);
									}
									if (dup2(client_fd, STDERR_FILENO) == -1) {
										_exit(1);
									}
									close(pipefd[0]);
									close(pipefd[1]);

									//Adding file redirections to override the pipe streams now
									if (left_input_file != NULL) {
										int fd = open(left_input_file, O_RDONLY);
										if (fd < 0) {
											perror("open");
											_exit(1);
										}
										if (dup2(fd, STDIN_FILENO) < 0) {
											perror("dup2");
											_exit(1);
										}
										close(fd);
									}
									
									if (left_output_file != NULL) {
										int fd = open(left_output_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
										if (fd < 0) {
											perror("open");
											_exit(1);
										}
										if (dup2(fd, STDOUT_FILENO) < 0) {
											perror("dup2");
											_exit(1);
										}
										close(fd);
									}

									execvp(left_args[0], left_args);
									perror("execvp");
									_exit(127);
								}
								else {
									pid_t right_pid = fork();

									if (right_pid == -1) {
										perror("fork");
										close(pipefd[0]);
										close(pipefd[1]);
										kill(left_pid, SIGTERM);
										waitpid(left_pid, NULL, 0);
									}
									else if (right_pid == 0) {
										//Read from pipe
										if(dup2(pipefd[0], STDIN_FILENO) == -1) {
											_exit(1);
										}
										//Send final output to the TCP client
										if (dup2(client_fd, STDOUT_FILENO) == -1) {
											_exit(1);
										}
										if (dup2(client_fd, STDERR_FILENO) == -1) {
											_exit(1);
										}

										close(pipefd[0]);
										close(pipefd[1]);

										//Add logic for the right input files now
										if (right_input_file != NULL) {
											int fd = open(right_input_file, O_RDONLY);
											if (fd < 0) {
												perror("open");
												_exit(1);
											}
											if (dup2(fd, STDIN_FILENO) < 0) {
												perror("dup2");
												_exit(1);
											}
											close(fd);
										}
										
										execvp(right_argv[0], right_argv);
										perror("execvp");
										_exit(127);
									}
									else {
										//Parent should close both ends 
										close(pipefd[0]);
										close(pipefd[1]);

										//Wait for both commands to finish
										wait_for_child(left_pid);
										wait_for_child(right_pid);
									
									}
								}
							}
						}

						//Commands without pipes
						if (left_count > 0 && !invalid_redirection && !invalid_pipe && pipe_index == -1) {
							
							pid_t pid = fork();

							if (pid == -1) {
								perror("fork");
							} 
							else if (pid == 0) {
								if (dup2(client_fd, STDOUT_FILENO) == -1) {
									_exit(1);
								}
								if (dup2(client_fd, STDERR_FILENO) == -1) {
									_exit(1);
								}
									
								//Output redirection from phase 1
								if (left_output_file != NULL) {
									int fd_out = open(left_output_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
									if (fd_out < 0) {
										perror("open");
										_exit(1);
									}
									if (dup2(fd_out, STDOUT_FILENO) == -1) {
										perror("dup2");
										_exit(1);
									}
									close(fd_out);
								}

								if (left_input_file != NULL) {
									int fd_in = open(left_input_file, O_RDONLY);
									if (fd_in < 0) {
										perror("open");
										_exit(1);
									}
									if (dup2(fd_in, STDIN_FILENO) == -1) {
										perror("dup2");
										_exit(1);
									}
									close(fd_in);
								}

								if (execvp(left_args[0], left_args) == -1) {
									perror("execvp");
									_exit(127);
								}
							}
							else {	
								//Parent waits for chid
								wait_for_child(pid);
							}

						}
					}
					else if (command[0] != '\0') {
						//Need to implement CTL and interactive stdin are part of the later control phase
						if (send_all(client_fd, "Unsupported message \n", strlen("Unsupported message \n")) == -1) {
							goto client_cleanup;
						}
					}
					
					if (send_prompt(client_fd) == -1) {
						goto client_cleanup;
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
		return NULL;
}

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

		pthread_t thread_id;
		
		int *client_ptr = malloc(sizeof(int));

		if (client_ptr == NULL) {
			perror("malloc");
			close(client_fd);
			continue;
		}
		
		*client_ptr = client_fd;

		int thread_result = pthread_create(&thread_id, NULL, handle_client, client_ptr);

		if (thread_result != 0) {
			fprintf(stderr, "pthread_create: %s\n", strerror(thread_result));
			free(client_ptr);
			close(client_fd);
			continue;
		}

		pthread_detach(thread_id);

	}
		close(server_fd);
	


}
