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
#include <poll.h>

#define PORT 3820
#define MAX_JOBS 20

typedef struct {
	pid_t pid;
	int job_id;
	bool stopped;
	bool running;
	unsigned long recency; // timestamp like var to keep of job 
	char command[256];
} job_t;

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

static int wait_for_child_poll(pid_t pid, pid_t pgid, int client_fd, int stdin_fd) {
	int status;
	int stopped = 0;
	int wait_error = 0;
	char control_buffer[1024]; // stores incoming bytes when child executes
	size_t control_len = 0;

	struct pollfd pfd = {
		.fd = client_fd,
		.events = POLLIN
	};

	while (1) {
		pid_t result = waitpid(pid, &status, WNOHANG | WUNTRACED);

		if (result == pid) {
			if (WIFEXITED(status) || WIFSIGNALED(status)) {
				break;
			}
			if (WIFSTOPPED(status)) {
				stopped = 1; //child stopped
				break;
			}
		} else if (result == -1) {
			if (errno == EINTR) {
				continue;
			}
			perror("waitpid");
			wait_error = 1;
			break;
		}

		int ready = poll(&pfd,1,100);

		if (ready < 0 && errno != EINTR) {
			perror("poll");
			wait_error = 1;
			break;
		}
		if (ready > 0 && (pfd.revents & POLLIN)) {
			//Prevent zero length receive from being mistaken as client disconnect
			if (control_len == sizeof(control_buffer)) {
				fprintf(stderr, "Control buffer full \n");
				wait_error = 1;
				break;
			}
			//Incoming data
			ssize_t n = recv(client_fd, control_buffer + control_len, sizeof(control_buffer) - control_len, 0);
			if (n > 0) {
				control_len += (size_t)n;
				size_t start = 0;
				for (size_t i=0; i < control_len; i++) {
					
					// Process completes new line terminated message
					if (control_buffer[i] == '\n') {
						size_t len = i - start;

		if (len == 5 && memcmp(control_buffer + start, "CTL c", 5) == 0) {
							if (pgid > 0) {
								kill(-pgid, SIGINT);
							}
						}
						else if (len == 5 && memcmp(control_buffer + start, "CTL z", 5) == 0) {
							if (pgid > 0) {
								kill(-pgid, SIGTSTP);
							}
						}

						else if (len==5 && memcmp(control_buffer + start, "CTL d", 5) == 0) {
							if (stdin_fd >=0) { 
								close(stdin_fd);
								stdin_fd = -1;
							}
						}

						else {
							//Forward regular input to the child's stdin
							if (stdin_fd >= 0 && len > 0) {
								ssize_t written = write(stdin_fd, control_buffer + start, len);

								if (written == -1) {
									perror("stdin write");
								}
							}

							if (stdin_fd >= 0) {
								write(stdin_fd, "\n",1);
							}

						}


						start = i + 1;
				
					}	
				}
				if (start > 0) {
					memmove(control_buffer, control_buffer + start, control_len - start);
					control_len -= start;
				}

			}
			else if (n==0) {
				//client disconnected, child completion not confirmed
				wait_error = 1;

				if (pgid > 0) {
					kill(-pgid, SIGKILL);
				}
				
				//reap the child so it doesn't become a zombie!!
				while (waitpid(pid, NULL, 0) == -1) {
					if (errno == EINTR) {
						continue;
					}
					if (errno != ECHILD) {
						perror("foreground cleanup waitpid");
					}
					break;
				}

				break;
			}
			else if (errno != EINTR) {
				perror("recv");
				wait_error = 1;
				break;
			}
		}
	}

	if (stdin_fd >= 0) {
		close(stdin_fd);
	}
	return wait_error ? -1 : stopped;
}

void *handle_client(void *arg) {
	int client_fd = *(int *)arg;
	free(arg);
	char buffer[1024];
	ssize_t bytes_received;
	char command[1024];
	size_t command_len = 0;
	
	bool discarding = false;
	//Job tracking for client's shell session as each client gets its own job array
	job_t jobs[MAX_JOBS];
	int job_count = 0;
	int next_job_id = 1;
	unsigned long recency_counter = 0; //every time a job becomes relevant to the rank, increment counter and assign value to that job

	//forcing prompt to start at server on first connection
	if (send_prompt(client_fd) == -1) goto client_cleanup;
	
	while (1) {

		//This block only runs when the handler reaches the main receive loop. Thread blocked inside fg won't perform cleanup until it returns
		for (int j = job_count - 1; j >= 0; j--) {
			if (!jobs[j].running) {
				continue;
			}
		

			int status;
			pid_t result = waitpid(jobs[j].pid, &status, WNOHANG);

			if (result == jobs[j].pid) {
				if (WIFEXITED(status) || WIFSIGNALED(status)) {
					for (int k = j; k < job_count - 1; k++) {
						jobs[k] = jobs[k+1];
					}
					job_count--;
				}
			} else if (result == -1 && errno != EINTR) {
				perror("background waitpid");
			}
		}
		//Making serer check periodically on background children even if client isn't receiving commands
		struct pollfd client_poll = {
			.fd = client_fd,
			.events = POLLIN
		};

		int poll_result = poll(&client_poll, 1, 500);

		if (poll_result == -1) {
			if (errno == EINTR) {
				continue;
			}
			perror("client poll");
			goto client_cleanup;
		}
		if (poll_result == 0) {
			continue;
		}
	
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
						
						char original_command[256];
						snprintf(original_command, sizeof(original_command), "%s", actual_command);
						//Checks whether the command is jobs
						if (strcmp(actual_command, "jobs") == 0) {
							for (int j = job_count - 1; j >= 0; j--) {
								if (!jobs[j].running) {
									continue;
								}

								int status;
								pid_t result = waitpid(jobs[j].pid, &status, WNOHANG);

								if (result == jobs[j].pid) {
									if (WIFEXITED(status) || WIFSIGNALED(status)) {
										for (int k = j; k < job_count -1; k++) {
											jobs[k] = jobs[k+1];
										}
										job_count--;
									}
								} else if (result == -1) {
									perror("background waitpid");
								}
							}


							unsigned long newest = 0;
							unsigned long previous = 0;

							for (int j=0; j < job_count; j++) {
								unsigned long r = jobs[j].recency;

								if (r > newest) {
									previous = newest;
									newest = r;
								} else if (r > previous) {
									previous = r;
								}
							}

							for (int j =0; j < job_count; j++) {
								char line[512];

								snprintf(line, sizeof(line), 
									"[%d]%c %s %s\n",
									jobs[j].job_id, 
									(jobs[j].recency == newest) ? '+' : 
									(jobs[j].recency == previous && previous != 0) ? '-' : ' ',
									jobs[j].stopped ? "Stopped" : "Running",
									jobs[j].command);

								if (send_all(client_fd, line, strlen(line)) == -1) {
									goto client_cleanup;
								}
							}
							if (send_prompt(client_fd) == -1) {
								goto client_cleanup;
							}

							command_len = 0;
							continue;

						}
						//Command recognition / fg block
						if (strncmp(actual_command, "fg ", 3) == 0) {
							int requested_id = 0;

							if (sscanf(actual_command, "fg %%%d", &requested_id) != 1 || requested_id <=0) {
								if (send_all(client_fd, "Invalid job ID\n", strlen("Invalid job ID \n")) == -1) {
									goto client_cleanup;
								}
								if (send_prompt(client_fd) == -1) {
									goto client_cleanup;
								}
								command_len = 0;
								continue;
							}

							int job_index = -1;

							for (int j = 0; j < job_count; j++) {
								if (jobs[j].job_id == requested_id) {
									job_index = j;
									break;
								}
							}

							if (job_index == -1) {
								if (send_all(client_fd, "Job not found\n", strlen("Job not found\n")) == -1) {
									goto client_cleanup;
								}

								if (send_prompt(client_fd) == -1) {
									goto client_cleanup;
								}
								command_len = 0;
								continue;
							}

							pid_t target_pid = jobs[job_index].pid;

							if (kill(-target_pid, SIGCONT) == -1) {
								perror("fg SIGCONT");

								if (send_all(client_fd, "Cannot resume job\n", strlen("Cannot resume job\n")) == -1) {
									goto client_cleanup;
								}
							} else {
								int fg_result = wait_for_child_poll(target_pid, target_pid, client_fd, -1);

								if (fg_result == -1) {
									goto client_cleanup;
								}

								if (fg_result == 1) {
									//Child stopped again
									jobs[job_index].stopped = true;
									jobs[job_index].running = false;
									jobs[job_index].recency = ++recency_counter;
								} else if (fg_result == 0) {
									//child exited or terminated, remove from job table
									for (int j = job_index; j < job_count - 1; j++) {
										jobs[j] =jobs[j+1];
									}
									job_count--;
								}
							}
							if (send_prompt(client_fd) == -1) {
								goto client_cleanup;
							}

							command_len = 0;
							continue;
						}

						//Bg block
						if (strncmp(actual_command, "bg ", 3) == 0) {
							int requested_id = 0;
							
							//invald id check
							if (sscanf(actual_command, "bg %%%d", &requested_id) != 1 || requested_id <= 0) {
								if (send_all(client_fd, "Invalid job ID \n", strlen("Invalid job ID\n")) == -1) {
									goto client_cleanup;
								}
								if (send_prompt(client_fd) == -1) {
									goto client_cleanup;
								}

								command_len = 0;
								continue;
							}

							//search for jobs associated with bg
							int job_index = -1;

							for (int j=0; j < job_count; j++) {
								if (jobs[j].job_id == requested_id) {
									job_index = j;
									break;
								}
							}

							if (job_index == -1) {
								if (send_all(client_fd, "Job not found \n", strlen("Job not found\n")) == -1) {
									goto client_cleanup;
								}
								if (send_prompt(client_fd) == -1) {
									goto client_cleanup;
								}
								
								command_len = 0;
								continue;
							}

							pid_t target_pid = jobs[job_index].pid;

							if (kill(-target_pid, SIGCONT) == -1) {
								perror("bg SIGCONT");

								if (send_all(client_fd, "Cannot resume job \n", strlen("Cannot resume job \n")) == -1) {
									goto client_cleanup;
								}
							} else {
								jobs[job_index].stopped = false;
								jobs[job_index].running = true;
								jobs[job_index].recency = ++recency_counter;
							}
							if (send_prompt(client_fd) == -1) {
								goto client_cleanup;
							}

							command_len = 0;
							continue;
						}


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

						//Background detection block
						bool background = false;

						if (argc > 0 && strcmp(argv[argc - 1], "&") == 0) {
							background = true;
							argc--;
							argv[argc] = NULL;
						}
						//Edge case against empty commands so it doesn't crash the server
						if (argc == 0) {
							if (send_all(client_fd, "Empty command \n", strlen("Empty command \n")) == -1) {
								goto client_cleanup;
							}
							if (send_prompt(client_fd) == -1) {
								goto client_cleanup;
							}

							command_len = 0;
							continue;
						}

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
									struct sigaction child_sa = {0};
									child_sa.sa_handler = SIG_DFL;
									sigemptyset(&child_sa.sa_mask);

									if (sigaction(SIGPIPE, &child_sa, NULL) == -1) {
										perror("sigaction left SIGPIPE");
										_exit(1);
									}


									if (setpgid(0,0) == -1) {
										perror("setpgid left");
										_exit(1);
									}
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
									//Parent establishes the left child process group
									if (setpgid(left_pid, left_pid) == -1 && errno != EACCES) {
										perror("setpgid left parent");
									}

									pid_t right_pid = fork();

									if (right_pid == -1) {
										perror("fork");
										close(pipefd[0]);
										close(pipefd[1]);
										kill(left_pid, SIGTERM);
										waitpid(left_pid, NULL, 0);
									}
									else if (right_pid == 0) {
										struct sigaction child_sa = {0};
										child_sa.sa_handler = SIG_DFL;
										sigemptyset(&child_sa.sa_mask);

										if (sigaction(SIGPIPE, &child_sa, NULL) == -1) {
											perror("sigaction right SIGPIPE");
											_exit(1);
										}
										if (setpgid(0, left_pid) == -1) {
											perror("setpgid right");
											_exit(1);
										}
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
										if (right_pid > 0) {
											if (setpgid(right_pid, left_pid) == -1 && errno != EACCES) {
												perror("setpgid right parent");
											}
										}
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
							
							//Create pipe for interactive stdin
							int stdin_pipe[2];

							if (pipe(stdin_pipe) == -1) {
								perror("stdin pipe");
								goto client_cleanup;
							}

							pid_t pid = fork();

							if (pid == -1) {
								perror("fork");
							} 
							else if (pid == 0) {
								//restore normal sigpipe behavior for execute
								struct sigaction child_sa = {0};
								child_sa.sa_handler = SIG_DFL;
								sigemptyset(&child_sa.sa_mask);

								if (sigaction(SIGPIPE, &child_sa, NULL) == -1) {
									perror("sigaction child SIGPIPE");
									_exit(1);
								}
								//Establish progress group if there is one!!
								if (setpgid(0,0) == -1) {
									perror("pgid");
									_exit(1);
								}

								//child cannot write to its own stdin pipe
								close(stdin_pipe[1]);

								//Use pipe unless input comes from a file
								if (left_input_file == NULL) {
									if (dup2(stdin_pipe[0], STDIN_FILENO) == -1) {
										perror("dup2 stdin");
										_exit(1);
									}
								}

								//original read descriptor should not be needed
								close(stdin_pipe[0]);

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
								close(stdin_pipe[0]); //Parent does not read from child's stdin pipe
								//Calls pgid
								if (setpgid(pid, pid) == -1) {
									if (errno != EACCES) {
										perror("setpgid parent");
									}
								}
								
								//Close unused pipe when input comes from a file
								if (left_input_file != NULL) {
									close(stdin_pipe[1]);
								}

								//background for parent
								if (background) {
									//Background jobs won't receive interactive stdin
									if (left_input_file == NULL) {
										close(stdin_pipe[1]);
									}
									job_t *job = &jobs[job_count];
									job->pid = pid;
									job->job_id = next_job_id++;
									job->stopped = false;
									job->running = true;
									job->recency = ++recency_counter;

									snprintf(job->command, sizeof(job->command), "%s", original_command);
									job_count++;
								}

								else { //Poll client messages while child executes
									int child_result = wait_for_child_poll(pid, pid, client_fd, left_input_file == NULL ? stdin_pipe[1] : -1);

									if (child_result == -1) {
										goto client_cleanup;
									}

									if (child_result == 1 && job_count < MAX_JOBS) { //if child is stopped
										job_t *job = &jobs[job_count];

										job->pid = pid;
										job->job_id = next_job_id++;
										job->stopped = true;
										job->running = false;
										job->recency = ++recency_counter;

										snprintf(job->command, sizeof(job->command), "%s", original_command);

										job_count++;
									}
								}
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
		for (int j = 0; j < job_count; j++) {
			pid_t pid = jobs[j].pid;

			//resume stopped children so they can be reaped
			if (jobs[j].stopped) {
				kill(-pid, SIGCONT);
			}

			//terminate entire process group
			kill(-pid, SIGKILL);

			//Reap the direct child to avoid zombie
			while (waitpid(pid, NULL, 0) == -1) {
				if (errno == EINTR) {
					continue;
				}
				if (errno != ECHILD) {
					perror("cleanup waitpid");
				}
				break;
			}
		}
		close(client_fd);
		return NULL;
}

int main(void) {

	//Prevent broken pipe from terminating everything
	struct sigaction sa = {0};
	sa.sa_handler = SIG_IGN;
	sigemptyset(&sa.sa_mask);

	if (sigaction(SIGPIPE, &sa, NULL) == -1) {
		perror("sigaction SIGPIPE");
		return EXIT_FAILURE;
	}

	int server_fd = socket(AF_INET, SOCK_STREAM, 0); //server_fd contains socket file descriptor & socket creates the IPv4 TCP socket
	
	if(server_fd == -1) {
		perror("socket");
		exit(EXIT_FAILURE);
	}

	int flags = fcntl(server_fd, F_GETFD);
	//one of the ports was still active during server testing
	if (flags == -1 || fcntl(server_fd, F_SETFD, flags | FD_CLOEXEC) == -1) {
		perror("fcntl FD_CLOEXEC");
		close(server_fd);
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
