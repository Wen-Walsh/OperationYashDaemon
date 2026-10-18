#define _GNU_SOURCE // pipe2

#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define PORT 3820
#define MAX_JOBS 20

// umask yashd was started with; commands get it back before exec
static mode_t start_umask = 022;

typedef struct {
  pid_t pid;
  pid_t right_pid;
  bool left_finished;
  bool right_finished;
  bool left_stopped;
  bool right_stopped;
  int job_id;
  bool stopped;
  bool running;
  unsigned long recency; // timestamp like var to keep of job
  int stdin_fd;          // write end of the child's stdin pipe, -1 = none
  char command[256];
} job_t;

// connection struct
typedef struct {
  // client socket
  int fd;
  // client's IP:port
  struct sockaddr_in addr;
} conn_t;

// send every byte of the message, even send() writes only part
static int send_all(int fd, const char *data, size_t length) {
  size_t sent = 0;
  while (sent < length) {
    ssize_t n = send(fd, data + sent, length - sent, MSG_NOSIGNAL);
    if (n > 0) {
      sent += (size_t)n;
    } else if (n == -1 && errno == EINTR) {
      continue;
    } else {
      return -1;
    }
  }
  return 0;
}

static int send_prompt(int fd) { return send_all(fd, "\n#", 2); }

static int send_msg(int fd, const char *msg) {
  return send_all(fd, msg, strlen(msg));
}

static bool job_done(const job_t *job);
static void reap_job(job_t *job);

static int wait_for_child_poll(job_t *job, int client_fd, int *stdin_fd) {
  int status;
  int stopped = 0;
  int wait_error = 0;
  char control_buffer[1024]; // stores incoming bytes when child executes
  size_t control_len = 0;

  struct pollfd pfd = {.fd = client_fd, .events = POLLIN};

  while (1) {
    pid_t result =
        waitpid(-job->pid, &status, WNOHANG | WUNTRACED | WCONTINUED);

    if (result > 0) {
      bool *finished =
          result == job->pid ? &job->left_finished : &job->right_finished;
      bool *stopped_flag =
          result == job->pid ? &job->left_stopped : &job->right_stopped;

      if (WIFEXITED(status) || WIFSIGNALED(status)) {
        *finished = true;
        *stopped_flag = false;
      } else if (WIFSTOPPED(status)) {
        *stopped_flag = true;
      } else if (WIFCONTINUED(status)) {
        *stopped_flag = false;
      }
    } else if (result == -1) {
      if (errno == EINTR) {
        continue;
      }

      if (errno == ECHILD) {
        job->left_finished = true;
        job->right_finished = true;
      } else {
        perror("waitpid");
        wait_error = 1;
        break;
      }
    }

    // Both children completed
    if (job_done(job)) {
      break;
    }

    // all remaining children are stopped
    if ((job->left_finished || job->left_stopped) &&
        (job->right_finished || job->right_stopped)) {
      stopped = 1;
      break;
    }

    int ready = poll(&pfd, 1, 100);

    if (ready < 0 && errno != EINTR) {
      perror("poll");
      wait_error = 1;
      break;
    }
    if (ready > 0 && (pfd.revents & POLLIN)) {
      // Prevent zero length receive from being mistaken as client disconnect
      if (control_len == sizeof(control_buffer)) {
        fprintf(stderr, "Control buffer full \n");
        wait_error = 1;
        break;
      }
      // Incoming data
      ssize_t n = recv(client_fd, control_buffer + control_len,
                       sizeof(control_buffer) - control_len, 0);
      if (n > 0) {
        control_len += (size_t)n;
        size_t start = 0;
        for (size_t i = 0; i < control_len; i++) {

          // Process completes new line terminated message
          if (control_buffer[i] == '\n') {
            size_t len = i - start;

            if (len == 5 && memcmp(control_buffer + start, "CTL c", 5) == 0) {
              if (job->pid > 0) {
                kill(-job->pid, SIGINT);
              }
            } else if (len == 5 &&
                       memcmp(control_buffer + start, "CTL z", 5) == 0) {
              if (job->pid > 0) {
                kill(-job->pid, SIGTSTP);
              }
            }

            else if (len == 5 &&
                     memcmp(control_buffer + start, "CTL d", 5) == 0) {
              if (*stdin_fd >= 0) {
                close(*stdin_fd);
                *stdin_fd = -1;
              }
            }

            else {
              // Forward regular input to the child's stdin
              if (*stdin_fd >= 0 && len > 0) {
                ssize_t written = write(*stdin_fd, control_buffer + start, len);

                if (written == -1) {
                  perror("stdin write");
                }
              }

              if (*stdin_fd >= 0) {
                write(*stdin_fd, "\n", 1);
              }
            }

            start = i + 1;
          }
        }
        if (start > 0) {
          memmove(control_buffer, control_buffer + start, control_len - start);
          control_len -= start;
        }

      } else if (n == 0) {
        // kill and reap the entire fg job
        wait_error = 1;

        if (job->pid > 0) {
          kill(-job->pid, SIGKILL);
        }

        reap_job(job);
        break;
      } else if (errno != EINTR) {
        perror("recv");
        wait_error = 1;
        break;
      }
    }
  }

  // stopped: keep the pipe open so fg can feed it again
  if (!stopped && *stdin_fd >= 0) {
    close(*stdin_fd);
    *stdin_fd = -1;
  }
  return wait_error ? -1 : stopped;
}

// Job management helpers which determine when a job is completed
static void refresh_job(job_t *job) {
  pid_t children[2] = {job->pid, job->right_pid};

  bool *finished[2] = {&job->left_finished, &job->right_finished};

  bool *stopped[2] = {&job->left_stopped, &job->right_stopped};

  for (int i = 0; i < 2; i++) {
    if (children[i] == 0 || *finished[i])
      continue;
    int status;
    pid_t result;

    do {
      result = waitpid(children[i], &status, WNOHANG | WUNTRACED | WCONTINUED);
    } while (result == -1 && errno == EINTR);

    if (result == children[i]) {
      if (WIFEXITED(status) || WIFSIGNALED(status)) {
        *finished[i] = true;
        *stopped[i] = false;
      }

      else if (WIFSTOPPED(status)) {
        *stopped[i] = true;
      }

    } else if (result == -1 && errno == ECHILD) {
      *finished[i] = true;
    }
  }
  job->running = !((job->left_finished || job->left_stopped) &&
                   (job->right_finished || job->right_stopped));
  job->stopped = !job->running && !(job->left_finished && job->right_finished);
}

static bool job_done(const job_t *job) {
  return job->left_finished && job->right_finished;
}

static void reap_job(job_t *job) {
  if (!job->left_finished) {
    while (waitpid(job->pid, NULL, 0) == -1 && errno == EINTR) {
    }
    job->left_finished = true;
  }

  if (job->right_pid > 0 && !job->right_finished) {
    while (waitpid(job->right_pid, NULL, 0) == -1 && errno == EINTR) {
    }
    job->right_finished = true;
  }
}

// close both ends of a pipe that may not have been opened (-1)
static void close_pipe(int p[2]) {
  if (p[0] >= 0)
    close(p[0]);
  if (p[1] >= 0)
    close(p[1]);
}

// new job number
static int next_job_num(const job_t *jobs, int count) {
  int highest = 0;
  for (int j = 0; j < count; j++)
    if (jobs[j].job_id > highest)
      highest = jobs[j].job_id;
  return highest + 1;
}

// find the newest job for bg and fg cmds
static int pick_job(const job_t *jobs, int count, bool stopped_only) {
  int best = -1;
  for (int j = 0; j < count; j++) {
    if (stopped_only && !jobs[j].stopped)
      continue;
    if (best == -1 || jobs[j].recency > jobs[best].recency)
      best = j;
  }
  return best;
}

// print [n]- Done cmd for each finished job
// then drop it
static int report_done(int fd, job_t *jobs, int *count) {
  unsigned long newest = 0;
  for (int j = 0; j < *count; j++)
    if (jobs[j].recency > newest)
      newest = jobs[j].recency;
  for (int j = 0; j < *count; j++) {
    refresh_job(&jobs[j]);
    if (!job_done(&jobs[j]))
      continue;
    char line[512];
    snprintf(line, sizeof(line), "[%d]%c Done %s\n", jobs[j].job_id,
             jobs[j].recency == newest ? '+' : '-', jobs[j].command);
    if (send_msg(fd, line) == -1)
      return -1;
    if (jobs[j].stdin_fd >= 0)
      close(jobs[j].stdin_fd);
    for (int k = j; k < *count - 1; k++)
      jobs[k] = jobs[k + 1];
    (*count)--;
    j--;
  }
  return 0;
}

// logging
static pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;

static void log_cmd(const char *who, const char *cmd) {
  time_t now = time(NULL);
  struct tm t;
  localtime_r(&now, &t);
  char when[32];
  strftime(when, sizeof(when), "%b %d %H:%M:%S", &t);

  pthread_mutex_lock(&log_lock);
  FILE *f = fopen("/tmp/yashd.log", "ae"); // e: O_CLOEXEC
  if (f != NULL) {
    fprintf(f, "%s yashd[%s]: %s\n", when, who, cmd);
    fclose(f);
  }
  pthread_mutex_unlock(&log_lock);
}

void *handle_client(void *arg) {
  // recast the connection struct
  conn_t conn = *(conn_t *)arg;
  free(arg);

  // grab the ip for logging
  char ip[INET_ADDRSTRLEN];
  inet_ntop(AF_INET, &conn.addr.sin_addr, ip, sizeof(ip));
  char who[32];
  snprintf(who, sizeof(who), "%s:%d", ip, ntohs(conn.addr.sin_port));

  int client_fd = conn.fd;
  char buffer[1024];
  ssize_t bytes_received;
  char command[1024];
  size_t command_len = 0;

  bool discarding = false;
  // Job tracking for client's shell session as each client gets its own job
  // array
  job_t jobs[MAX_JOBS];
  int job_count = 0;
  unsigned long recency_counter =
      0; // every time a job becomes relevant to the rank, increment counter and
         // assign value to that job

  // forcing prompt to start at server on first connection
  if (send_prompt(client_fd) == -1)
    goto client_cleanup;

  while (1) {

    // This block only runs when the handler reaches the main receive loop.
    // Thread blocked inside fg won't perform cleanup until it returns
    // report_done prints and removes on the next cmd
    for (int j = 0; j < job_count; j++) {
      refresh_job(&jobs[j]);
    }

    // Making serer check periodically on background children even if client
    // isn't receiving commands
    struct pollfd client_poll = {.fd = client_fd, .events = POLLIN};

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

    // one byte at a time: anything after the CMD line (pasted stdin) stays
    // in the socket for wait_for_child_poll to hand to the command
    bytes_received = recv(client_fd, buffer, 1, 0);

    if (bytes_received > 0) {
      for (size_t i = 0; i < (size_t)bytes_received; i++) {
        char ch = buffer[i];
        if (ch == '\n') {
          if (discarding) {
            discarding = false;
            command_len = 0;
            if (send_prompt(client_fd) == -1)
              goto client_cleanup;
            continue;
          }

          command[command_len] = '\0';

          // save the received line before strtok_r edits buffer
          printf("Complete command: %s\n", command);

          // Adding logic to check for CMD based on the phase 2 specification
          if (strncmp(command, "CMD ", 4) == 0) {
            char *actual_command = command + 4;

            // log every cmd before running (not a blank Enter)
            if (actual_command[strspn(actual_command, " ")] != '\0')
              log_cmd(who, actual_command);

            // done notice for finished job
            if (report_done(client_fd, jobs, &job_count) == -1) {
              goto client_cleanup;
            }

            char original_command[256];
            snprintf(original_command, sizeof(original_command), "%s",
                     actual_command);
            // jobs stores "sleep 30" instead of "sleep 30 &"
            size_t n = strlen(original_command);
            if (n >= 2 && strcmp(original_command + n - 2, " &") == 0)
              original_command[n - 2] = '\0';

            // Checks whether the command is jobs
            if (strcmp(actual_command, "jobs") == 0) {
              unsigned long newest = 0;

              for (int j = 0; j < job_count; j++) {
                if (jobs[j].recency > newest)
                  newest = jobs[j].recency;
              }

              for (int j = 0; j < job_count; j++) {
                char line[512];

                snprintf(line, sizeof(line), "[%d]%c %s %s%s\n", jobs[j].job_id,
                         (jobs[j].recency == newest) ? '+' : '-',
                         jobs[j].stopped ? "Stopped" : "Running",
                         jobs[j].command, jobs[j].stopped ? "" : " &");

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
            // Command recognition / fg block
            if (strcmp(actual_command, "fg") == 0 ||
                strncmp(actual_command, "fg ", 3) == 0) {
              bool bare = strcmp(actual_command, "fg") == 0;
              int requested_id = 0;

              if (!bare &&
                  (sscanf(actual_command, "fg %%%d", &requested_id) != 1 ||
                   requested_id <= 0)) {
                if (send_msg(client_fd, "Invalid job ID\n") == -1) {
                  goto client_cleanup;
                }
                if (send_prompt(client_fd) == -1) {
                  goto client_cleanup;
                }
                command_len = 0;
                continue;
              }

              int job_index = bare ? pick_job(jobs, job_count, false) : -1;

              for (int j = 0; j < job_count; j++) {
                if (jobs[j].job_id == requested_id) {
                  job_index = j;
                  break;
                }
              }

              if (job_index == -1) {
                if (send_msg(client_fd, "Job not found\n") == -1) {
                  goto client_cleanup;
                }

                if (send_prompt(client_fd) == -1) {
                  goto client_cleanup;
                }
                command_len = 0;
                continue;
              }

              // fg echoes the command
              char line[512];
              snprintf(line, sizeof(line), "%s\n", jobs[job_index].command);
              if (send_all(client_fd, line, strlen(line)) == -1) {
                goto client_cleanup;
              }

              pid_t target_pid = jobs[job_index].pid;

              if (kill(-target_pid, SIGCONT) == -1) {
                perror("fg SIGCONT");

                if (send_msg(client_fd, "Cannot resume job\n") == -1) {
                  goto client_cleanup;
                }
              } else {
                jobs[job_index].left_stopped = false;
                jobs[job_index].right_stopped = false;

                int fg_result = wait_for_child_poll(&jobs[job_index], client_fd,
                                                    &jobs[job_index].stdin_fd);

                if (fg_result == -1) {
                  goto client_cleanup;
                }
                if (fg_result == 1) {
                  jobs[job_index].stopped = true;
                  jobs[job_index].running = false;
                  jobs[job_index].recency = ++recency_counter;
                } else {
                  // entire job completed
                  for (int j = job_index; j < job_count - 1; j++) {
                    jobs[j] = jobs[j + 1];
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

            // Bg block
            if (strcmp(actual_command, "bg") == 0 ||
                strncmp(actual_command, "bg ", 3) == 0) {
              bool bare = strcmp(actual_command, "bg") == 0;
              int requested_id = 0;

              // invald id check
              if (!bare &&
                  (sscanf(actual_command, "bg %%%d", &requested_id) != 1 ||
                   requested_id <= 0)) {
                if (send_msg(client_fd, "Invalid job ID\n") == -1) {
                  goto client_cleanup;
                }
                if (send_prompt(client_fd) == -1) {
                  goto client_cleanup;
                }

                command_len = 0;
                continue;
              }

              // search for jobs associated with bg
              int job_index = bare ? pick_job(jobs, job_count, true) : -1;

              for (int j = 0; j < job_count; j++) {
                if (jobs[j].job_id == requested_id) {
                  job_index = j;
                  break;
                }
              }

              if (job_index == -1) {
                if (send_msg(client_fd, "Job not found\n") == -1) {
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

                if (send_msg(client_fd, "Cannot resume job\n") == -1) {
                  goto client_cleanup;
                }
              } else {
                // stopped status for both children
                jobs[job_index].left_stopped = false;
                jobs[job_index].right_stopped = false;
                jobs[job_index].stopped = false;
                jobs[job_index].running = true;
                jobs[job_index].recency = ++recency_counter;

                // bg jobs get no stdin, same as & jobs
                if (jobs[job_index].stdin_fd >= 0) {
                  close(jobs[job_index].stdin_fd);
                  jobs[job_index].stdin_fd = -1;
                }

                // bg prints jobs with &
                char line[512];
                snprintf(line, sizeof(line), "[%d]+ %s &\n",
                         jobs[job_index].job_id, jobs[job_index].command);
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

            char *argv[100];
            int argc = 0;
            char *saveptr = NULL;
            char *token =
                strtok_r(actual_command, " ", &saveptr); // multiple threads now

            while (token != NULL && argc < 99) {
              argv[argc] = token;
              argc++;
              token = strtok_r(NULL, " ", &saveptr);
            }

            argv[argc] = NULL;

            // Background detection block
            bool background = false;

            if (argc > 0 && strcmp(argv[argc - 1], "&") == 0) {
              background = true;
              argc--;
              argv[argc] = NULL;
            }
            // Edge case against empty commands so it doesn't crash the server
            if (argc == 0) {
              if (send_prompt(client_fd) == -1) {
                goto client_cleanup;
              }

              command_len = 0;
              continue;
            }
            // stop from writing beyond max jobs
            if (background && job_count >= MAX_JOBS) {
              if (send_msg(client_fd, "Job limit reached\n") == -1) {
                goto client_cleanup;
              }
              if (send_prompt(client_fd) == -1) {
                goto client_cleanup;
              }

              command_len = 0;
              continue;
            }

            // Redirection parser similar to phase 1
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

            // First check: where is da pipe?? (do not allow for multiple pipes)
            for (int i = 0; i < argc; i++) {
              if (strcmp(argv[i], "|") == 0) {
                if (pipe_index != -1)
                  invalid_pipe = true;
                pipe_index = i;
              }
            }
            // Second check, collect command args and associate filename with
            // the command on its side of the pipe
            for (int i = 0; i < argc && !invalid_pipe; i++) {
              if (i == pipe_index)
                continue;
              bool right_side = (pipe_index != -1 && i > pipe_index);
              if (strcmp(argv[i], ">") == 0 || strcmp(argv[i], "<") == 0) {
                bool output = (argv[i][0] == '>');
                if (i + 1 >= argc || i + 1 == pipe_index ||
                    strcmp(argv[i + 1], "<") == 0 ||
                    strcmp(argv[i + 1], ">") == 0 ||
                    strcmp(argv[i + 1], "|") == 0) {
                  invalid_redirection = true;
                  break;
                }
                char **slot =
                    output
                        ? (right_side ? &right_output_file : &left_output_file)
                        : (right_side ? &right_input_file : &left_input_file);
                if (*slot != NULL) {
                  invalid_redirection = true;
                  break;
                }
                *slot = argv[++i];
              } else if (right_side) {
                right_args[right_count++] = argv[i];
              } else {
                left_args[left_count++] = argv[i];
              }
            }
            left_args[left_count] = NULL;
            right_args[right_count] = NULL;

            // Pipe validation
            if (pipe_index != -1 && (left_count == 0 || right_count == 0)) {
              invalid_pipe = true;
            }
            if (invalid_pipe &&
                send_msg(client_fd, "Invalid pipe syntax\n") == -1)
              goto client_cleanup;
            if (invalid_redirection &&
                send_msg(client_fd, "Invalid redirection syntax\n") == -1)
              goto client_cleanup;

            // Only nonempty commands should enter this logic
            // Commands with pipes
            if (left_count > 0 && !invalid_redirection && !invalid_pipe &&
                pipe_index != -1) {
              // typed input goes to the left command, same as a single one
              int stdin_pipe[2] = {-1, -1};
              if (left_input_file == NULL &&
                  pipe2(stdin_pipe, O_CLOEXEC) == -1) {
                perror("stdin pipe");
                goto client_cleanup;
              }

              int pipefd[2];

              // O_CLOEXEC
              if (pipe2(pipefd, O_CLOEXEC) == -1) {
                perror("pipe");
                close_pipe(stdin_pipe);
              } else {
                pid_t left_pid = fork();

                if (left_pid == -1) {
                  perror("fork");
                  close(pipefd[0]);
                  close(pipefd[1]);
                  close_pipe(stdin_pipe);
                } else if (left_pid == 0) {
                  struct sigaction child_sa = {0};
                  child_sa.sa_handler = SIG_DFL;
                  sigemptyset(&child_sa.sa_mask);

                  if (sigaction(SIGPIPE, &child_sa, NULL) == -1) {
                    perror("sigaction left SIGPIPE");
                    _exit(1);
                  }

                  if (setpgid(0, 0) == -1) {
                    perror("setpgid left");
                    _exit(1);
                  }
                  if (dup2(pipefd[1], STDOUT_FILENO) == -1) {
                    perror("dup2");
                    _exit(1);
                  }
                  // typed input (a < file below replaces it)
                  if (stdin_pipe[0] >= 0 &&
                      dup2(stdin_pipe[0], STDIN_FILENO) == -1) {
                    _exit(1);
                  }
                  if (dup2(client_fd, STDERR_FILENO) == -1) {
                    _exit(1);
                  }
                  close(pipefd[0]);
                  close(pipefd[1]);

                  // Adding file redirections to override the pipe streams now
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
                    int fd = open(left_output_file,
                                  O_WRONLY | O_CREAT | O_TRUNC, 0664);
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

                  umask(start_umask);      // after the > open, so it stays 0664
                  signal(SIGHUP, SIG_DFL); // undo the daemon's SIG_IGN
                  execvp(left_args[0], left_args);
                  _exit(127);
                } else {
                  // Parent establishes the left child process group
                  if (setpgid(left_pid, left_pid) == -1 && errno != EACCES) {
                    perror("setpgid left parent");
                  }

                  pid_t right_pid = fork();

                  if (right_pid == -1) {
                    perror("fork");
                    close(pipefd[0]);
                    close(pipefd[1]);
                    close_pipe(stdin_pipe);

                    kill(-left_pid, SIGKILL);

                    while (waitpid(left_pid, NULL, 0) == -1 && errno == EINTR) {
                    }
                  } else if (right_pid == 0) {
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
                    // Read from pipe
                    if (dup2(pipefd[0], STDIN_FILENO) == -1) {
                      _exit(1);
                    }
                    // Send final output to the TCP client
                    if (dup2(client_fd, STDOUT_FILENO) == -1) {
                      _exit(1);
                    }
                    if (dup2(client_fd, STDERR_FILENO) == -1) {
                      _exit(1);
                    }

                    close(pipefd[0]);
                    close(pipefd[1]);

                    // Add logic for the right input files now
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
                    if (right_output_file != NULL) {
                      int fd = open(right_output_file,
                                    O_WRONLY | O_CREAT | O_TRUNC, 0664);

                      if (fd < 0) {
                        perror("open");
                        _exit(1);
                      }

                      if (dup2(fd, STDOUT_FILENO) == -1) {
                        perror("dup2");
                        _exit(1);
                      }

                      close(fd);
                    }
                    umask(start_umask); // after the > open, so it stays 0664
                    signal(SIGHUP, SIG_DFL); // undo the daemon's SIG_IGN
                    execvp(right_argv[0], right_argv);
                    _exit(127);
                  } else {
                    if (right_pid > 0) {
                      if (setpgid(right_pid, left_pid) == -1 &&
                          errno != EACCES) {
                        perror("setpgid right parent");
                      }
                    }
                    // Parent should close both ends
                    close(pipefd[0]);
                    close(pipefd[1]);
                    // parent keeps only the write end of the left's stdin
                    if (stdin_pipe[0] >= 0)
                      close(stdin_pipe[0]);

                    // Block waits on the updating wait_for_child_poll() to
                    // acept job and track full proess group
                    job_t pipeline_job = {0};
                    pipeline_job.stdin_fd = stdin_pipe[1];

                    pipeline_job.pid = left_pid;
                    pipeline_job.right_pid = right_pid;
                    pipeline_job.running = true;
                    pipeline_job.recency = ++recency_counter;

                    snprintf(pipeline_job.command, sizeof(pipeline_job.command),
                             "%s", original_command);

                    if (background) {
                      // background jobs get no typed input
                      if (pipeline_job.stdin_fd >= 0) {
                        close(pipeline_job.stdin_fd);
                        pipeline_job.stdin_fd = -1;
                      }
                      pipeline_job.job_id = next_job_num(jobs, job_count);
                      jobs[job_count++] = pipeline_job;
                    } else {
                      int result = wait_for_child_poll(&pipeline_job, client_fd,
                                                       &pipeline_job.stdin_fd);

                      if (result == -1) {
                        kill(-left_pid, SIGKILL);
                        reap_job(&pipeline_job);
                        goto client_cleanup;
                      }

                      if (result == 1) {
                        if (job_count < MAX_JOBS) {
                          pipeline_job.job_id = next_job_num(jobs, job_count);
                          pipeline_job.stopped = true;
                          pipeline_job.running = false;
                          jobs[job_count++] = pipeline_job;
                        } else {
                          if (pipeline_job.stdin_fd >= 0)
                            close(pipeline_job.stdin_fd);
                          kill(-left_pid, SIGKILL);
                          reap_job(&pipeline_job);
                        }
                      }
                    }
                  }
                }
              }
            }

            // Commands without pipes
            if (left_count > 0 && !invalid_redirection && !invalid_pipe &&
                pipe_index == -1) {

              // Create pipe for interactive stdin
              int stdin_pipe[2];

              if (pipe2(stdin_pipe, O_CLOEXEC) == -1) {
                perror("stdin pipe");
                goto client_cleanup;
              }

              pid_t pid = fork();

              if (pid == -1) {
                perror("fork");

                close(stdin_pipe[0]);
                close(stdin_pipe[1]);
              } else if (pid == 0) {
                // restore normal sigpipe behavior for execute
                struct sigaction child_sa = {0};
                child_sa.sa_handler = SIG_DFL;
                sigemptyset(&child_sa.sa_mask);

                if (sigaction(SIGPIPE, &child_sa, NULL) == -1) {
                  perror("sigaction child SIGPIPE");
                  _exit(1);
                }
                // Establish progress group if there is one!!
                if (setpgid(0, 0) == -1) {
                  perror("pgid");
                  _exit(1);
                }

                // child cannot write to its own stdin pipe
                close(stdin_pipe[1]);

                // Use pipe unless input comes from a file
                if (left_input_file == NULL) {
                  if (dup2(stdin_pipe[0], STDIN_FILENO) == -1) {
                    perror("dup2 stdin");
                    _exit(1);
                  }
                }

                // original read descriptor should not be needed
                close(stdin_pipe[0]);

                if (dup2(client_fd, STDOUT_FILENO) == -1) {
                  _exit(1);
                }
                if (dup2(client_fd, STDERR_FILENO) == -1) {
                  _exit(1);
                }

                // Output redirection from phase 1
                if (left_output_file != NULL) {
                  int fd_out = open(left_output_file,
                                    O_WRONLY | O_CREAT | O_TRUNC, 0664);
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

                umask(start_umask);      // after the > open, so it stays 0664
                signal(SIGHUP, SIG_DFL); // undo the daemon's SIG_IGN
                if (execvp(left_args[0], left_args) == -1) {
                  _exit(127);
                }
              } else {
                close(stdin_pipe[0]); // Parent does not read from child's
                                      // stdin pipe
                // Calls pgid
                if (setpgid(pid, pid) == -1) {
                  if (errno != EACCES) {
                    perror("setpgid parent");
                  }
                }

                // Close unused pipe when input comes from a file
                if (left_input_file != NULL) {
                  close(stdin_pipe[1]);
                }

                // background for parent
                if (background) {
                  // Background jobs won't receive interactive stdin
                  if (left_input_file == NULL) {
                    close(stdin_pipe[1]);
                  }
                  job_t *job = &jobs[job_count];
                  job->pid = pid;
                  job->right_pid = 0;
                  job->left_finished = false;
                  job->right_finished = true;
                  job->left_stopped = false;
                  job->right_stopped = false;
                  job->job_id = next_job_num(jobs, job_count);
                  job->stopped = false;
                  job->running = true;
                  job->recency = ++recency_counter;
                  job->stdin_fd = -1;

                  snprintf(job->command, sizeof(job->command), "%s",
                           original_command);
                  job_count++;
                }

                else { // Poll client messages while child executes
                  job_t foreground_job = {0};

                  foreground_job.pid = pid;
                  foreground_job.right_finished = true;

                  int in_fd = left_input_file == NULL ? stdin_pipe[1] : -1;
                  int child_result =
                      wait_for_child_poll(&foreground_job, client_fd, &in_fd);

                  if (child_result == -1) {
                    goto client_cleanup;
                  }

                  if (child_result == 1 &&
                      job_count < MAX_JOBS) { // if child is stopped
                    job_t *job = &jobs[job_count];

                    job->pid = pid;
                    job->right_pid = 0;
                    job->left_finished = false;
                    job->right_finished = true;
                    job->left_stopped = true;
                    job->right_stopped = false;
                    job->job_id = next_job_num(jobs, job_count);
                    job->stopped = true;
                    job->running = false;
                    job->recency = ++recency_counter;
                    job->stdin_fd = in_fd; // fg feeds cat through it again

                    snprintf(job->command, sizeof(job->command), "%s",
                             original_command);

                    job_count++;
                  } else if (child_result == 1) {
                    // no space to track stopped child
                    if (in_fd >= 0) {
                      close(in_fd);
                    }
                    kill(-pid, SIGKILL);

                    while (waitpid(pid, NULL, 0) == -1 && errno == EINTR) {
                    }
                  }
                }
              }
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
    } else if (bytes_received == 0) {
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

    // resume stopped children so they can be reaped
    if (jobs[j].stopped) {
      kill(-pid, SIGCONT);
    }

    // negative pid signals = whole process group
    kill(-jobs[j].pid, SIGKILL);

    if (jobs[j].stdin_fd >= 0) {
      close(jobs[j].stdin_fd);
    }

    // reap both children if job is a pipeline
    reap_job(&jobs[j]);
  }
  close(client_fd);
  return NULL;
}

static int term_pipe[2];
static pid_t daemon_pid;

// SIGTERM handler: only wake the watcher thread (write is signal-safe)
static void on_term(int sig) {
  // a command between fork and exec still has this handler: let it die
  // the normal way instead of shutting the whole daemon down
  if (getpid() != daemon_pid) {
    signal(sig, SIG_DFL);
    raise(sig);
    return;
  }
  write(term_pipe[1], "x", 1);
}

// kill every process in our session (every client's jobs), then exit
static void *term_watch(void *arg) {
  (void)arg;
  char c;
  while (read(term_pipe[0], &c, 1) == -1 && errno == EINTR) {
  }
  // the pid in it is about to be dead, so don't leave it for kill $(cat ..)
  unlink("/tmp/yashd.pid");
  pid_t me = getpid();
  DIR *proc = opendir("/proc");
  struct dirent *e;
  while (proc != NULL && (e = readdir(proc)) != NULL) {
    pid_t pid = (pid_t)atoi(e->d_name);
    if (pid > 0 && pid != me && getsid(pid) == me)
      kill(pid, SIGKILL);
  }
  exit(EXIT_SUCCESS);
}

static void daemon_init(const char *dir) {
  pid_t pid = fork();
  if (pid < 0)
    exit(EXIT_FAILURE);
  // parent quits
  if (pid > 0)
    exit(EXIT_SUCCESS);
  // new session, no terminal
  setsid();
  signal(SIGHUP, SIG_IGN);
  // change dir to /tmp
  chdir(dir);
  // open() mode is used as-is (save the old one for the commands)
  start_umask = umask(0);
  // drop every inherited fd
  for (int fd = getdtablesize() - 1; fd >= 0; fd--)
    close(fd);
  // lowest free fd = 0 (stdin)
  int null = open("/dev/null", O_RDWR);
  // printf goes nowhere now
  dup2(null, STDOUT_FILENO);
  dup2(null, STDERR_FILENO);
}

int main(void) {

  // daemon creation
  daemon_init("/tmp");
  // get a file to put the pid in
  int pf = open("/tmp/yashd.pid", O_RDWR | O_CREAT | O_CLOEXEC, 0644);
  // if another yashd already has the file then exit
  // so there isn't multiple yashd
  if (pf < 0 || lockf(pf, F_TLOCK, 0) != 0) {
    // stderr is /dev/null by now, so leave the reason in the log
    log_cmd("startup", pf < 0 ? "cannot open /tmp/yashd.pid"
                              : "another yashd is already running");
    exit(EXIT_FAILURE);
  }
  // write pid to file
  char buf[16];
  int n = snprintf(buf, sizeof buf, "%d\n", getpid());
  ftruncate(pf, 0);
  write(pf, buf, n);

  // Prevent broken pipe from terminating everything
  struct sigaction sa = {0};
  sa.sa_handler = SIG_IGN;
  sigemptyset(&sa.sa_mask);

  if (sigaction(SIGPIPE, &sa, NULL) == -1) {
    perror("sigaction SIGPIPE");
    return EXIT_FAILURE;
  }

  // kill yashd (SIGTERM, SIGINT, SIGQUIT): take every client's jobs with it
  daemon_pid = getpid();
  if (pipe2(term_pipe, O_CLOEXEC) == -1) {
    perror("term pipe");
    return EXIT_FAILURE;
  }
  struct sigaction term_sa = {0};
  term_sa.sa_handler = on_term;
  term_sa.sa_flags = SA_RESTART;
  sigemptyset(&term_sa.sa_mask);
  if (sigaction(SIGTERM, &term_sa, NULL) == -1 ||
      sigaction(SIGINT, &term_sa, NULL) == -1 ||
      sigaction(SIGQUIT, &term_sa, NULL) == -1) {
    perror("sigaction TERM/INT/QUIT");
    return EXIT_FAILURE;
  }
  pthread_t watcher;
  pthread_create(&watcher, NULL, term_watch, NULL);
  pthread_detach(watcher);

  int server_fd = socket(AF_INET, SOCK_STREAM,
                         0); // server_fd contains socket file descriptor &
                             // socket creates the IPv4 TCP socket

  if (server_fd == -1) {
    perror("socket");
    exit(EXIT_FAILURE);
  }

  int flags = fcntl(server_fd, F_GETFD);
  // one of the ports was still active during server testing
  if (flags == -1 || fcntl(server_fd, F_SETFD, flags | FD_CLOEXEC) == -1) {
    perror("fcntl FD_CLOEXEC");
    close(server_fd);
    exit(EXIT_FAILURE);
  }

  // allow for quick TCP server restarts
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) ==
      -1) {
    perror("setsockopt SO_REUSEADDR");
    close(server_fd);
    return EXIT_FAILURE;
  }

  // socket creation phase
  struct sockaddr_in
      server_addr; // holds server's local address, IPv4 specific address
  server_addr = (struct sockaddr_in){0}; // IPv4 address structure
  server_addr.sin_family = AF_INET;      // Which address family that is used
  server_addr.sin_port = htons(PORT);
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

  // binding: assigning local IP address and port
  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) ==
      -1) {
    // stderr is /dev/null by now, so leave the reason in the log
    char why[128];
    snprintf(why, sizeof(why), "bind port %d: %s", PORT, strerror(errno));
    log_cmd("startup", why);
    unlink("/tmp/yashd.pid");
    perror("bind");
    close(server_fd);
    exit(EXIT_FAILURE);
  }
  // listening: creating the queue
  if (listen(server_fd, SOMAXCONN) == -1) {
    perror("listen");
    close(server_fd);
    exit(EXIT_FAILURE);
  }

  while (1) {
    // one connection per thread
    conn_t *conn = malloc(sizeof(*conn));
    if (conn == NULL)
      continue; // out of memory
    socklen_t len = sizeof(conn->addr);
    // SOCK_CLOEXEC: other clients' commands don't inherit this socket
    conn->fd =
        accept4(server_fd, (struct sockaddr *)&conn->addr, &len, SOCK_CLOEXEC);

    if (conn->fd == -1) {
      free(conn);
      continue;
    }

    pthread_t thread_id;
    if (pthread_create(&thread_id, NULL, handle_client, conn) != 0) {
      perror("pthread_create error");
      close(server_fd);
      free(conn);
      exit(EXIT_FAILURE);
    }

    pthread_detach(thread_id);
  }

  close(server_fd);
}
