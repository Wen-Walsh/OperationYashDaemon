#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define PORT 3820

// CTRL-C and CTRL-Z handlers
static int sock = -1;
static void on_sigint(int sig) {
  (void)sig;
  send(sock, "CTL c\n", 6, MSG_NOSIGNAL);
}
static void on_sigtstp(int sig) {
  (void)sig;
  send(sock, "CTL z\n", 6, MSG_NOSIGNAL);
}

int main(int argc, char *argv[]) {

  if (argc != 2) {
    fprintf(stderr, "usage: %s <ip>\n", argv[0]);
    exit(1);
  }

  // server_fd contains socket file descriptor & socket creates the IPv4 TCP
  // socket
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd == -1) {
    perror("socket");
    exit(EXIT_FAILURE);
  }

  // socket setup
  struct sockaddr_in server_addr;        // holds server's address
  server_addr = (struct sockaddr_in){0}; // IPv4 address structure
  server_addr.sin_family = AF_INET;      // Which address family that is used
  server_addr.sin_port = htons(PORT);    // defined port
  if (inet_pton(AF_INET, argv[1], &server_addr.sin_addr) != 1) {
    fprintf(stderr, "bad ip: %s\n", argv[1]);
    close(server_fd);
    exit(EXIT_FAILURE);
  }

  // connect to server
  if (connect(server_fd, (struct sockaddr *)&server_addr,
              sizeof server_addr) == -1) { // blocks until accepted
    perror("connect");
    close(server_fd);
    exit(EXIT_FAILURE);
  }

  // CTRL-C and CTRL-Z
  sock = server_fd;
  signal(SIGINT, on_sigint);
  signal(SIGTSTP, on_sigtstp);

  struct pollfd fds[2] = {
      {.fd = STDIN_FILENO, .events = POLLIN},
      {.fd = server_fd, .events = POLLIN},
  };

  // main loop
  char line[256], cmd[300], buf[1024];
  while (1) {
    if (poll(fds, 2, -1) == -1) {
      if (errno == EINTR)
        continue;
      perror("poll");
      break;
    }

    // server sent something
    if (fds[1].revents & (POLLIN | POLLHUP)) {
      ssize_t n = recv(server_fd, buf, sizeof buf, 0);
      if (n <= 0)
        break;
      fwrite(buf, 1, n, stdout);
      fflush(stdout);
    }

    // send loop
    if (fds[0].revents & POLLIN) {
      // get stdin and check for Ctrl-D (NULL)
      if (fgets(line, sizeof line, stdin) == NULL)
        break;
      // drop the newline
      line[strcspn(line, "\n")] = '\0';
      if (strcmp(line, "quit") == 0)
        break;
      // send
      int len = snprintf(cmd, sizeof cmd, "CMD %s\n", line);
      int sent = 0;
      while (sent < len) {
        ssize_t n = send(server_fd, cmd + sent, len - sent, MSG_NOSIGNAL);
        if (n == -1 && errno == EINTR) {
          continue;
        }
        if (n == -1) {
          perror("send");
          exit(EXIT_FAILURE);
        }
        sent += n;
      }
    }
  }

  close(server_fd);
}
