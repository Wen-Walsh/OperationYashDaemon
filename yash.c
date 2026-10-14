#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>

#define PORT 3820

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

  struct sockaddr_in server_addr;        // holds server's address
  server_addr = (struct sockaddr_in){0}; // IPv4 address structure
  server_addr.sin_family = AF_INET;      // Which address family that is used
  server_addr.sin_port = htons(PORT);    // defined port
  if (inet_pton(AF_INET, argv[1], &server_addr.sin_addr) != 1) {
    fprintf(stderr, "bad ip: %s\n", argv[1]);
    close(server_fd);
    exit(EXIT_FAILURE);
  }

  if (connect(server_fd, (struct sockaddr *)&server_addr,
              sizeof server_addr) == -1) { // blocks until accepted
    perror("connect");
    close(server_fd);
    exit(EXIT_FAILURE);
  }
  printf("connected\n");
  close(server_fd);
}
