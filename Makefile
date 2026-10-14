CC = gcc
CFLAGS = -Wall -Wextra -std=gnu99 -g -pthread

all: yash yashd

yash: yash.c
	$(CC) $(CFLAGS) -o yash yash.c

yashd: yashd.c
	$(CC) $(CFLAGS) -o yashd yashd.c

clean:
	rm -f yash yashd

.PHONY: clean all
