CFLAGS=-I.. -g -Wall -Werror

test_add.exe: test_add.c ../libmo.a
	$(CC) $(CFLAGS) -o test_add.exe test_add.c -L.. -lmo

clean:
	rm -f *.o *.exe
