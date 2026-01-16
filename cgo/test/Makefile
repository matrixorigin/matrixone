CFLAGS=-I.. -g -Wall -Werror -lm -I../../thirdparties/install/include

all: test_add.exe test_bloom.exe

test_add.exe: test_add.c ../libmo.a
	$(CC) $(CFLAGS) -o test_add.exe test_add.c -L.. -lmo

test_bloom.exe: test_bloom.c ../libmo.a
	$(CC) $(CFLAGS) -o test_bloom.exe test_bloom.c -L.. -lmo

clean:
	rm -f *.o *.exe
