CFLAGS=-I.. -g -Wall -Werror -lm -I../../thirdparties/install/include

all: test_add.exe xxhash.exe test_bloom.exe

test_add.exe: test_add.c ../libmo.a
	$(CC) $(CFLAGS) -o test_add.exe test_add.c -L.. -lmo

test_bloom.exe: test_bloom.c ../libmo.a
	$(CC) $(CFLAGS) -o test_bloom.exe test_bloom.c -L.. -lmo

xxhash.exe: xxhash.c ../libmo.a
	$(CC) $(CFLAGS) -o xxhash.exe xxhash.c -L.. -L../../thirdparties/install/lib -lusearch_c -lmo

clean:
	rm -f *.o *.exe
