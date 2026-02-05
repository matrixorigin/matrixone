CFLAGS=-I.. -g -Wall -Werror -lm -I../../thirdparties/install/include

all: test_add.exe test_bloom.exe test_varlena.exe bloom_whole_test.exe

test_add.exe: test_add.c ../libmo.a
	$(CC) $(CFLAGS) -o test_add.exe test_add.c -L.. -lmo

test_bloom.exe: test_bloom.c ../libmo.a
	$(CC) $(CFLAGS) -o test_bloom.exe test_bloom.c -L.. -lmo

test_varlena.exe: varlena_test.c ../libmo.a
	$(CC) $(CFLAGS) -o test_varlena.exe varlena_test.c -L.. -lmo

bloom_whole_test.exe: bloom_whole_test.c ../libmo.a
	$(CC) $(CFLAGS) -o bloom_whole_test.exe bloom_whole_test.c -L.. -lmo

clean:
	rm -f *.o *.exe
