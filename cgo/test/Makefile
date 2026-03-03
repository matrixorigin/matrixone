CFLAGS=-I.. -g -I../../thirdparties/install/include
NVCC_FLAGS=-Xcompiler "-Wall -Werror"
LDFLAGS=-L.. -lmo_c -Xlinker "-rpath=$(shell realpath ..)" -lm

all: test_add.exe test_bloom.exe test_varlena.exe bloom_whole_test.exe

test_add.exe: test_add.c ../libmo_c.so
	$(CC) $(CFLAGS) $(NVCC_FLAGS) -o test_add.exe test_add.c $(LDFLAGS)

test_bloom.exe: test_bloom.c ../libmo_c.so
	$(CC) $(CFLAGS) $(NVCC_FLAGS) -o test_bloom.exe test_bloom.c $(LDFLAGS)

test_varlena.exe: varlena_test.c ../libmo_c.so
	$(CC) $(CFLAGS) $(NVCC_FLAGS) -o test_varlena.exe varlena_test.c $(LDFLAGS)

bloom_whole_test.exe: bloom_whole_test.c ../libmo_c.so
	$(CC) $(CFLAGS) $(NVCC_FLAGS) -o bloom_whole_test.exe bloom_whole_test.c $(LDFLAGS)

clean:
	rm -f *.o *.exe
