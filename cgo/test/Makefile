CFLAGS=-I.. -g -Wall -Werror -lm -I../../thirdparties/install/include

all: test_add.exe xxhash.exe 

test_add.exe: test_add.c ../libmo.a
	$(CC) $(CFLAGS) -o test_add.exe test_add.c -L.. -lmo

xxhash.exe: xxhash.c ../libmo.a
	$(CC) $(CFLAGS) -o xxhash.exe xxhash.c -L.. -L../../thirdparties/install/lib -lusearch_c -lmo

clean:
	rm -f *.o *.exe
