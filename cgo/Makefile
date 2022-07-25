CFLAGS=-I. -I./external/include -g -O3 -Wall -Werror
OBJS=mo.o arith.o decimal.o
LIBS=libdecnumber.a

%.o: %.c mo.h mo_impl.h
	$(CC) $(CFLAGS) -c -o $@ $< 

all: $(LIBS) $(OBJS)
	ar -x libdecnumber.a
	ar -rcs libmo.a *.o
	ranlib libmo.a
	rm -f *.o
	rm -f $(LIBS)

libdecnumber.a:
	(cd external; bash ./build.sh)

.PHONY: clean
clean:
	rm -f *.o *.a
