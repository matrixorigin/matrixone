CFLAGS=-I. -I./external/include -g -O3 -Wall -Werror
OBJS=mo.o arith.o decimal.o
LIBS=libdecnumber.a

%.o: %.c mo.h mo_impl.h
	$(CC) $(CFLAGS) -c -o $@ $< 

all: $(LIBS) libmo.a
	ar -x libmo.a
	ar -x libdecnumber.a
	ar -rcs libmo.a *.o

libmo.a: $(OBJS) 
	ar -rcs libmo.a $(OBJS)
	ranlib libmo.a
	rm -f *.o

libdecnumber.a:
	(cd external; bash ./build.sh)
	rm -f *.o

.PHONY: clean
clean:
	rm -f *.o *.a
