CFLAGS=-I. -I./external/include -g -O3 -Wall -Werror
OBJS=mo.o arith.o decimal.o
LIBS=libdecnumber.a

%.o: %.c mo.h mo_impl.h
	$(CC) $(CFLAGS) -c -o $@ $< 

all: $(LIBS) libmo.a

libmo.a: $(OBJS) 
	ar -rcs libmo.a $(OBJS)
	ranlib libmo.a

libdecnumber.a:
	(cd external; bash ./build.sh)

.PHONY: clean
clean:
	rm -f *.o *.a
