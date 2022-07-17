CFLAGS=-g -O3 -Wall -Werror
OBJS=mo.o arith.o
LIBS=./external/lib/libdecnumber.a

%.o: %.c mo.h mo_impl.h
	$(CC) $(CFLAGS) -c -o $@ $< 

libmo.a: $(OBJS) $(LIBS)
	ar rcs libmo.a $(LIBS) $(OBJS)
	ranlib libmo.a

external/lib/libdecnumber.a:
	(cd external; bash ./build.sh)

.PHONY: libs

.PHONY: clean
clean:
	rm -f *.o *.a
