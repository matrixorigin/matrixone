CFLAGS=-g -O3 -Wall -Werror
OBJS=mo.o arith.o

%.o: %.c mo.h mo_impl.h
	$(CC) $(CFLAGS) -c -o $@ $< 

libmo.a: $(OBJS)
	ar rcs libmo.a $(OBJS)
	ranlib libmo.a

.PHONY: clean
clean:
	rm -f *.o *.a
