CFLAGS=-I./external/decNumber -g -O3 -Wall -Werror
OBJS=mo.o arith.o decimal.o

all: libmo.a

libmo.a: $(OBJS)
	$(MAKE) -C external/decNumber
	ar -rcs libmo.a *.o

.PHONY: clean
clean:
	rm -f *.o *.a
