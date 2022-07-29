CFLAGS=-I./external/decNumber -g -O3 -Wall -Werror
OBJS=mo.o arith.o decimal.o

all: $(OBJS)
	$(MAKE) -C external/decNumber
	ar -rcs libmo.a *.o
	rm -f *.o

.PHONY: clean
clean:
	rm -f *.o *.a
