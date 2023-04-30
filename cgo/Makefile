DEBUG_OPT :=
OPT_LV := -O3
CFLAGS=-std=c99 -g ${OPT_LV} -Wall -Werror
OBJS=mo.o arith.o compare.o logic.o

all: libmo.a

libmo.a: $(OBJS)
	ar -rcs libmo.a *.o

.PHONY: debug
debug: override OPT_LV := -O0
debug: override DEBUG_OPT := debug
debug: all

.PHONY: clean
clean:
	rm -f *.o *.a
