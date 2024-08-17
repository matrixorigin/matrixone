DEBUG_OPT :=
# Yeah, fast math.  We want it to be fast, for all xcall, 
# IEEE compliance should not be an issue.
OPT_LV := -O3 -ffast-math
CFLAGS=-std=c99 -g ${OPT_LV} -Wall -Werror
OBJS=mo.o arith.o compare.o logic.o xcall.o

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
