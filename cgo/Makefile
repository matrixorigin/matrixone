CFLAGS=-I./external/decNumber -g -O3 -Wall -Werror
OBJS=mo.o arith.o decimal.o decSingle.o decDouble.o decQuad.o decNumber.o decContext.o

all: $(OBJS)
	ar -rcs libmo.a *.o
	rm -f *.o

decNumberCFLAGS=-g -O2
decNumberSRC=external/decNumber
%.o: $(decNumberSRC)/%.c
	$(CC) -M $(decNumberCFLAGS) -c -o $@ $<

.PHONY: clean
clean:
	rm -f *.o *.a
