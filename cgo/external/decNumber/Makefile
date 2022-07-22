#
# We (MO) added this makefile.   
#
# CFLAGS is -g -O2 because -Wall and -O3 will generate warnings.  
# There are patches on internet that silence these warnings, here
# we just trust the decNumber authors.
#

OBJS = decSingle.o decDouble.o decQuad.o decNumber.o decContext.o

CFLAGS = -g -O2

libdecnumber.a: $(OBJS)
	ar -rcs libdecnumber.a $(OBJS)

clean:
	rm -f *.a *.o

install: libdecnumber.a
	mkdir -p ../include/decnumber
	mkdir -p ../lib
	cp *.h ../include/decnumber/
	cp libdecnumber.a ../..
