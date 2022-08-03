#
# We (MO) added this makefile.
#
# CFLAGS is -g -O2 because -Wall and -O3 will generate warnings.
# There are patches on internet that silence these warnings, here
# we just trust the decNumber authors.
#

OPT_LV := -O2
CFLAGS=-g ${OPT_LV}

OBJS=decSingle.o decDouble.o decQuad.o decNumber.o decContext.o

all: $(OBJS)
	mv $(OBJS) ../..

.PHONY: debug
debug: override OPT_LV := -O0
debug: all
