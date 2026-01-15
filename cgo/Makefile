DEBUG_OPT :=
UNAME_M := $(shell uname -m)

# Yeah, fast math.  We want it to be fast, for all xcall, 
# IEEE compliance should not be an issue.
OPT_LV := -O3 -ffast-math
CFLAGS=-std=c99 -g ${OPT_LV} -Wall -Werror
OBJS=mo.o arith.o compare.o logic.o xcall.o
CUDA_OBJS=

ifeq ($(UNAME_M), x86_64)
	CFLAGS+= -march=haswell
endif

ifeq ($(MO_CL_CUDA),1)
	CC = /usr/local/cuda/bin/nvcc 
	CFLAGS = -ccbin g++ -m64 --shared -gencode arch=compute_75,code=sm_75 -gencode arch=compute_80,code=sm_80 -gencode arch=compute_86,code=sm_86 -gencode arch=compute_89,code=sm_89 -gencode arch=compute_90,code=sm_90 -gencode arch=compute_90,code=compute_90
	CFLAGS += -DMO_CL_CUDA
	CUDA_OBJS += cuda/cuda.o
	CUDA_LDFLAGS := -L/usr/local/cuda/lib64/stubs -lcuda -L/usr/local/cuda/lib64 -lcudart -lstdc++
endif

all: libmo.a

libmo.a: $(OBJS) 
ifeq ($(MO_CL_CUDA),1)
	make -C cuda
endif
	ar -rcs libmo.a $(OBJS) $(CUDA_OBJS)

# 
#	$(CC) -o libmo.a $(OBJS) $(CUDA_OBJS) $(CUDA_LDFLAGS)



.PHONY: debug
debug: override OPT_LV := -O0
debug: override DEBUG_OPT := debug
debug: all

.PHONY: clean
clean:
	rm -f *.o *.a *.so
ifeq ($(MO_CL_CUDA),1)
	make -C cuda clean
endif
