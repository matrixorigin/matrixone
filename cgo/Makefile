DEBUG_OPT :=
UNAME_M := $(shell uname -m)
CC ?= gcc

# Yeah, fast math.  We want it to be fast, for all xcall, 
# IEEE compliance should not be an issue.
OPT_LV := -O3 -ffast-math -ftree-vectorize -funroll-loops
COMMON_CFLAGS := -g $(OPT_LV) -Wall -Werror -fPIC -I../thirdparties/install/include
CFLAGS := -std=c99 $(COMMON_CFLAGS)
OBJS := mo.o arith.o compare.o logic.o xcall.o usearchex.o bloom.o
CUDA_OBJS :=
LDFLAGS := -shared

ifeq ($(UNAME_M), x86_64)
	CFLAGS += -march=haswell
endif

ifeq ($(MO_CL_CUDA),1)
	ifeq ($(CONDA_PREFIX),)
		$(error CONDA_PREFIX env variable not found. Please activate your conda environment.)
	endif
	CC = /usr/local/cuda/bin/nvcc 
	CFLAGS = -ccbin g++ -m64 -Xcompiler -fPIC -gencode arch=compute_75,code=sm_75 -gencode arch=compute_80,code=sm_80 -gencode arch=compute_86,code=sm_86 -gencode arch=compute_89,code=sm_89 -gencode arch=compute_90,code=sm_90 -gencode arch=compute_90,code=compute_90
	CFLAGS += -I../thirdparties/install/include -DMO_CL_CUDA
	CUDA_OBJS += cuda/cuda.o
	# Explicitly include all needed libraries for shared library linking
	CUDA_LDFLAGS := -L/usr/local/cuda/lib64/stubs -lcuda -L/usr/local/cuda/lib64 -lcudart -L$(CONDA_PREFIX)/lib -lcuvs -lcuvs_c -ldl -lrmm -lstdc++
	LDFLAGS += $(CUDA_LDFLAGS)
endif

.PHONY: all clean test debug

all: libmo_c.so libmo_c.a

libmo_c.so: $(OBJS) 
ifeq ($(MO_CL_CUDA),1)
	$(MAKE) -C cuda
	$(MAKE) -C cuvs
	$(CC) $(LDFLAGS) -o $@ $(OBJS) $(CUDA_OBJS) cuvs/*.o
else
	$(CC) $(LDFLAGS) -o $@ $(OBJS)
endif

libmo_c.a: $(OBJS)
ifeq ($(MO_CL_CUDA),1)
	$(MAKE) -C cuda
	$(MAKE) -C cuvs
	ar -rcs $@ $(OBJS) $(CUDA_OBJS) cuvs/*.o
else
	ar -rcs $@ $(OBJS)
endif

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

test: libmo_c.so
	$(MAKE) -C test

debug: override OPT_LV := -O0
debug: override DEBUG_OPT := debug
debug: all

clean:
	rm -f *.o *.a *.so
ifeq ($(MO_CL_CUDA),1)
	$(MAKE) -C cuda clean
	$(MAKE) -C cuvs clean
endif
