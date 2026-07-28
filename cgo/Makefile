DEBUG_OPT :=
UNAME_M := $(shell uname -m)
UNAME_S := $(shell uname -s)
CC ?= gcc

# Yeah, fast math.  We want it to be fast, for all xcall, 
# IEEE compliance should not be an issue.
OPT_LV := -O3 -ffast-math -ftree-vectorize -funroll-loops
COMMON_CFLAGS := -g $(OPT_LV) -Wall -Werror -fPIC -I../thirdparties/install/include
CFLAGS := -std=c11 $(COMMON_CFLAGS)
OBJS := mo.o arith.o compare.o logic.o xcall.o usearchex.o bloom.o croaring.o cbitmap.o
CUDA_OBJS :=
LDFLAGS := -L../thirdparties/install/lib -lusearch_c -lroaring
TARGET_LIB := libmo.so

ifeq ($(UNAME_S),Darwin)
	TARGET_LIB := libmo.dylib
	LDFLAGS += -dynamiclib -undefined dynamic_lookup -install_name @rpath/$(TARGET_LIB)
else
	LDFLAGS += -shared
endif

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

.PHONY: all clean test debug cuda_objs cuvs_objs

# Pin the default goal: the cuda_objs/cuvs_objs targets below are defined before
# `all`, so without this `make` (no explicit target) would build cuda_objs first
# and leave libmo.so/libmo.a unbuilt (-> the top build fails with "cannot find -lmo").
.DEFAULT_GOAL := all

ifeq ($(MO_CL_CUDA),1)
# The cuda/cuvs objects are produced by sub-makes (incremental — only changed
# .cu/.cpp/.hpp recompile). cuda_objs / cuvs_objs are ORDER-ONLY prerequisites,
# so the sub-makes ALWAYS run (rebuilding any changed object); because those
# phony prereqs always "run", libmo.so / libmo.a are re-linked on EVERY build
# from the freshest objects. The relink is cheap (~1s) and GUARANTEES the libs
# can never be stale.
#
# Why this matters: the old rules listed only $(OBJS) (the cgo C objects), so a
# change to only a cuvs source rebuilt cuvs/*.o but left libmo.so STALE. Since
# mo-service loads libmo.so *dynamically*, it then silently ran old C++ code —
# costing hours of "fix doesn't work" confusion. Always-relink ends that, and
# you no longer need the `rm -f cgo/libmo.so cgo/libmo.a` workaround.
# (The wildcard normal-prereqs add no "no rule" errors on a clean build — they
# match only already-built objects; the order-only sub-makes create them.)
cuda_objs:
	$(MAKE) -C cuda
cuvs_objs:
	$(MAKE) -C cuvs
LIB_PREREQS := $(OBJS) $(wildcard cuda/*.o cuvs/*.o) | cuda_objs cuvs_objs
else
LIB_PREREQS := $(OBJS)
endif

all: $(TARGET_LIB) libmo.a

$(TARGET_LIB): $(LIB_PREREQS)
ifeq ($(MO_CL_CUDA),1)
	$(CC) $(LDFLAGS) -o $@ $(OBJS) $(CUDA_OBJS) cuvs/*.o
else
	$(CC) $(LDFLAGS) -o $@ $(OBJS)
endif

libmo.a: $(LIB_PREREQS)
ifeq ($(MO_CL_CUDA),1)
	ar -rcs $@ $(OBJS) $(CUDA_OBJS) cuvs/*.o
else
	ar -rcs $@ $(OBJS)
endif

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

test: $(TARGET_LIB)
	$(MAKE) -C test

debug: override OPT_LV := -O0
debug: override DEBUG_OPT := debug
debug: all

clean:
	rm -f *.o *.a *.so *.dylib
ifeq ($(MO_CL_CUDA),1)
	$(MAKE) -C cuda clean
	$(MAKE) -C cuvs clean
endif
