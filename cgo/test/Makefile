UNAME_S := $(shell uname -s)
TEST_CC = $(CC)

ifeq ($(MO_CL_CUDA),1)
	include ../gpu-toolchain.mk
	TEST_CC = $(MO_GPU_NVCC) -ccbin $(MO_GPU_CXX)
	COMPILER_FLAGS := -Xcompiler "-Wall -Werror"
	# When using nvcc to link, we need to pass the libraries and rpath
	LINKER_FLAGS := -Xlinker "-rpath=$(shell realpath ..)"
	# We must also include the cuVS and other deps that libmo.so needs if linked statically, 
	# but since libmo.so is shared, we just need to link against it.
	LIBS += -L.. -lmo -L../../thirdparties/install/lib -lusearch_c $(MO_GPU_LDFLAGS)
	LIBS += -ldl -lrmm -lpthread -lgomp
	LIBS += -Xlinker -lpthread -Xlinker -lm
else
	COMPILER_FLAGS := -Wall -Werror
	ifeq ($(UNAME_S),Darwin)
		LINKER_FLAGS := -Wl,-rpath,$(shell realpath ..)
	else
		LINKER_FLAGS := -Wl,-rpath=$(shell realpath ..)
	endif
	LIBS := -L.. -lmo -L../../thirdparties/install/lib -lusearch_c -lm -lstdc++
	ifneq ($(UNAME_S),Darwin)
		LIBS += -fopenmp
	endif
endif

CFLAGS := -I.. -g -I../../thirdparties/install/include $(COMPILER_FLAGS) $(MO_GPU_CFLAGS)
LDFLAGS := $(LIBS) -lroaring $(LINKER_FLAGS)

ifeq ($(MO_CL_CUDA),1)
# test_xcall_cuda compares the C loop against the CUDA kernel, so it only exists in a CUDA build.
all: test_add.exe test_bloom.exe test_varlena.exe test_cbitmap.exe test_croaring.exe test_xcall_cuda.exe
else
all: test_add.exe test_bloom.exe test_varlena.exe test_cbitmap.exe test_croaring.exe
endif

test_xcall_cuda.exe: test_xcall_cuda.c ../xcall.h
	$(TEST_CC) $(CFLAGS) -DMO_CL_CUDA -o $@ test_xcall_cuda.c $(LDFLAGS)

test_add.exe: test_add.c
	$(TEST_CC) $(CFLAGS) -o $@ test_add.c $(LDFLAGS)

test_cbitmap.exe: test_cbitmap.c
	$(TEST_CC) $(CFLAGS) -o $@ test_cbitmap.c $(LDFLAGS)

test_croaring.exe: test_croaring.c
	$(TEST_CC) $(CFLAGS) -o $@ test_croaring.c $(LDFLAGS)

test_bloom.exe: test_bloom.c
	$(TEST_CC) $(CFLAGS) -o $@ test_bloom.c $(LDFLAGS)

test_varlena.exe: varlena_test.c
	$(TEST_CC) $(CFLAGS) -o $@ varlena_test.c $(LDFLAGS)

bloom_whole_test.exe: bloom_whole_test.c 
	$(TEST_CC) $(CFLAGS) $(NVCC_FLAGS) -o bloom_whole_test.exe bloom_whole_test.c $(LDFLAGS)

clean:
	rm -f *.o *.exe
