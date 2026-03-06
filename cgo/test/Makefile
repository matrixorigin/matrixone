UNAME_S := $(shell uname -s)

ifeq ($(MO_CL_CUDA),1)
        ifeq ($(CONDA_PREFIX),)
                $(error CONDA_PREFIX env variable not found. Please activate your conda environment.)
        endif
        CC = /usr/local/cuda/bin/nvcc
	COMPILER_FLAGS := -Xcompiler "-Wall -Werror"
	# When using nvcc to link, we need to pass the libraries and rpath
	LINKER_FLAGS := -Xlinker "-rpath=$(shell realpath ..)"
	# We must also include the cuVS and other deps that libmo.so needs if linked statically, 
	# but since libmo.so is shared, we just need to link against it.
	LIBS += -L.. -lmo -L../../thirdparties/install/lib -lusearch_c -L$(CUDA_PATH)/lib64/stubs -lcuda -L$(CUDA_PATH)/lib64 -lcudart
	LIBS += -L$(CONDA_PREFIX)/lib -lcuvs -lcuvs_c -ldl -lrmm -lpthread  -lgomp
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

CFLAGS := -I.. -g -I../../thirdparties/install/include $(COMPILER_FLAGS)
LDFLAGS := $(LIBS) $(LINKER_FLAGS)

all: test_add.exe test_bloom.exe test_varlena.exe

test_add.exe: test_add.c 
	$(CC) $(CFLAGS) -o $@ test_add.c $(LDFLAGS)

test_bloom.exe: test_bloom.c
	$(CC) $(CFLAGS) -o $@ test_bloom.c $(LDFLAGS)

test_varlena.exe: varlena_test.c
	$(CC) $(CFLAGS) -o $@ varlena_test.c $(LDFLAGS)

bloom_whole_test.exe: bloom_whole_test.c 
	$(CC) $(CFLAGS) $(NVCC_FLAGS) -o bloom_whole_test.exe bloom_whole_test.c $(LDFLAGS)

clean:
	rm -f *.o *.exe
