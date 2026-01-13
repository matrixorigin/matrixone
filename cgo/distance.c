#include "usearch_extend.h"
#include <string.h>
#include <stdio.h>
#include <stdint.h>

#define XXH_INLINE_ALL
#include "xxhash.h"

float mo_distance(void const* vector_first, void const* vector_second, size_t dimensions, mo_error_t* error) {
	return usearch_distance(vector_first, vector_second, usearch_scalar_f32_k, dimensions, usearch_metric_l2sq_k, error);
}

void xxhash_test() {
    const char* input = "xxHash - Extremely Fast Hash algorithm";
    const int inputLen = strlen(input);
    const uint32_t seed = 0;

    printf("input     = %s\n"    , input);
    printf("inputLen  = %d\n"    , inputLen);

    const uint32_t h1 = XXH32(input, inputLen, seed);
    printf("XXH32()   = 0x%08x\n", h1);

}
