#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "../varlena.h"

void test_small_varlena() {
    printf("--- Running test_small_varlena --- \n");
    char v[VARLENA_SIZE];
    
    // Test small varlena
    const char* test_str = "hello small varlena";
    uint8_t test_len = strlen(test_str);
    
    v[0] = test_len;
    memcpy(v + 1, test_str, test_len);

    assert(varlena_is_small((varlena_t)v));
    
    uint32_t len;
    const uint8_t *slice = varlena_get_byte_slice((varlena_t)v, NULL, &len);
    
    assert(len == test_len);
    assert(strncmp((char *)slice, test_str, len) == 0);
    
    printf("test_small_varlena passed.");
}

void test_big_varlena() {
    printf("--- Running test_big_varlena --- \n"); 
    char v[VARLENA_SIZE];
    uint8_t area[1024];

    // Test big varlena
    const char* test_str = "this is a long string that does not fit in small varlena";
    uint32_t test_len = strlen(test_str);
    uint32_t test_offset = 100;

    // Simulate a big varlena by setting the first byte to a value > VARLENA_INLINE_SIZE
    // and setting offset/length.
    // Note: The current varlena.h doesn't have a setter for big varlena,
    // so we manually craft it.
    v[0] = VARLENA_INLINE_SIZE + 1; 
    uint32_t* p = (uint32_t*)v;
    p[0] = test_offset; 
    p[1] = test_len;

    // Copy test string into the 'area'
    memcpy(area + test_offset, test_str, test_len);

    assert(!varlena_is_small((varlena_t)v));

    uint32_t offset, length;
    varlena_get_offset_len((varlena_t)v, &offset, &length);
    assert(offset == test_offset);
    assert(length == test_len);

    uint32_t slice_len;
    const uint8_t *slice = varlena_get_byte_slice((varlena_t)v, area, &slice_len);
    
    assert(slice_len == test_len);
    assert(strncmp((char *)slice, test_str, slice_len) == 0);
    assert((char *)slice == (const char*)(area + test_offset));

    printf("test_big_varlena passed.\n");
}


int main() {
    test_small_varlena();
    test_big_varlena();
    printf("All varlena tests passed!\n");
    return 0;
}
