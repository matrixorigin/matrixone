#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "../varlena.h"

void test_small_varlena() {
    printf("--- Running test_small_varlena --- \n");
    uint8_t v[VARLENA_SIZE];
    
    // Test small varlena
    const char* test_str = "hello small varlena";
    uint8_t test_len = strlen(test_str);
    
    v[0] = test_len;
    memcpy(v + 1, test_str, test_len);

    assert(varlena_is_small(v));
    
    uint32_t len;
    const uint8_t *slice = varlena_get_byte_slice(v, NULL, &len);
    
    assert(len == test_len);
    assert(strncmp((char *)slice, test_str, len) == 0);
    
    printf("test_small_varlena passed.");
}

void test_big_varlena() {
    printf("--- Running test_big_varlena --- \n"); 
    uint8_t v[VARLENA_SIZE];
    uint8_t area[1024];

    // Test big varlena
    const char* test_str = "this is a long string that does not fit in small varlena";
    uint32_t test_len = strlen(test_str);
    uint32_t test_offset = 100;

    // Simulate a big varlena by setting the first byte to a value > VARLENA_INLINE_SIZE
    // and setting offset/length.
    varlena_set_big_offset_len(v, test_offset, test_len);

    // Copy test string into the 'area'
    memcpy(area + test_offset, test_str, test_len);

    assert(!varlena_is_small(v));

    uint32_t offset, length;
    varlena_get_big_offset_len(v, &offset, &length);
    assert(offset == test_offset);
    assert(length == test_len);

    uint32_t slice_len;
    const uint8_t *slice = varlena_get_byte_slice(v, area, &slice_len);
    
    assert(slice_len == test_len);
    assert(strncmp((char *)slice, test_str, slice_len) == 0);
    assert((char *)slice == (const char*)(area + test_offset));

    printf("test_big_varlena passed.\n");
}


void test_multi_varlena_buffer() {
    printf("--- Running test_multi_varlena_buffer ---\n");

    uint8_t buffer[100];
    uint8_t area[100];
    
    // --- Construct buffer ---
    uint8_t *ptr = buffer;

    // Element 1: small
    const char *str1 = "small1";
    uint8_t len1 = strlen(str1);
    ptr[0] = len1;
    memcpy(ptr + 1, str1, len1);
    ptr += VARLENA_SIZE;

    // Element 2: big
    const char *str2 = "this is a big string for testing";
    uint32_t len2 = strlen(str2);
    uint32_t offset2 = 40; // Use offset > 23 to make is_small() false
    memcpy(area + offset2, str2, len2);
    
    varlena_set_big_offset_len(ptr, offset2, len2);
    ptr += VARLENA_SIZE;

    // Element 3: small
    const char *str3 = "small2";
    uint8_t len3 = strlen(str3);
    ptr[0] = len3;
    memcpy(ptr + 1, str3, len3);

    // --- Traverse and check ---
    uint8_t *v = buffer;

    // Check element 1
    uint32_t current_len1;
    const uint8_t *current_data1 = varlena_get_byte_slice(v, area, &current_len1);
    assert(current_len1 == len1);
    assert(memcmp(current_data1, str1, len1) == 0);
    v += VARLENA_SIZE;

    // Check element 2
    uint32_t current_len2;
    const uint8_t *current_data2 = varlena_get_byte_slice(v, area, &current_len2);
    assert(current_len2 == len2);
    assert(memcmp(current_data2, str2, len2) == 0);
    v += VARLENA_SIZE;

    // Check element 3
    uint32_t current_len3;
    const uint8_t *current_data3 = varlena_get_byte_slice(v, area, &current_len3);
    assert(current_len3 == len3);
    assert(memcmp(current_data3, str3, len3) == 0);
    
    printf("test_multi_varlena_buffer passed.\n\n");
}


int main() {
    test_small_varlena();
    test_big_varlena();
	test_multi_varlena_buffer();
    printf("All varlena tests passed!\n");
    return 0;
}
