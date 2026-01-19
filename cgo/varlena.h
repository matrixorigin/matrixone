#ifndef _CGO_VARLENA_H_
#define _CGO_VARLENA_H_

#include <stdint.h>
#include <stdbool.h>
#include <string.h>

#define VARLENA_INLINE_SIZE 23
#define VARLENA_SIZE 24
#define VARLENA_BIG_HDR 0xffffffff

static inline bool varlena_is_small(const uint8_t *v) {
    // For small varlena, the first byte stores the length.
    return (uint32_t) v[0] <= VARLENA_INLINE_SIZE;
}

static inline void varlena_get_big_offset_len(const uint8_t *v, uint32_t *offset, uint32_t *length, uint32_t *next_offset) {
    const uint32_t *p = (const uint32_t*)v;
    // p[0] = VARLENA_BIG_HDR
    *offset = p[1];
    *length = p[2];
    *next_offset = VARLENA_SIZE;
}

static inline void varlena_set_big_offset_len(uint8_t *v, uint32_t offset, uint32_t length) {
    uint32_t *p = (uint32_t*)v;
    p[0] = VARLENA_BIG_HDR;
    p[1] = offset;
    p[2] = length;
}

static inline const uint8_t* varlena_get_byte_slice(const uint8_t *v, const uint8_t *area, uint32_t *len, uint32_t *next_offset) {
    if (varlena_is_small(v)) {
        *len = (uint32_t) v[0];
        *next_offset = VARLENA_SIZE;
        return v+1;
    } else {
        uint32_t offset, length;
        varlena_get_big_offset_len(v, &offset, &length, next_offset);
        *len = length;
        return area + offset;
    }
}

#endif // _CGO_VARLENA_H_
