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

static inline void varlena_get_offset_len(const uint8_t *v, uint32_t *offset, uint32_t *length) {
    const uint32_t *p = (const uint32_t*)v;
    *offset = p[0];
    *length = p[1];
}

static inline const uint8_t* varlena_get_byte_slice(const uint8_t *v, const uint8_t *area, uint32_t *len, uint32_t *next_offset) {
    if (varlena_is_small(v)) {
        *len = (uint32_t) v[0];
        *next_offset = *len+1;
        return v+1;
    } else {
        uint32_t offset, length;
        varlena_get_offset_len(v, &offset, &length);
        *len = length;
        *next_offset = sizeof(uint32_t)*2;
        return area + offset;
    }
}

#endif // _CGO_VARLENA_H_
