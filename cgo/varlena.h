#ifndef _CGO_VARLENA_H_
#define _CGO_VARLENA_H_

#include <stdint.h>
#include <stdbool.h>
#include <string.h>

#define VARLENA_INLINE_SIZE 23
#define VARLENA_SIZE 24
#define VARLENA_BIG_HDR 0xffffffff

typedef uint8_t * varlena_t;

static inline bool varlena_is_small(const varlena_t v) {
    // For small varlena, the first byte stores the length.
    return (uint32_t) v[0] <= VARLENA_INLINE_SIZE;
}

void varlena_get_offset_len(const varlena_t v, uint32_t *offset, uint32_t *length) {
    uint32_t *p = (uint32_t*)v;
    *offset = p[0];
    *length = p[1];
}

const uint8_t* varlena_get_byte_slice(const varlena_t v, const uint8_t *area, uint32_t *len) {
    if (varlena_is_small(v)) {
        *len = (uint32_t) v[0];
        return v+1;
    } else {
        uint32_t offset, length;
        varlena_get_offset_len(v, &offset, &length);
        *len = length;
        return area + offset;
    }
}

#endif // _CGO_VARLENA_H_
