#ifndef _CGO_VARLENA_H_
#define _CGO_VARLENA_H_

/*
 * This header defines the C-compatible interface for the Varlena data type.
 * It is designed to be fully compatible and interoperable with the Go Varlena
 * type defined in 'pkg/container/types'. Both implementations share the same
 * memory layout and constants for handling variable-length data, whether
 * stored inline or referencing an external data area.
 *
 * Compatibility Analysis:
 *
 * 1. Constants:
 *  The constants defined in both files are identical:
 *   - VarlenaInlineSize / VARLENA_INLINE_SIZE = 23
 *   - VarlenaSize / VARLENA_SIZE = 24
 *   - VarlenaBigHdr / VARLENA_BIG_HDR = 0xffffffff
 *
 * 2. Data Structure:
 *  - The Go Varlena type is defined as [24]byte, which is consistent with the VARLENA_SIZE constant.
 *  - The C functions operate on a uint8_t *, which is a pointer to a byte array, matching how Go's Varlena is handled in memory.
 *
 * 3. Memory Layout:
 *  The memory layout for both small and large Varlena values is consistent between the C and Go implementations.
 *
 *  * Small Varlena (length <= 23):
 *      * Go & C: The first byte (v[0]) stores the length, and the following bytes store the data.
 *
 *  * Large Varlena (length > 23):
 *      * Go & C: The Varlena is treated as an array of uint32_t.
 *          * bytes[0:4] (or p[0]) is the header (0xffffffff).
 *          * bytes[4:8] (or p[1]) is the offset into the data area.
 *          * bytes[8:12] (or p[2]) is the length of the data.
 *
 * 4. Functionality:
 *  The inline C functions mirror the logic of the Go methods for Varlena:
 *   - varlena_is_small is equivalent to (v *Varlena) IsSmall().
 *   - varlena_get_big_offset_len is equivalent to (v *Varlena) OffsetLen().
 *   - varlena_set_big_offset_len is equivalent to (v *Varlena) SetOffsetLen().
 *   - varlena_get_byte_slice is equivalent to (v *Varlena) GetByteSlice().
 */

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

static inline void varlena_get_big_offset_len(const uint8_t *v, uint32_t *offset, uint32_t *length) {
    const uint32_t *p = (const uint32_t*)v;
    // p[0] = VARLENA_BIG_HDR
    *offset = p[1];
    *length = p[2];
}

static inline void varlena_set_big_offset_len(uint8_t *v, uint32_t offset, uint32_t length) {
    uint32_t *p = (uint32_t*)v;
    p[0] = VARLENA_BIG_HDR;
    p[1] = offset;
    p[2] = length;
}

static inline uint32_t varlena_get_len(const uint8_t *v) {
    if (varlena_is_small(v)) {
        return (uint32_t)v[0];
    } else {
        const uint32_t *p = (const uint32_t*)v;
        return p[2];
    }
}

static inline const uint8_t* varlena_get_byte_slice(const uint8_t *v, const uint8_t *area, uint32_t *len) {
    if (varlena_is_small(v)) {
        *len = (uint32_t) v[0];
        return v+1;
    } else {
        uint32_t offset, length;
        varlena_get_big_offset_len(v, &offset, &length);
        *len = length;
        return area + offset;
    }
}

#endif // _CGO_VARLENA_H_
