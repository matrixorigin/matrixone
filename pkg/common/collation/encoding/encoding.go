// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package encoding implements bounded byte conversions, not SQL admission or
// collation comparison. Callers resolve compatibility aliases before entry.
package encoding

import (
	"context"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// Policy identifies the semantic boundary. Identity preserves bytes, even if
// they are outside the declared repertoire; Validate is a separate operation.
type Policy uint8

const (
	Identity Policy = iota
	Connection
	Result
	ConvertUsing
)

// Check slightly before 4096 so a four-byte rune cannot cross that bound.
const cancelInterval = 4096 - utf8.UTFMax

func convertible(charset collation.Charset) bool {
	return charset == collation.CharsetBinary || charset == collation.CharsetASCII || charset == collation.CharsetUTF8MB4
}

// Validate checks repertoire without allocating payload memory. Strict MB3 is
// available only here; public utf8/utf8mb3 aliases must still resolve to MB4.
func Validate(ctx context.Context, charset collation.Charset, input []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if charset == collation.CharsetBinary {
		return nil
	}
	if !convertible(charset) && charset != collation.CharsetUTF8MB3 {
		return moerr.NewInvalidInput(ctx, "unsupported encoding")
	}
	if charset == collation.CharsetUTF8MB4 {
		// Preserve utf8.Valid's optimized ASCII path, but bound each scan and
		// keep complete runes together when a valid sequence crosses a chunk.
		for offset := 0; offset < len(input); {
			end := offset + min(4096, len(input)-offset)
			if end < len(input) {
				for n := 0; n < utf8.UTFMax-1 && !utf8.RuneStart(input[end]); n++ {
					end--
				}
			}
			if !utf8.Valid(input[offset:end]) {
				return moerr.NewInvalidInput(ctx, "invalid utf8 encoding")
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			offset = end
		}
		return ctx.Err()
	}
	lastCheck := 0
	for offset := 0; offset < len(input); {
		if offset-lastCheck >= cancelInterval {
			if err := ctx.Err(); err != nil {
				return err
			}
			lastCheck = offset
		}
		if charset == collation.CharsetASCII {
			if input[offset] > 0x7f {
				return moerr.NewInvalidInput(ctx, "invalid ascii encoding")
			}
			offset++
			continue
		}
		r, size := utf8.DecodeRune(input[offset:])
		if (r == utf8.RuneError && size == 1) || (charset == collation.CharsetUTF8MB3 && size == 4) {
			return moerr.NewInvalidInput(ctx, "invalid utf8 encoding")
		}
		offset += size
	}
	return ctx.Err()
}

// Convert never modifies input. owned=false borrows input (possibly a prefix);
// owned=true transfers one pool allocation, which the caller must Free with the
// same pool after copying into its destination. NULL and failure never transfer
// an allocation. limit bounds output bytes, including borrowed output.
//
// Binary destinations and same-encoding connection/result paths preserve bytes.
// CONVERT from binary to UTF8MB4 yields NULL on invalid UTF-8; to ASCII it
// replaces each high-bit byte with '?'. Other cross-encoding paths replace
// unrepresentable or malformed characters, stopping at an incomplete source sequence, as MySQL
// does. Strict MB3 conversion and GBK are deliberately not activated here.
func Convert(ctx context.Context, pool *mpool.MPool, src, dst collation.Charset, policy Policy, input []byte, limit int64) (output []byte, owned bool, isNull bool, err error) {
	if err = ctx.Err(); err != nil {
		return nil, false, false, err
	}
	if !convertible(src) || !convertible(dst) || policy > ConvertUsing || (policy == Identity && src != dst) {
		return nil, false, false, moerr.NewInvalidInput(ctx, "unsupported encoding conversion")
	}
	if limit < 0 {
		return nil, false, false, moerr.NewInvalidInput(ctx, "negative encoding output limit")
	}
	if limit > mpool.MaxAllocationSize() {
		limit = mpool.MaxAllocationSize()
	}
	if policy == ConvertUsing && src == collation.CharsetBinary && dst == collation.CharsetUTF8MB4 {
		if err = Validate(ctx, dst, input); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrInvalidInput) {
				return nil, false, true, nil
			}
			return nil, false, false, err
		}
	}
	binaryASCIIConvert := policy == ConvertUsing && src == collation.CharsetBinary && dst == collation.CharsetASCII
	if !binaryASCIIConvert && (src == dst || src == collation.CharsetBinary || dst == collation.CharsetBinary) {
		if int64(len(input)) > limit {
			return nil, false, false, moerr.NewInvalidInput(ctx, "encoding output exceeds limit")
		}
		if err = ctx.Err(); err != nil {
			return nil, false, false, err
		}
		return input, false, false, nil
	}

	// The first pass computes the exact size and whether a prefix can be borrowed.
	size, prefix, err := transcode(ctx, src, dst, input, nil)
	if err != nil {
		return nil, false, false, err
	}
	if int64(size) > limit {
		return nil, false, false, moerr.NewInvalidInput(ctx, "encoding output exceeds limit")
	}
	if err = ctx.Err(); err != nil {
		return nil, false, false, err
	}
	if prefix {
		return input[:size], false, false, nil
	}
	if pool == nil {
		return nil, false, false, moerr.NewInvalidInput(ctx, "encoding conversion requires a memory pool")
	}
	output, err = pool.Alloc(size, true)
	if err != nil {
		return nil, false, false, err
	}
	if _, _, err = transcode(ctx, src, dst, input, output); err != nil {
		pool.Free(output)
		return nil, false, false, err
	}
	if err = ctx.Err(); err != nil {
		pool.Free(output)
		return nil, false, false, err
	}
	return output, true, false, nil
}

// Both passes consume at most len(input) bytes and produce no more than that.
// MySQL checks the expected UTF-8 width before checking continuation validity:
// e2 41 is incomplete (stop), whereas e2 41 42 is malformed (replace e2).
func transcode(ctx context.Context, src, dst collation.Charset, input, output []byte) (size int, prefix bool, err error) {
	prefix = true
	lastCheck := 0
	for offset := 0; offset < len(input); {
		if offset-lastCheck >= cancelInterval {
			if err = ctx.Err(); err != nil {
				return 0, false, err
			}
			lastCheck = offset
		}
		r, width := rune(input[offset]), 1
		replace := false
		if src == collation.CharsetASCII || src == collation.CharsetBinary {
			// Binary-to-ASCII CONVERT treats each byte as one source unit,
			// including bytes that together form a valid UTF-8 character.
			replace = r > 0x7f
		} else {
			expected := 1
			switch {
			case input[offset] >= 0xf0:
				expected = 4
			case input[offset] >= 0xe0:
				expected = 3
			case input[offset] >= 0xc0:
				expected = 2
			}
			if len(input)-offset < expected {
				break
			}
			r, width = utf8.DecodeRune(input[offset:])
			replace = r == utf8.RuneError && width == 1
		}
		replace = replace || (dst == collation.CharsetASCII && r > 0x7f)
		if replace {
			prefix = false
			if output != nil {
				output[size] = '?'
			}
			size++
		} else {
			if output != nil {
				copy(output[size:], input[offset:offset+width])
			}
			size += width
		}
		offset += width
	}
	return size, prefix, ctx.Err()
}
