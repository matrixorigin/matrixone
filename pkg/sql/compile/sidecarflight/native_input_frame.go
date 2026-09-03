// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sidecarflight

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// fixedNativeFrameWriter exposes an already-admitted payload region to the
// canonical Batch encoder. It can never grow or replace that region.
type fixedNativeFrameWriter struct {
	data   []byte
	offset int
}

func (w *fixedNativeFrameWriter) Write(value []byte) (int, error) {
	if w == nil || len(value) > len(w.data)-w.offset {
		return 0, io.ErrShortWrite
	}
	copy(w.data[w.offset:], value)
	w.offset += len(value)
	return len(value), nil
}

func (w *fixedNativeFrameWriter) WriteString(value string) (int, error) {
	if w == nil || len(value) > len(w.data)-w.offset {
		return 0, io.ErrShortWrite
	}
	copy(w.data[w.offset:], value)
	w.offset += len(value)
	return len(value), nil
}

func (w *fixedNativeFrameWriter) Len() int {
	if w == nil {
		return 0
	}
	return w.offset
}

func (w *fixedNativeFrameWriter) EnsureCapacity(required int) error {
	if w == nil || required < 0 || required > len(w.data) {
		return io.ErrShortWrite
	}
	return nil
}

func (w *fixedNativeFrameWriter) appendSpace(size int) ([]byte, error) {
	if w == nil || size < 0 || size > len(w.data)-w.offset {
		return nil, io.ErrShortWrite
	}
	start := w.offset
	w.offset += size
	return w.data[start:w.offset], nil
}

func (w *fixedNativeFrameWriter) WriteByte(value byte) error {
	dst, err := w.appendSpace(1)
	if err != nil {
		return err
	}
	dst[0] = value
	return nil
}

func (w *fixedNativeFrameWriter) WriteUint32(value uint32) error {
	dst, err := w.appendSpace(4)
	if err != nil {
		return err
	}
	binary.NativeEndian.PutUint32(dst, value)
	return nil
}

func (w *fixedNativeFrameWriter) WriteInt32(value int32) error {
	return w.WriteUint32(uint32(value))
}

func (w *fixedNativeFrameWriter) WriteUint64(value uint64) error {
	dst, err := w.appendSpace(8)
	if err != nil {
		return err
	}
	binary.NativeEndian.PutUint64(dst, value)
	return nil
}

func (w *fixedNativeFrameWriter) WriteInt64(value int64) error {
	return w.WriteUint64(uint64(value))
}

// marshalNativeInputFrame allocates the final header-plus-payload frame from
// the query pool and writes the canonical Batch encoding directly into it. On
// success the caller owns frame and must release it through mp.
func marshalNativeInputFrame(
	sequence uint64,
	bat *batch.Batch,
	payloadBytes int,
	mp *mpool.MPool,
) (frame []byte, err error) {
	if sequence == 0 || bat == nil || mp == nil || payloadBytes <= 0 ||
		payloadBytes > math.MaxInt-nativeBatchFrameHeaderBytes {
		return nil, internalErrorf("sidecar flight: invalid native input frame allocation")
	}
	frame, err = mp.Alloc(nativeBatchFrameHeaderBytes+payloadBytes, true)
	if err != nil {
		return nil, internalErrorf("sidecar flight: allocate native input frame: %w", err)
	}
	allocated := frame
	defer func() {
		if err != nil {
			mp.Free(allocated)
			frame = nil
		}
	}()

	clear(frame[:nativeBatchFrameHeaderBytes])
	copy(frame[:4], "MOB1")
	binary.LittleEndian.PutUint16(frame[4:6], 1)
	binary.LittleEndian.PutUint64(frame[8:16], sequence)
	binary.LittleEndian.PutUint64(frame[16:24], uint64(payloadBytes))
	w := fixedNativeFrameWriter{data: frame[nativeBatchFrameHeaderBytes:]}
	if err = bat.MarshalBinaryTo(&w); err != nil {
		return nil, internalErrorf("sidecar flight: marshal native input batch: %w", err)
	}
	if w.Len() != payloadBytes {
		return nil, internalErrorf(
			"sidecar flight: native input batch size changed during marshal: planned=%d actual=%d",
			payloadBytes, w.Len(),
		)
	}
	return frame, nil
}

type nativeWindowPlan struct {
	end          int
	payloadBytes int
}

const nativeVectorMarshalFixedBytes = 1 + types.TSize + 4 + 4 + 4 + 4 + 1

// planNativeWindow returns the largest compact clone of source[start:] whose
// canonical encoding fits limit. It visits each candidate row once and retains
// only per-column null state; no vector data or row-sized planning index is
// materialized.
func planNativeWindow(source *batch.Batch, start int, limit uint64) (nativeWindowPlan, error) {
	if source == nil || start < 0 || start >= source.RowCount() || limit == 0 {
		return nativeWindowPlan{}, internalErrorf("sidecar flight: invalid native input split window")
	}
	total, err := nativeWindowBaseSize(source)
	if err != nil {
		return nativeWindowPlan{}, err
	}
	hasNull := make([]bool, len(source.Vecs))
	plan := nativeWindowPlan{end: start, payloadBytes: total}
	for row := start; row < source.RowCount(); row++ {
		rows := row - start + 1
		delta := 0
		for column, vec := range source.Vecs {
			if vec.IsConst() {
				continue
			}
			typeSize := vec.GetType().TypeSize()
			if typeSize < 0 {
				return nativeWindowPlan{}, internalErrorf("sidecar flight: native input vector has invalid type size")
			}
			if err = addNativeWindowSize(&delta, typeSize); err != nil {
				return nativeWindowPlan{}, err
			}
			if vec.GetType().IsVarlen() && !vec.IsNull(uint64(row)) {
				value := vec.GetBytesAt(row)
				if len(value) > types.VarlenaInlineSize {
					if err = addNativeWindowSize(&delta, len(value)); err != nil {
						return nativeWindowPlan{}, err
					}
				}
			}
			if hasNull[column] {
				if nativeBitmapWords(rows) != nativeBitmapWords(rows-1) {
					if err = addNativeWindowSize(&delta, 8); err != nil {
						return nativeWindowPlan{}, err
					}
				}
			} else if vec.IsNull(uint64(row)) {
				if err = addNativeWindowSize(
					&delta,
					bitmap.MarshalHeaderSize+8*nativeBitmapWords(rows),
				); err != nil {
					return nativeWindowPlan{}, err
				}
			}
		}
		if delta > math.MaxInt-total || uint64(total+delta) > limit {
			break
		}
		total += delta
		for column, vec := range source.Vecs {
			if !vec.IsConst() && vec.IsNull(uint64(row)) {
				hasNull[column] = true
			}
		}
		plan.end = row + 1
		plan.payloadBytes = total
	}
	if plan.end == start {
		return nativeWindowPlan{}, internalErrorf("sidecar flight: one native input row exceeds the negotiated limit")
	}
	return plan, nil
}

func nativeWindowBaseSize(source *batch.Batch) (int, error) {
	// Batch.MarshalBinary's fixed fields are row count, vector count, attribute
	// count, extra-buffer length, recursive marker, and shuffle marker.
	total := 8 + 4 + 4 + 4 + 4 + 4
	for _, vec := range source.Vecs {
		if vec == nil {
			return 0, internalErrorf("sidecar flight: native input contains a nil vector")
		}
		if err := addNativeWindowSize(&total, 4+nativeVectorMarshalFixedBytes); err != nil {
			return 0, err
		}
		if !vec.IsConst() {
			continue
		}
		if !vec.IsConstNull() {
			typeSize := vec.GetType().TypeSize()
			if typeSize < 0 {
				return 0, internalErrorf("sidecar flight: native input vector has invalid type size")
			}
			if err := addNativeWindowSize(&total, typeSize); err != nil {
				return 0, err
			}
			if vec.GetType().IsVarlen() {
				value := vec.GetBytesAt(0)
				if len(value) > types.VarlenaInlineSize {
					if err := addNativeWindowSize(&total, len(value)); err != nil {
						return 0, err
					}
				}
			}
		}
		// cloneNativeWindow clones a constant's physical row zero, then restores
		// the logical window length. Preserve that exact bitmap encoding.
		if vec.Length() > 0 && vec.GetNulls().GetBitmap().CountRange(0, 1) != 0 {
			if err := addNativeWindowSize(&total, bitmap.MarshalHeaderSize+8); err != nil {
				return 0, err
			}
		}
	}
	for _, attr := range source.Attrs {
		if err := addNativeWindowSize(&total, 4+len(attr)); err != nil {
			return 0, err
		}
	}
	return total, nil
}

func nativeBitmapWords(rows int) int {
	if rows <= 0 {
		return 0
	}
	return 1 + (rows-1)/64
}

func addNativeWindowSize(total *int, value int) error {
	if total == nil || value < 0 || *total > math.MaxInt-value {
		return internalErrorf("sidecar flight: native input split size overflows")
	}
	*total += value
	return nil
}
