// Copyright 2021 Matrix Origin
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

package batch

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func New(attrs []string) *Batch {
	return &Batch{
		Attrs:    attrs,
		Vecs:     make([]*vector.Vector, len(attrs)),
		rowCount: 0,
	}
}

func NewOffHeap(attrs []string) *Batch {
	ret := New(attrs)
	ret.offHeap = true
	return ret
}

func NewOffHeapEmpty() *Batch {
	return &Batch{
		offHeap: true,
	}
}

func NewWithSize(n int) *Batch {
	return &Batch{
		Vecs:     make([]*vector.Vector, n),
		rowCount: 0,
	}
}

func NewOffHeapWithSize(n int) *Batch {
	ret := NewWithSize(n)
	ret.offHeap = true
	return ret
}

func NewWithSchema(offHeap bool, attrs []string, attTypes []types.Type) *Batch {
	var bat *Batch
	if offHeap {
		bat = NewOffHeapWithSize(len(attTypes))
	} else {
		bat = NewWithSize(len(attTypes))
	}
	bat.Attrs = attrs
	for i, t := range attTypes {
		if offHeap {
			bat.Vecs[i] = vector.NewOffHeapVecWithType(t)
		} else {
			bat.Vecs[i] = vector.NewVec(t)
		}
	}
	return bat
}

func EmptyBatchWithAttrs(attrs []string) Batch {
	bat := Batch{
		Attrs: attrs,
		Vecs:  make([]*vector.Vector, len(attrs)),
	}
	for i := range attrs {
		bat.Vecs[i] = vector.NewVec(types.T_any.ToType())
	}

	return bat
}

func SetLength(bat *Batch, n int) {
	for _, vec := range bat.Vecs {
		vec.SetLength(n)
	}
	bat.rowCount = n
}

func (bat *Batch) CheckLength() error {
	for _, vec := range bat.Vecs {
		if vec.Length() != bat.rowCount {
			return moerr.NewInternalErrorNoCtx("vec.Length() != bat.rowCount")
		}
	}
	return nil
}

func (bat *Batch) MarshalBinary() ([]byte, error) {
	var w bytes.Buffer
	return bat.MarshalBinaryWithBuffer(&w, false)
}

func (bat *Batch) MarshalBinaryWithBuffer(w *bytes.Buffer, reset bool) ([]byte, error) {
	if reset {
		w.Reset()
	}
	if err := bat.MarshalBinaryTo(w); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (bat *Batch) MarshalBinarySize() (int, error) {
	return bat.prepareMarshalBinary(nil)
}

func (bat *Batch) prepareMarshalBinary(
	plans []vector.MarshalBinaryPlan,
) (int, error) {
	if bat == nil {
		return 0, moerr.NewInvalidInputNoCtx("invalid batch for marshal")
	}
	const fixedSize = uint64(8 + 4 + 4 + 4 + 4 + 4)
	total := fixedSize
	add := func(value uint64) bool {
		if value > uint64(^uint(0)>>1)-total {
			return false
		}
		total += value
		return true
	}
	if uint64(len(bat.Vecs)) > uint64(^uint32(0)>>1) ||
		uint64(len(bat.Attrs)) > uint64(^uint32(0)>>1) ||
		uint64(len(bat.ExtraBuf)) > uint64(^uint32(0)>>1) {
		return 0, moerr.NewInvalidInputNoCtx(
			"batch field exceeds marshal format",
		)
	}
	if plans != nil && len(plans) < len(bat.Vecs) {
		return 0, moerr.NewInvalidInputNoCtx("short batch marshal plan")
	}
	for i, vec := range bat.Vecs {
		if vec == nil {
			return 0, moerr.NewInvalidInputNoCtx(
				"cannot marshal a nil batch vector",
			)
		}
		plan, err := vec.PrepareMarshalBinary()
		if err != nil {
			return 0, err
		}
		size := plan.Size()
		if uint64(size) > uint64(^uint32(0)) ||
			!add(4+uint64(size)) {
			return 0, moerr.NewInvalidInputNoCtx(
				"batch vector exceeds marshal format",
			)
		}
		if plans != nil {
			plans[i] = plan
		}
	}
	for _, attr := range bat.Attrs {
		if uint64(len(attr)) > uint64(^uint32(0)>>1) ||
			!add(4+uint64(len(attr))) {
			return 0, moerr.NewInvalidInputNoCtx(
				"batch attribute exceeds marshal format",
			)
		}
	}
	if !add(uint64(len(bat.ExtraBuf))) {
		return 0, moerr.NewInvalidInputNoCtx(
			"batch marshal size exceeds platform limit",
		)
	}
	return int(total), nil
}

func (bat *Batch) MarshalBinaryTo(w io.Writer) error {
	if bat == nil || w == nil {
		return io.ErrClosedPipe
	}
	var inlinePlans [64]vector.MarshalBinaryPlan
	var plans []vector.MarshalBinaryPlan
	if len(bat.Vecs) <= len(inlinePlans) {
		plans = inlinePlans[:len(bat.Vecs)]
	} else {
		plans = make([]vector.MarshalBinaryPlan, len(bat.Vecs))
	}
	size, err := bat.prepareMarshalBinary(plans)
	if err != nil {
		return err
	}
	if sized, ok := w.(interface {
		Len() int
		EnsureCapacity(int) error
	}); ok {
		if sized.Len() > math.MaxInt-size {
			return moerr.NewInvalidInputNoCtx("batch marshal size exceeds platform limit")
		}
		if err := sized.EnsureCapacity(sized.Len() + size); err != nil {
			return err
		}
	}
	if err := writeBatchMarshalInt64(w, int64(bat.rowCount)); err != nil {
		return err
	}

	l := int32(len(bat.Vecs))
	if err := writeBatchMarshalInt32(w, l); err != nil {
		return err
	}
	for i := 0; i < int(l); i++ {
		if err := writeBatchMarshalUint32(w, uint32(plans[i].Size())); err != nil {
			return err
		}
		if err := plans[i].MarshalTo(w); err != nil {
			return err
		}
	}

	l = int32(len(bat.Attrs))
	if err := writeBatchMarshalInt32(w, l); err != nil {
		return err
	}
	for i := 0; i < int(l); i++ {
		size := int32(len(bat.Attrs[i]))
		if err := writeBatchMarshalInt32(w, size); err != nil {
			return err
		}
		n, err := io.WriteString(w, bat.Attrs[i])
		if err != nil {
			return err
		}
		if int32(n) != size {
			return io.ErrShortWrite
		}
	}

	extraSize := int32(len(bat.ExtraBuf))
	if err := writeBatchMarshalInt32(w, extraSize); err != nil {
		return err
	}
	if err := writeBatchMarshalBytes(w, bat.ExtraBuf); err != nil {
		return err
	}

	if err := writeBatchMarshalInt32(w, bat.Recursive); err != nil {
		return err
	}
	return writeBatchMarshalInt32(w, bat.ShuffleIDX)
}

type batchPrimitiveWriter interface {
	WriteUint32(uint32) error
	WriteInt32(int32) error
	WriteInt64(int64) error
}

func writeBatchMarshalUint32(w io.Writer, value uint32) error {
	if typed, ok := w.(batchPrimitiveWriter); ok {
		return typed.WriteUint32(value)
	}
	var data [4]byte
	binary.NativeEndian.PutUint32(data[:], value)
	return writeBatchMarshalBytes(w, data[:])
}

func writeBatchMarshalInt32(w io.Writer, value int32) error {
	if typed, ok := w.(batchPrimitiveWriter); ok {
		return typed.WriteInt32(value)
	}
	var data [4]byte
	binary.NativeEndian.PutUint32(data[:], uint32(value))
	return writeBatchMarshalBytes(w, data[:])
}

func writeBatchMarshalInt64(w io.Writer, value int64) error {
	if typed, ok := w.(batchPrimitiveWriter); ok {
		return typed.WriteInt64(value)
	}
	var data [8]byte
	binary.NativeEndian.PutUint64(data[:], uint64(value))
	return writeBatchMarshalBytes(w, data[:])
}

func writeBatchMarshalBytes(w io.Writer, value []byte) error {
	written, err := w.Write(value)
	if err != nil {
		return err
	}
	if written != len(value) {
		return io.ErrShortWrite
	}
	return nil
}

func (bat *Batch) UnmarshalBinary(data []byte) (err error) {
	return bat.UnmarshalBinaryWithAnyMp(data, nil)
}

type batchUnmarshalCursor struct {
	data []byte
}

func (c *batchUnmarshalCursor) read(size int) ([]byte, error) {
	if size < 0 || size > len(c.data) {
		return nil, io.ErrUnexpectedEOF
	}
	data := c.data[:size]
	c.data = c.data[size:]
	return data, nil
}

func (c *batchUnmarshalCursor) readInt32() (int32, error) {
	data, err := c.read(4)
	if err != nil {
		return 0, err
	}
	return types.DecodeInt32(data), nil
}

func (c *batchUnmarshalCursor) readInt64() (int64, error) {
	data, err := c.read(8)
	if err != nil {
		return 0, err
	}
	return types.DecodeInt64(data), nil
}

func (c *batchUnmarshalCursor) readUint32() (uint32, error) {
	data, err := c.read(4)
	if err != nil {
		return 0, err
	}
	return types.DecodeUint32(data), nil
}

func (bat *Batch) UnmarshalBinaryWithAnyMp(data []byte, mp *mpool.MPool) (err error) {
	allocationAccount := bat.allocationAccount
	cursor := batchUnmarshalCursor{data: data}
	rowCount, err := cursor.readInt64()
	if err != nil {
		return err
	}
	if rowCount < 0 || int64(int(rowCount)) != rowCount {
		return moerr.NewInvalidInputNoCtx("invalid batch row count")
	}
	decodedRowCount := int(rowCount)

	l, err := cursor.readInt32()
	if err != nil {
		return err
	}
	if l < 0 || int64(l) > int64(len(cursor.data)/4) {
		return moerr.NewInvalidInputNoCtx("invalid batch vector count")
	}
	// Fix for bug #23156: Handle Vecs length changes (from d4b79f12) while maintaining revert version's firstTime logic
	firstTime := bat.Vecs == nil
	vecsLen := int(l)
	vecsLenChanged := !firstTime && vecsLen != len(bat.Vecs)

	// CRITICAL FIX: When batch is reused (not firstTime), always reallocate Vecs if length changed
	// This ensures Vecs are properly reset and prevents stale data from previous unmarshal operations
	if firstTime || vecsLenChanged {
		if vecsLenChanged && len(bat.Vecs) > 0 {
			if mp == nil {
				for _, vec := range bat.Vecs {
					if vec != nil && !vec.NeedDup() {
						return moerr.NewInvalidInputNoCtx("cannot unmarshal into an owned batch vector without a memory pool")
					}
				}
			}
			bat.Clean(mp)
			bat.allocationAccount = allocationAccount
		}
		bat.Vecs = make([]*vector.Vector, vecsLen)
	}

	vecs := bat.Vecs
	// SelectColumns and ReplaceVector can leave multiple slots pointing to the
	// same Vector. Reuse each receiver for at most one decoded column.
	// Most batches are narrow, so avoid allocating a map on every decode. The
	// prefix scan is bounded; wide batches keep the linear-time map path.
	const linearReceiverScanLimit = 64
	var usedReceivers map[*vector.Vector]struct{}
	for i := 0; i < vecsLen; i++ {
		size, err := cursor.readUint32()
		if err != nil {
			return err
		}
		if uint64(size) > uint64(len(cursor.data)) {
			return io.ErrUnexpectedEOF
		}
		vecData, err := cursor.read(int(size))
		if err != nil {
			return err
		}
		if vecs[i] != nil {
			used := false
			if vecsLen <= linearReceiverScanLimit {
				for j := 0; j < i; j++ {
					if vecs[j] == vecs[i] {
						used = true
						break
					}
				}
			} else {
				_, used = usedReceivers[vecs[i]]
			}
			if used {
				vecs[i] = nil
			} else if vecsLen > linearReceiverScanLimit {
				if usedReceivers == nil {
					usedReceivers = make(map[*vector.Vector]struct{}, vecsLen)
				}
				usedReceivers[vecs[i]] = struct{}{}
			}
		}
		if vecs[i] == nil {
			if bat.offHeap {
				vecs[i] = vector.NewOffHeapVec()
			} else {
				vecs[i] = vector.NewVecFromReuse()
			}
		} else if vecs[i].Allocated() > 0 || vecs[i].NeedDup() {
			if mp == nil && !vecs[i].NeedDup() {
				return moerr.NewInvalidInputNoCtx("cannot unmarshal into an owned batch vector without a memory pool")
			}
			vecs[i].Free(mp)
		}
		// UnmarshalBinary installs aliases into vecData. An empty accounted
		// receiver must explicitly drop its future-allocation selection first;
		// the Batch retains the destination context for a later owned copy.
		if vecs[i].AllocationAccountSelection() != nil {
			if err := vecs[i].SetAllocationAccount(nil); err != nil {
				return err
			}
		}
		if err := vecs[i].UnmarshalBinary(vecData); err != nil {
			return err
		}
	}

	l, err = cursor.readInt32()
	if err != nil {
		return err
	}
	if l < 0 || int64(l) > int64(len(cursor.data)/4) {
		return moerr.NewInvalidInputNoCtx("invalid batch attribute count")
	}
	// Fix for bug #23156: Attrs length MUST always match Vecs length
	// Vecs length (vecsLen) is authoritative - it's already allocated and deserialized
	// If serialized Attrs length differs from Vecs length, we use Vecs length as the source of truth
	// This handles cases where serialized data has inconsistent lengths (which can occur in practice)
	serializedAttrsLen := int(l)
	// CRITICAL FIX: Always reallocate Attrs to ensure clean state for batch reuse
	// This prevents stale Attrs values from previous unmarshal operations
	// Special case: if vecsLen == 0 but serializedAttrsLen > 0, use serializedAttrsLen
	// This handles cases where Vecs are empty but Attrs are preserved (e.g., in tests)
	attrsLen := vecsLen
	if vecsLen == 0 && serializedAttrsLen > 0 {
		attrsLen = serializedAttrsLen
	}
	if attrsLen != len(bat.Attrs) {
		if attrsLen == 0 {
			// If attrsLen is 0, keep Attrs as nil (not empty array) for consistency
			bat.Attrs = nil
		} else {
			bat.Attrs = make([]string, attrsLen)
		}
	} else if !firstTime {
		// When batch is reused and lengths match, still clear Attrs to prevent stale values
		// This is critical for UPDATE operations where batch is reused multiple times
		// Performance note: This is O(n) where n is typically small (dozens of columns)
		// The cost is acceptable compared to data corruption issues
		for i := range bat.Attrs {
			bat.Attrs[i] = ""
		}
	}

	// Read serialized Attrs, but only up to min(serializedAttrsLen, attrsLen)
	// If serialized length > attrsLen: ignore excess (data inconsistency, attrsLen is authoritative)
	// If serialized length < attrsLen: read what's available (remaining will be empty strings, should not happen normally)
	attrsToRead := serializedAttrsLen
	if attrsToRead > attrsLen {
		attrsToRead = attrsLen
	}
	for i := 0; i < attrsToRead; i++ {
		size, err := cursor.readInt32()
		if err != nil {
			return err
		}
		if size < 0 {
			return moerr.NewInvalidInputNoCtx("invalid batch attribute size")
		}
		attrData, err := cursor.read(int(size))
		if err != nil {
			return err
		}
		bat.Attrs[i] = string(attrData)
	}
	// CRITICAL FIX: Clear remaining Attrs to prevent stale values when serializedAttrsLen < attrsLen
	// This is essential for batch reuse scenarios (e.g., UPDATE operations with IVF index)
	for i := attrsToRead; i < attrsLen; i++ {
		bat.Attrs[i] = ""
	}
	// If serialized Attrs length > vecsLen, skip the excess data
	for i := attrsToRead; i < serializedAttrsLen; i++ {
		size, err := cursor.readInt32()
		if err != nil {
			return err
		}
		if size < 0 {
			return moerr.NewInvalidInputNoCtx("invalid batch attribute size")
		}
		if _, err := cursor.read(int(size)); err != nil {
			return err
		}
	}

	// ExtraBuf
	l, err = cursor.readInt32()
	if err != nil {
		return err
	}
	if l < 0 {
		return moerr.NewInvalidInputNoCtx("invalid batch extra buffer size")
	}
	extraBuf, err := cursor.read(int(l))
	if err != nil {
		return err
	}
	bat.ExtraBuf = nil
	bat.ExtraBuf = append(bat.ExtraBuf, extraBuf...)

	bat.Recursive, err = cursor.readInt32()
	if err != nil {
		return err
	}
	bat.ShuffleIDX, err = cursor.readInt32()
	if err != nil {
		return err
	}
	bat.rowCount = decodedRowCount
	return nil
}

func (bat *Batch) UnmarshalFromReader(r io.Reader, mp *mpool.MPool) (err error) {
	return bat.unmarshalFromReader(r, mp, true)
}

func (bat *Batch) unmarshalFromReader(
	r io.Reader,
	mp *mpool.MPool,
	allowMetadata bool,
) (err error) {
	if bat == nil || r == nil {
		return io.ErrClosedPipe
	}
	i64, err := types.ReadInt64(r)
	if err != nil {
		return err
	}
	if i64 < 0 || int64(int(i64)) != i64 {
		return moerr.NewInvalidInputNoCtx("invalid batch row count")
	}
	decodedRowCount := int(i64)

	l, err := types.ReadInt32AsInt(r)
	if err != nil {
		return err
	}
	if err = validateReaderElementCount(r, l, 4, "vector"); err != nil {
		return err
	}
	if err = bat.prepareOwnedDecodeVectors(l, mp); err != nil {
		return err
	}
	vecs := bat.Vecs

	for i := 0; i < l; i++ {
		vecL, err := types.ReadUint32(r)
		if err != nil {
			return err
		}
		limitedReader := io.LimitReader(r, int64(vecL))
		if err := vecs[i].UnmarshalWithReader(limitedReader, mp); err != nil {
			return err
		}
		// Ensure the vector consumed exactly the bytes allocated by its length prefix.
		// Any leftover bytes indicate a serialization mismatch and would corrupt
		// subsequent reads from the underlying reader.
		if n, _ := io.Copy(io.Discard, limitedReader); n > 0 {
			return moerr.NewInternalErrorNoCtxf("vector unmarshal did not consume all bytes: %d remaining", n)
		}
	}

	l, err = types.ReadInt32AsInt(r)
	if err != nil {
		return err
	}
	if err = validateReaderElementCount(r, l, 4, "attribute"); err != nil {
		return err
	}
	if !allowMetadata && l != 0 {
		return moerr.NewInvalidInputNoCtx("spill batch attributes are not allowed")
	}
	if l != len(bat.Attrs) {
		bat.Attrs = make([]string, l)
	}

	for i := 0; i < int(l); i++ {
		bs, err := readBatchSizedBytes(r)
		if err != nil {
			return err
		}
		bat.Attrs[i] = string(bs)
	}

	// ExtraBuf is a data-scaled Go-heap field in the stable Batch codec. Spill
	// records do not use it and reject it before allocating its payload.
	if allowMetadata {
		if bat.ExtraBuf, err = readBatchSizedBytes(r); err != nil {
			return err
		}
	} else {
		extraSize, readErr := types.ReadInt32AsInt(r)
		if readErr != nil {
			return readErr
		}
		if extraSize != 0 {
			return moerr.NewInvalidInputNoCtx("spill batch extra buffer is not allowed")
		}
		bat.ExtraBuf = nil
	}

	if bat.Recursive, err = types.ReadInt32(r); err != nil {
		return err
	}
	if bat.ShuffleIDX, err = types.ReadInt32(r); err != nil {
		return err
	}
	bat.rowCount = decodedRowCount
	return nil
}

func readBatchSizedBytes(r io.Reader) ([]byte, error) {
	size, err := types.ReadInt32AsInt(r)
	if err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, moerr.NewInvalidInputNoCtx("negative batch buffer size")
	}
	if limited, ok := r.(*io.LimitedReader); ok && int64(size) > limited.N {
		return nil, io.ErrUnexpectedEOF
	}
	if lengthAware, ok := r.(interface{ Len() int }); ok && size > lengthAware.Len() {
		return nil, io.ErrUnexpectedEOF
	}
	if size == 0 {
		return nil, nil
	}
	value := make([]byte, size)
	if _, err = io.ReadFull(r, value); err != nil {
		return nil, err
	}
	return value, nil
}

func validateReaderElementCount(
	r io.Reader,
	count int,
	minimumWireBytes int64,
	field string,
) error {
	const maxBatchWireFields = 1 << 20
	if count < 0 || count > maxBatchWireFields || minimumWireBytes <= 0 {
		return moerr.NewInvalidInputNoCtx("invalid batch " + field + " count")
	}
	var remaining int64 = -1
	switch reader := r.(type) {
	case *io.LimitedReader:
		remaining = reader.N
	case interface{ Len() int }:
		remaining = int64(reader.Len())
	}
	if remaining >= 0 && int64(count) > remaining/minimumWireBytes {
		return moerr.NewInvalidInputNoCtx("invalid batch " + field + " count")
	}
	return nil
}

// prepareOwnedDecodeVectors makes every destination an independent owner.
// Alias decoding deliberately installs borrowed vector buffers; those buffers
// must never be grown or relabeled by the owned streaming decoder.
func (bat *Batch) prepareOwnedDecodeVectors(count int, mp *mpool.MPool) error {
	if count < 0 {
		return moerr.NewInvalidInputNoCtx("invalid batch vector count")
	}
	allocationAccount := bat.allocationAccount
	if count != len(bat.Vecs) {
		if len(bat.Vecs) > 0 {
			bat.Clean(mp)
			bat.allocationAccount = allocationAccount
		}
		bat.Vecs = make([]*vector.Vector, count)
	}

	const inlineReceivers = 16
	var inline [inlineReceivers]*vector.Vector
	var used map[*vector.Vector]struct{}
	for i, vec := range bat.Vecs {
		selection := allocationAccount
		if selection == nil && vec != nil {
			selection = vec.AllocationAccountSelection()
		}
		if vec != nil {
			exists := false
			if i < inlineReceivers {
				for j := 0; j < i; j++ {
					if inline[j] == vec {
						exists = true
						break
					}
				}
				inline[i] = vec
			} else {
				if used == nil {
					used = make(map[*vector.Vector]struct{}, count)
					for _, prior := range inline {
						if prior != nil {
							used[prior] = struct{}{}
						}
					}
				}
				_, exists = used[vec]
				used[vec] = struct{}{}
			}
			if exists {
				vec = nil
			}
		}
		if vec == nil {
			if bat.offHeap {
				vec = vector.NewOffHeapVec()
			} else {
				vec = vector.NewVecFromReuse()
			}
		} else if vec.NeedDup() {
			vec.Free(mp)
		}
		vec.SetOffHeap(bat.offHeap)
		if vec.AllocationAccountSelection() != selection {
			if err := vec.CanSetAllocationAccount(selection); err != nil {
				vec.Free(mp)
				vec.SetOffHeap(bat.offHeap)
			}
			if err := vec.SetAllocationAccount(selection); err != nil {
				return err
			}
		}
		bat.Vecs[i] = vec
	}
	return nil
}

func (bat *Batch) ShrinkByMask(sels *bitmap.Bitmap, negate bool, offset uint64) {
	if !negate {
		if sels.Count() == bat.rowCount {
			return
		}
	}

	for _, vec := range bat.Vecs {
		vec.ShrinkByMask(sels, negate, offset)
	}

	if negate {
		bat.rowCount -= sels.Count()
		return
	}
	bat.rowCount = sels.Count()
}

func (bat *Batch) Shrink(sels []int64, negate bool) {
	if !negate {
		if len(sels) == bat.rowCount {
			return
		}
	}
	for _, vec := range bat.Vecs {
		vec.Shrink(sels, negate)
	}
	if negate {
		bat.rowCount -= len(sels)
		return
	}
	bat.rowCount = len(sels)
}

func (bat *Batch) Shuffle(sels []int64, m *mpool.MPool) error {
	if len(sels) > 0 {
		mp := make(map[*vector.Vector]uint8)
		for _, vec := range bat.Vecs {
			if _, ok := mp[vec]; ok {
				continue
			}
			mp[vec]++
			if err := vec.Shuffle(sels, m); err != nil {
				return err
			}
		}
		bat.rowCount = len(sels)
	}
	return nil
}

func (bat *Batch) Size() int {
	var size int

	for _, vec := range bat.Vecs {
		size += vec.Size()
	}
	return size
}

func (bat *Batch) RowCount() int {
	return bat.rowCount
}

func (bat *Batch) VectorCount() int {
	return len(bat.Vecs)
}

func (bat *Batch) SetAttributes(attrs []string) {
	bat.Attrs = attrs
}

// AllocationAccountSelection returns the immutable destination selection used
// by this batch's owned off-heap vectors.
func (bat *Batch) AllocationAccountSelection() *vector.AllocationAccountSelection {
	if bat == nil {
		return nil
	}
	return bat.allocationAccount
}

// SetAllocationAccount configures every existing empty destination vector as
// one transaction. Existing physical allocations are never relabeled.
func (bat *Batch) SetAllocationAccount(
	selection *vector.AllocationAccountSelection,
) error {
	if bat == nil || (selection != nil && !bat.offHeap) {
		return mpool.ErrAllocationAccountInvalid
	}
	for _, vec := range bat.Vecs {
		if vec != nil {
			if err := vec.CanSetAllocationAccount(selection); err != nil {
				return err
			}
		}
	}
	for _, vec := range bat.Vecs {
		if vec != nil {
			if err := vec.SetAllocationAccount(selection); err != nil {
				panic(err)
			}
		}
	}
	bat.allocationAccount = selection
	return nil
}

func (bat *Batch) configureOwnedVector(vec *vector.Vector) {
	if vec == nil {
		return
	}
	vec.SetOffHeap(bat.offHeap)
	if bat.allocationAccount != nil {
		if err := vec.SetAllocationAccount(bat.allocationAccount); err != nil {
			panic(err)
		}
	}
}

func (bat *Batch) InsertVector(
	pos int32,
	attr string,
	vec *vector.Vector,
) {
	bat.configureOwnedVector(vec)
	bat.Vecs = append(bat.Vecs, nil)
	copy(bat.Vecs[pos+1:], bat.Vecs[pos:])
	bat.Vecs[pos] = vec
	bat.Attrs = append(bat.Attrs, "")
	copy(bat.Attrs[pos+1:], bat.Attrs[pos:])
	bat.Attrs[pos] = attr
}

func (bat *Batch) SetVector(pos int32, vec *vector.Vector) {
	bat.configureOwnedVector(vec)
	bat.Vecs[pos] = vec
}

func (bat *Batch) GetVector(pos int32) *vector.Vector {
	return bat.Vecs[pos]
}

func (bat *Batch) CloneSelectedColumns(
	selectCols []int,
	selectAttrs []string,
	mp *mpool.MPool,
) (cloned *Batch, err error) {
	cloned = NewWithSize(len(selectCols))
	cloned.Attrs = selectAttrs
	cloned.offHeap = bat.offHeap || bat.selectedColumnsHaveAllocationAccount(selectCols)
	var typ types.Type
	for idx := range selectCols {
		if cloned.offHeap {
			cloned.Vecs[idx] = vector.NewOffHeapVecWithType(typ)
		} else {
			cloned.Vecs[idx] = vector.NewVec(typ)
		}
	}
	if err = configureCloneAllocation(bat, cloned, selectCols); err != nil {
		cloned.Clean(mp)
		return nil, err
	}
	if err = bat.CloneSelectedColumnsTo(selectCols, cloned, mp); err != nil {
		cloned.Clean(mp)
		cloned = nil
		return
	}
	return
}

func (bat *Batch) CloneSelectedColumnsTo(
	selectCols []int,
	toBat *Batch,
	mp *mpool.MPool,
) (err error) {
	for idx, sourceIdx := range selectCols {
		toVec := toBat.Vecs[idx]
		toVec.ResetWithNewType(bat.Vecs[sourceIdx].GetType())
		if err = toVec.UnionBatch(
			bat.Vecs[sourceIdx],
			0,
			bat.Vecs[sourceIdx].Length(),
			nil,
			mp,
		); err != nil {
			return
		}
		toVec.SetSorted(bat.Vecs[sourceIdx].GetSorted())

		if toVec.Length() != bat.rowCount {
			return moerr.NewInternalErrorNoCtx("toVec.Length() != bat.rowCount")
		}
	}
	toBat.rowCount = bat.rowCount
	return nil
}

func (bat *Batch) SelectColumns(cols []int, attrs []string) *Batch {
	rbat := NewWithSize(len(cols))
	rbat.Attrs = attrs
	rbat.offHeap = bat.offHeap
	rbat.allocationAccount = bat.allocationAccount
	for i, col := range cols {
		rbat.Vecs[i] = bat.Vecs[col]
	}
	rbat.rowCount = bat.rowCount
	return rbat
}

func (bat *Batch) Clean(m *mpool.MPool) {
	// situations that batch was still in use.
	if bat == EmptyBatch || bat == CteEndBatch || bat == EmptyForConstFoldBatch {
		return
	}

	for i, vec := range bat.Vecs {
		if vec != nil {
			bat.SetVector(int32(i), nil)
			vec.Free(m)
		}
	}

	bat.Vecs = nil
	bat.Attrs = nil
	bat.ExtraBuf = nil
	bat.SetRowCount(0)
	bat.allocationAccount = nil
}

func (bat *Batch) Last() bool {
	return bat.Recursive > 0
}

func (bat *Batch) SetEnd() {
	bat.Recursive = 2
}

func (bat *Batch) SetLast() {
	bat.Recursive = 1
}

func (bat *Batch) End() bool {
	return bat.Recursive == 2
}

func (bat *Batch) CleanOnlyData() {
	for _, vec := range bat.Vecs {
		if vec != nil {
			vec.CleanOnlyData()
		}
	}
	bat.rowCount = 0
}

func (bat *Batch) FreeColumns(m *mpool.MPool) {
	for _, vec := range bat.Vecs {
		if vec != nil {
			selection := vec.AllocationAccountSelection()
			vec.Free(m)
			if bat.allocationAccount != nil {
				selection = bat.allocationAccount
			}
			if err := vec.SetAllocationAccount(selection); err != nil {
				panic(err)
			}
		}
	}
}

func (bat *Batch) String() string {
	var buf bytes.Buffer

	for i, vec := range bat.Vecs {
		buf.WriteString(fmt.Sprintf("%d : %s\n", i, vec.String()))
	}
	return buf.String()
}

func (bat *Batch) GetSchema() (attrs []string, attrTypes []types.Type) {
	attrs = make([]string, len(bat.Attrs))
	attrTypes = make([]types.Type, len(bat.Vecs))
	copy(attrs, bat.Attrs)
	for i, vec := range bat.Vecs {
		attrTypes[i] = *vec.GetType()
	}
	return
}

func vectorAllocationSelectionsMatch(left, right *Batch) bool {
	if left == nil || right == nil || len(left.Vecs) != len(right.Vecs) {
		return false
	}
	if left.allocationAccount != right.allocationAccount {
		return false
	}
	for i := range left.Vecs {
		if left.Vecs[i] == nil || right.Vecs[i] == nil {
			if left.Vecs[i] != right.Vecs[i] {
				return false
			}
			continue
		}
		if left.Vecs[i].AllocationAccountSelection() !=
			right.Vecs[i].AllocationAccountSelection() {
			return false
		}
	}
	return true
}

func configureCloneAllocation(
	source, destination *Batch,
	selectedColumns []int,
) error {
	if source == nil || destination == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if source.allocationAccount != nil {
		return destination.SetAllocationAccount(source.allocationAccount)
	}
	for destinationIdx := range destination.Vecs {
		sourceIdx := destinationIdx
		if len(selectedColumns) > 0 {
			sourceIdx = selectedColumns[destinationIdx]
		}
		selection := source.Vecs[sourceIdx].AllocationAccountSelection()
		if selection == nil {
			continue
		}
		if !destination.offHeap {
			return mpool.ErrAllocationAccountInvalid
		}
		if err := destination.Vecs[destinationIdx].SetAllocationAccount(selection); err != nil {
			return err
		}
	}
	return nil
}

func (bat *Batch) Clone(mp *mpool.MPool, offHeap bool) (*Batch, error) {
	var (
		cloned           *Batch
		attrs, attrTypes = bat.GetSchema()
	)
	cloned = NewWithSchema(offHeap, attrs, attrTypes)
	if err := configureCloneAllocation(bat, cloned, nil); err != nil {
		cloned.Clean(mp)
		return nil, err
	}
	cloned.Recursive = bat.Recursive
	err := bat.CloneTo(cloned, mp)
	if err != nil {
		return nil, err
	}
	return cloned, nil
}

func (bat *Batch) CloneTo(toBat *Batch, mp *mpool.MPool) (err error) {
	for i, srcVec := range bat.Vecs {
		toVec := toBat.Vecs[i]
		toVec.ResetWithNewType(srcVec.GetType())
		if err = toVec.UnionBatch(srcVec, 0, srcVec.Length(), nil, mp); err != nil {
			toBat.Clean(mp)
			return
		}
		toVec.SetSorted(srcVec.GetSorted())
	}
	toBat.rowCount = bat.rowCount
	toBat.ShuffleIDX = bat.ShuffleIDX

	return
}

// Dup used to copy a Batch object, this method will create a new batch
// and copy all vectors (Vecs) of the current batch to the new batch.
func (bat *Batch) Dup(mp *mpool.MPool) (*Batch, error) {
	return bat.Clone(mp, bat.offHeap || bat.hasAllocationAccountVector())
}

func (bat *Batch) hasAllocationAccountVector() bool {
	for _, vec := range bat.Vecs {
		if vec != nil && vec.AllocationAccountSelection() != nil {
			return true
		}
	}
	return false
}

func (bat *Batch) selectedColumnsHaveAllocationAccount(selectCols []int) bool {
	for _, sourceIdx := range selectCols {
		vec := bat.Vecs[sourceIdx]
		if vec != nil && vec.AllocationAccountSelection() != nil {
			return true
		}
	}
	return false
}

func (bat *Batch) Union(bat2 *Batch, sels []int64, m *mpool.MPool) error {
	for i, vec := range bat.Vecs {
		if err := vec.Union(bat2.Vecs[i], sels, m); err != nil {
			return err
		}
	}
	if len(bat.Vecs) > 0 {
		bat.rowCount = bat.Vecs[0].Length()
	}
	return nil
}

func (bat *Batch) UnionWindow(bat2 *Batch, offset, cnt int, m *mpool.MPool) error {
	for i, vec := range bat.Vecs {
		if err := vec.UnionBatch(bat2.Vecs[i], int64(offset), cnt, nil, m); err != nil {
			return err
		}
	}
	bat.rowCount += cnt
	return nil
}

func (bat *Batch) UnionOne(bat2 *Batch, pos int64, m *mpool.MPool) error {
	for i, vec := range bat.Vecs {
		if err := vec.UnionOne(bat2.Vecs[i], pos, m); err != nil {
			return err
		}
	}
	bat.rowCount++
	return nil
}

func (bat *Batch) PreExtend(mp *mpool.MPool, rows int) error {
	for i := range bat.Vecs {
		if err := bat.Vecs[i].PreExtend(rows, mp); err != nil {
			return err
		}
	}
	return nil
}

// AppendWithCopy is used to append data from batch `b` to another batch `bat`. The function
// ensures that the batch structure is consistent and copies all vector data to the target batch.
// WARING: this function will cause a memory allocation.
func (bat *Batch) AppendWithCopy(ctx context.Context, mp *mpool.MPool, b *Batch) (*Batch, error) {
	if bat == nil {
		return b.Dup(mp)
	}
	if len(bat.Vecs) != len(b.Vecs) {
		return nil, moerr.NewInternalError(ctx, "unexpected error happens in batch append")
	}
	if len(bat.Vecs) == 0 {
		return bat, nil
	}

	for i := range bat.Vecs {
		if err := bat.Vecs[i].UnionBatch(b.Vecs[i], 0, b.Vecs[i].Length(), nil, mp); err != nil {
			return bat, err
		}
		bat.Vecs[i].SetSorted(false)
	}
	bat.rowCount += b.rowCount
	return bat, nil
}

func (bat *Batch) Append(ctx context.Context, mp *mpool.MPool, b *Batch) (*Batch, error) {
	if bat == nil {
		return b, nil
	}
	if len(bat.Vecs) != len(b.Vecs) {
		return nil, moerr.NewInternalError(ctx, "unexpected error happens in batch append")
	}
	if len(bat.Vecs) == 0 {
		return bat, nil
	}

	for i := range bat.Vecs {
		if err := bat.Vecs[i].UnionBatch(b.Vecs[i], 0, b.Vecs[i].Length(), nil, mp); err != nil {
			return bat, err
		}
		bat.Vecs[i].SetSorted(false)
	}
	bat.rowCount += b.rowCount
	return bat, nil
}

func (bat *Batch) AddRowCount(rowCount int) {
	bat.rowCount += rowCount
}

func (bat *Batch) SetRowCount(rowCount int) {
	bat.rowCount = rowCount
}

func (bat *Batch) ReplaceVector(oldVec *vector.Vector, newVec *vector.Vector, startIndex int) {
	for i := startIndex; i < len(bat.Vecs); i++ {
		if bat.Vecs[i] == oldVec {
			bat.SetVector(int32(i), newVec)
		}
	}
}

func (bat *Batch) IsEmpty() bool {
	return bat.rowCount == 0
}

func (bat *Batch) IsDone() bool {
	if bat == nil {
		return true
	}
	return bat.IsEmpty() || bat.Last()
}

func (bat *Batch) Allocated() int {
	if bat == nil {
		return 0
	}
	ret := 0
	for i := range bat.Vecs {
		if bat.Vecs[i] != nil {
			ret += bat.Vecs[i].Allocated()
		}
	}
	return ret
}

func (bat *Batch) Window(start, end int) (*Batch, error) {
	b := NewWithSize(len(bat.Vecs))
	var err error
	b.Attrs = bat.Attrs
	b.offHeap = bat.offHeap
	b.allocationAccount = bat.allocationAccount
	for i, vec := range bat.Vecs {
		b.Vecs[i], err = vec.Window(start, end)
		if err != nil {
			return nil, err
		}
		b.Vecs[i].SetOffHeap(bat.offHeap)
	}
	b.rowCount = end - start
	return b, nil
}

// WindowWithAllocation is the allocation-accounted counterpart of Window.
// Vector data and area remain borrowed; null/grouping range bitmaps are owned
// by selection and are released when the returned batch is cleaned.
func (bat *Batch) WindowWithAllocation(
	start int,
	end int,
	mp *mpool.MPool,
	selection *vector.AllocationAccountSelection,
) (*Batch, error) {
	if bat == nil || mp == nil || selection == nil ||
		start < 0 || end < start || end > bat.RowCount() {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	b := NewOffHeapWithSize(len(bat.Vecs))
	b.Attrs = bat.Attrs
	if err := b.SetAllocationAccount(selection); err != nil {
		b.Clean(mp)
		return nil, err
	}
	for i, vec := range bat.Vecs {
		if vec == nil {
			b.Clean(mp)
			return nil, mpool.ErrAllocationAccountInvalid
		}
		var err error
		b.Vecs[i], err = vec.WindowWithAllocation(start, end, mp, selection)
		if err != nil {
			b.Clean(mp)
			return nil, err
		}
	}
	b.rowCount = end - start
	return b, nil
}
