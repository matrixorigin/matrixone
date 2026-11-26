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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func NewWithSize(n int) *Batch {
	return &Batch{
		Vecs:     make([]*vector.Vector, n),
		rowCount: 0,
	}
}

// NewWithTypes returns a batch of empty vectors of attTypes, all from mp.
func NewWithTypes(attTypes []types.Type, mp *mpool.MPool) *Batch {
	bat := NewWithSize(len(attTypes))
	for i, t := range attTypes {
		bat.Vecs[i] = vector.NewVec(t, mp)
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

// MarshalBinary encodes bat in the Batch wire format: the row count, each
// vector with its length, the extra buffer, Recursive and ShuffleIDX. Peers
// run the same binary (shutdown upgrade only) and the bytes are never
// persisted, so the format carries no version or legacy sections.
func (bat *Batch) MarshalBinary() ([]byte, error) {
	var w binaryAppender
	if err := bat.MarshalBinaryTo(&w); err != nil {
		return nil, err
	}
	return w.buf, nil
}

// binaryAppender is the writer MarshalBinary encodes into. MarshalBinaryTo
// sizes it exactly through EnsureCapacity, and the typed writes keep the
// small headers from escaping to the heap through io.Writer.
type binaryAppender struct {
	buf []byte
}

func (a *binaryAppender) Len() int { return len(a.buf) }

func (a *binaryAppender) EnsureCapacity(n int) error {
	a.buf = slices.Grow(a.buf, n-len(a.buf))
	return nil
}

func (a *binaryAppender) Write(p []byte) (int, error) {
	a.buf = append(a.buf, p...)
	return len(p), nil
}

func (a *binaryAppender) WriteByte(v byte) error {
	a.buf = append(a.buf, v)
	return nil
}

func (a *binaryAppender) WriteUint32(v uint32) error {
	a.buf = binary.NativeEndian.AppendUint32(a.buf, v)
	return nil
}

func (a *binaryAppender) WriteInt32(v int32) error {
	a.buf = binary.NativeEndian.AppendUint32(a.buf, uint32(v))
	return nil
}

func (a *binaryAppender) WriteUint64(v uint64) error {
	a.buf = binary.NativeEndian.AppendUint64(a.buf, v)
	return nil
}

func (a *binaryAppender) WriteInt64(v int64) error {
	a.buf = binary.NativeEndian.AppendUint64(a.buf, uint64(v))
	return nil
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
	return bat.prepareMarshalBinary(nil, false)
}

func (bat *Batch) prepareMarshalBinary(plans []vector.MarshalBinaryPlan, trusted bool) (int, error) {
	if bat == nil {
		return 0, moerr.NewInvalidInputNoCtx("invalid batch for marshal")
	}
	if bat.selection != nil {
		return 0, errSelectedBatch()
	}
	const fixedSize = uint64(8 + 4 + 4 + 4 + 4)
	total := fixedSize
	add := func(value uint64) bool {
		if value > uint64(^uint(0)>>1)-total {
			return false
		}
		total += value
		return true
	}
	if uint64(len(bat.Vecs)) > uint64(^uint32(0)>>1) ||
		uint64(len(bat.extraBuf)) > uint64(^uint32(0)>>1) {
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
		var plan vector.MarshalBinaryPlan
		var err error
		if trusted {
			plan, err = vec.PrepareMarshalBinaryTrusted()
		} else {
			plan, err = vec.PrepareMarshalBinary()
		}
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
	if !add(uint64(len(bat.extraBuf))) {
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
	if bat.selection != nil {
		return errSelectedBatch()
	}
	var inlinePlans [64]vector.MarshalBinaryPlan
	var plans []vector.MarshalBinaryPlan
	if len(bat.Vecs) <= len(inlinePlans) {
		plans = inlinePlans[:len(bat.Vecs)]
	} else {
		plans = make([]vector.MarshalBinaryPlan, len(bat.Vecs))
	}
	size, err := bat.prepareMarshalBinary(plans, false)
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

	extraSize := int32(len(bat.extraBuf))
	if err := writeBatchMarshalInt32(w, extraSize); err != nil {
		return err
	}
	if err := types.WriteFull(w, bat.extraBuf); err != nil {
		return err
	}

	if err := writeBatchMarshalInt32(w, bat.Recursive); err != nil {
		return err
	}
	return writeBatchMarshalInt32(w, bat.ShuffleIDX)
}

// marshalTrustedSize is MarshalBinarySize for a valid batch; see
// vector.PrepareMarshalBinaryTrusted. It allocates nothing.
func (bat *Batch) marshalTrustedSize() (int, error) {
	return bat.prepareMarshalBinary(nil, true)
}

// errBatchChanged reports a batch that no longer matches the size its
// encoding was given: it changed while borrowed.
func errBatchChanged() error {
	return moerr.NewInvalidInputNoCtx("batch changed while marshaling")
}

// marshalTrustedTo writes MarshalBinary's encoding of a valid batch into dst,
// which must hold exactly marshalTrustedSize bytes. It allocates nothing.
func (bat *Batch) marshalTrustedTo(dst []byte) error {
	if bat == nil {
		return io.ErrClosedPipe
	}
	dst = dst[:len(dst):len(dst)]
	if bat.selection != nil {
		return errSelectedBatch()
	}
	if len(dst) < 12 {
		return errBatchChanged()
	}
	binary.NativeEndian.PutUint64(dst, uint64(bat.rowCount))
	binary.NativeEndian.PutUint32(dst[8:], uint32(len(bat.Vecs)))
	n := 12
	for _, vec := range bat.Vecs {
		if vec == nil {
			return moerr.NewInvalidInputNoCtx("cannot marshal a nil batch vector")
		}
		plan, err := vec.PrepareMarshalBinaryTrusted()
		if err != nil {
			return err
		}
		if len(dst)-n < 4+plan.Size() {
			return errBatchChanged()
		}
		binary.NativeEndian.PutUint32(dst[n:], uint32(plan.Size()))
		end := n + 4 + plan.Size()
		written, err := plan.MarshalToBytes(dst[n+4 : end : end])
		if err != nil {
			return err
		}
		n += 4 + written
	}
	if len(dst)-n != 4+len(bat.extraBuf)+8 {
		return errBatchChanged()
	}
	binary.NativeEndian.PutUint32(dst[n:], uint32(len(bat.extraBuf)))
	n += 4
	n += copy(dst[n:], bat.extraBuf)
	binary.NativeEndian.PutUint32(dst[n:], uint32(bat.Recursive))
	binary.NativeEndian.PutUint32(dst[n+4:], uint32(bat.ShuffleIDX))
	return nil
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
	return types.WriteFull(w, data[:])
}

func writeBatchMarshalInt32(w io.Writer, value int32) error {
	if typed, ok := w.(batchPrimitiveWriter); ok {
		return typed.WriteInt32(value)
	}
	var data [4]byte
	binary.NativeEndian.PutUint32(data[:], uint32(value))
	return types.WriteFull(w, data[:])
}

func writeBatchMarshalInt64(w io.Writer, value int64) error {
	if typed, ok := w.(batchPrimitiveWriter); ok {
		return typed.WriteInt64(value)
	}
	var data [8]byte
	binary.NativeEndian.PutUint64(data[:], uint64(value))
	return types.WriteFull(w, data[:])
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
	return bat.unmarshalBinary(data, mp, true)
}

// unmarshalBinary decodes data. Without validateValues it trusts data to
// come from MarshalBinary of a valid batch: the framing and sizes are still
// checked, but not the null bitmaps or varlen payloads.
func (bat *Batch) unmarshalBinary(data []byte, mp *mpool.MPool, validateValues bool) (err error) {
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
			if err := bat.freeReceivers(mp); err != nil {
				return err
			}
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
		// A receiver is reused only when it belongs to mp; with a nil mp the
		// decoded vectors are read-only references to data. A read-only
		// receiver is dropped; one owned by another pool is not ours to free.
		if vecs[i] != nil && vecs[i].MPool() != mp {
			if err := dropReadOnlyReceiver(vecs[i]); err != nil {
				return err
			}
			vecs[i] = nil
		}
		if vecs[i] == nil {
			vecs[i] = vector.NewVec(types.T_any.ToType(), mp)
		} else {
			vecs[i].Free(mp)
		}
		if validateValues {
			err = vecs[i].UnmarshalBinary(vecData)
		} else {
			err = vecs[i].UnmarshalBinaryTrusted(vecData)
		}
		if err != nil {
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
	// With a nil mp the decoded vectors are read-only references to data;
	// extraBuf borrows data the same way.
	if mp == nil {
		bat.releaseExtraBuf()
		bat.mp = nil
		if len(extraBuf) != 0 {
			bat.extraBuf = extraBuf
		}
	} else if err := bat.SetExtraBuf(mp, extraBuf); err != nil {
		return err
	}

	bat.Recursive, err = cursor.readInt32()
	if err != nil {
		return err
	}
	bat.ShuffleIDX, err = cursor.readInt32()
	if err != nil {
		return err
	}
	if len(cursor.data) != 0 {
		return moerr.NewInvalidInputNoCtxf("batch has %d trailing bytes", len(cursor.data))
	}
	bat.rowCount = decodedRowCount
	return nil
}

// UnmarshalFromReader decodes one batch from r, reading exactly its bytes.
func (bat *Batch) UnmarshalFromReader(r io.Reader, mp *mpool.MPool) (err error) {
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

	if err = bat.readExtraBuf(r, mp); err != nil {
		return err
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

// MarshalFramedSize returns the size of the record MarshalFramedTo writes.
func (bat *Batch) MarshalFramedSize() (int, error) {
	size, err := bat.MarshalBinarySize()
	if err != nil {
		return 0, err
	}
	if size > math.MaxInt-8 {
		return 0, moerr.NewInvalidInputNoCtx("batch record exceeds marshal format")
	}
	return size + 8, nil
}

// MarshalFramedTo writes bat as one record of a batch file: an int64 length
// followed by the batch. UnmarshalFramedFrom reads it back.
func (bat *Batch) MarshalFramedTo(w io.Writer) error {
	if bat == nil || w == nil {
		return io.ErrClosedPipe
	}
	size, err := bat.MarshalBinarySize()
	if err != nil {
		return err
	}
	if err := writeBatchMarshalInt64(w, int64(size)); err != nil {
		return err
	}
	return bat.MarshalBinaryTo(w)
}

// UnmarshalFramedFrom reads one record written by MarshalFramedTo, and checks
// that every vector holds the batch's row count.
func (bat *Batch) UnmarshalFramedFrom(r io.Reader, mp *mpool.MPool) error {
	size, err := types.ReadInt64(r)
	if err != nil {
		return err
	}
	if err := bat.UnmarshalSizedFrom(r, size, mp); err != nil {
		return err
	}
	if err := bat.CheckLength(); err != nil {
		return moerr.NewInvalidInputNoCtx("batch record vector length does not match row count")
	}
	return nil
}

// UnmarshalSizedFrom decodes a batch that takes exactly size bytes of r, for
// a caller that frames its own records.
func (bat *Batch) UnmarshalSizedFrom(r io.Reader, size int64, mp *mpool.MPool) error {
	if bat == nil || r == nil {
		return io.ErrClosedPipe
	}
	if size < 0 {
		return moerr.NewInvalidInputNoCtx("negative batch record size")
	}
	limited := &io.LimitedReader{R: r, N: size}
	if err := bat.UnmarshalFromReader(limited, mp); err != nil {
		return err
	}
	if limited.N != 0 {
		return moerr.NewInvalidInputNoCtxf("batch record has %d trailing bytes", limited.N)
	}
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

// readExtraBuf reads a size-prefixed extraBuf from r into memory allocated
// from mp, or into Go-heap memory the batch does not own when mp is nil.
func (bat *Batch) readExtraBuf(r io.Reader, mp *mpool.MPool) error {
	bat.releaseExtraBuf()
	bat.mp = mp
	if mp == nil {
		data, err := readBatchSizedBytes(r)
		bat.extraBuf = data
		return err
	}
	size, err := types.ReadInt32AsInt(r)
	if err != nil {
		return err
	}
	if size < 0 {
		return moerr.NewInvalidInputNoCtx("negative batch buffer size")
	}
	if limited, ok := r.(*io.LimitedReader); ok && int64(size) > limited.N {
		return io.ErrUnexpectedEOF
	}
	if lengthAware, ok := r.(interface{ Len() int }); ok && size > lengthAware.Len() {
		return io.ErrUnexpectedEOF
	}
	if size == 0 {
		return nil
	}
	data, err := mp.Alloc(size, true)
	if err != nil {
		return err
	}
	bat.extraBuf = data
	if _, err = io.ReadFull(r, data); err != nil {
		return err
	}
	return nil
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

// prepareOwnedDecodeVectors makes every destination an independent vector of
// mp for the streaming decoder.
func (bat *Batch) prepareOwnedDecodeVectors(count int, mp *mpool.MPool) error {
	if count < 0 {
		return moerr.NewInvalidInputNoCtx("invalid batch vector count")
	}
	if count != len(bat.Vecs) {
		if len(bat.Vecs) > 0 {
			if err := bat.freeReceivers(mp); err != nil {
				return err
			}
		}
		bat.Vecs = make([]*vector.Vector, count)
	}

	const inlineReceivers = 16
	var inline [inlineReceivers]*vector.Vector
	var used map[*vector.Vector]struct{}
	for i, vec := range bat.Vecs {
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
		if vec != nil && vec.MPool() != mp {
			if err := dropReadOnlyReceiver(vec); err != nil {
				return err
			}
			vec = nil
		}
		if vec == nil {
			vec = vector.NewVec(types.T_any.ToType(), mp)
		}
		bat.Vecs[i] = vec
	}
	return nil
}

func (bat *Batch) ShrinkByMask(sels *bitmap.Bitmap, negate bool, offset uint64, mp *mpool.MPool) error {
	if err := requireCompacted("ShrinkByMask", bat); err != nil {
		return err
	}
	if !negate {
		if sels.Count() == bat.rowCount {
			return nil
		}
	}

	for _, vec := range bat.Vecs {
		if err := vec.ShrinkByMask(sels, negate, offset, mp); err != nil {
			return err
		}
	}

	if negate {
		bat.rowCount -= sels.Count()
		return nil
	}
	bat.rowCount = sels.Count()
	return nil
}

func (bat *Batch) Shrink(sels []int64, negate bool, mp *mpool.MPool) error {
	if err := requireCompacted("Shrink", bat); err != nil {
		return err
	}
	if !negate {
		if len(sels) == bat.rowCount {
			return nil
		}
	}
	for _, vec := range bat.Vecs {
		if err := vec.Shrink(sels, negate, mp); err != nil {
			return err
		}
	}
	if negate {
		bat.rowCount -= len(sels)
		return nil
	}
	bat.rowCount = len(sels)
	return nil
}

func (bat *Batch) Shuffle(sels []int64, m *mpool.MPool) error {
	if err := requireCompacted("Shuffle", bat); err != nil {
		return err
	}
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

// ExtraBuf returns the batch's opaque payload, or nil. The bytes stay owned
// by the batch and are valid until it releases or replaces them.
func (bat *Batch) ExtraBuf() []byte {
	if bat == nil {
		return nil
	}
	return bat.extraBuf
}

// SetExtraBuf replaces the payload with a copy of data allocated from mp.
// data may alias the current payload.
func (bat *Batch) SetExtraBuf(mp *mpool.MPool, data []byte) error {
	if bat == nil || mp == nil {
		return moerr.NewInternalErrorNoCtx("batch extra buffer needs a batch and a pool")
	}
	var copied []byte
	if len(data) != 0 {
		var err error
		if copied, err = mp.Alloc(len(data), true); err != nil {
			return err
		}
		copy(copied, data)
	}
	bat.AdoptExtraBuf(mp, copied)
	return nil
}

// AdoptExtraBuf replaces the payload with data, an allocation from mp that the
// batch now owns and releases.
func (bat *Batch) AdoptExtraBuf(mp *mpool.MPool, data []byte) {
	bat.releaseExtraBuf()
	bat.mp = mp
	if len(data) == 0 {
		if mp != nil && cap(data) != 0 {
			mp.Free(data)
		}
		return
	}
	bat.extraBuf = data
}

// SetAccountedExtraBuffer transfers one accounted byte buffer into the Batch.
// Clean releases it exactly once; MoveExtraBufferFrom preserves ownership when
// a pipeline spool moves the payload to another Batch object.
func (bat *Batch) SetAccountedExtraBuffer(buffer *mpool.AccountedBuffer) error {
	if bat == nil || buffer == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	data, pool, err := buffer.Detach()
	if err != nil {
		return err
	}
	bat.AdoptExtraBuf(pool, data)
	return nil
}

// MoveExtraBufferFrom moves source's payload, and the pool that owns it, to
// bat; bat's previous payload is released first. When source only borrows its
// payload (a view, or a batch decoded without a pool), bat gets a copy
// allocated from mp instead, so it never outlives the real owner's memory.
func (bat *Batch) MoveExtraBufferFrom(source *Batch, mp *mpool.MPool) error {
	if bat == nil || source == nil || bat == source {
		return nil
	}
	if len(source.extraBuf) != 0 && source.mp == nil {
		return bat.SetExtraBuf(mp, source.extraBuf)
	}
	bat.releaseExtraBuf()
	bat.extraBuf = source.extraBuf
	bat.mp = source.mp
	source.extraBuf = nil
	return nil
}

// DropExtraBuffer releases the payload. It is used by reusable pipeline
// batches before entering a cache.
func (bat *Batch) DropExtraBuffer() {
	if bat != nil {
		bat.releaseExtraBuf()
	}
}

// releaseExtraBuf frees extraBuf through mp when the batch owns it. It keeps
// mp: the pool belongs to the batch, not to one payload.
func (bat *Batch) releaseExtraBuf() {
	if bat.mp != nil && cap(bat.extraBuf) != 0 {
		bat.mp.Free(bat.extraBuf)
	}
	bat.extraBuf = nil
}

// OwnReadOnlyVectors replaces every read-only (reference) vector of bat with
// a copy in mp, so the batch can be mutated in place. A vector shared by
// several columns is copied once.
//
// It copies every vector before replacing any: a vector can sit in several
// columns, and on a failed copy bat is left unchanged, its originals intact,
// while the copies already made are freed.
func (bat *Batch) OwnReadOnlyVectors(mp *mpool.MPool) error {
	var copied map[*vector.Vector]*vector.Vector
	for _, vec := range bat.Vecs {
		if vec == nil || !vec.IsReadOnly() {
			continue
		}
		if _, ok := copied[vec]; ok {
			continue
		}
		owned, err := vec.Dup(mp)
		if err != nil {
			for _, c := range copied {
				c.Free(mp)
			}
			return err
		}
		if copied == nil {
			copied = make(map[*vector.Vector]*vector.Vector)
		}
		copied[vec] = owned
	}
	for i, vec := range bat.Vecs {
		if owned, ok := copied[vec]; ok {
			bat.Vecs[i] = owned
		}
	}
	// No column references the originals now. A read-only vector can still
	// own its nulls bitmap.
	for vec := range copied {
		vec.Free(nil)
	}
	return nil
}

// errForeignReceiver reports a decode receiver vector owned by a pool other
// than the decode pool: the batch's owner does not own it, so it must not be
// freed or reused here.
func errForeignReceiver() error {
	return moerr.NewInternalErrorNoCtx("batch decode receiver vector belongs to another pool")
}

// dropReadOnlyReceiver drops a read-only receiver's references; a receiver
// owned by any pool is an error.
func dropReadOnlyReceiver(vec *vector.Vector) error {
	if !vec.IsReadOnly() {
		return errForeignReceiver()
	}
	vec.Free(nil)
	return nil
}

// freeReceivers releases the decode receivers before the vector count
// changes: receivers of mp are freed, read-only ones dropped, and a receiver
// of another pool is an error.
func (bat *Batch) freeReceivers(mp *mpool.MPool) error {
	for _, vec := range bat.Vecs {
		if vec != nil && !vec.IsReadOnly() && vec.MPool() != mp {
			return errForeignReceiver()
		}
	}
	for _, vec := range bat.Vecs {
		if vec != nil {
			vec.Free(mp)
		}
	}
	bat.Vecs = nil
	return nil
}

// FreeWindow releases a batch returned by Window. Its vectors are read-only
// references, so this only drops them; it panics on an owned vector.
func (bat *Batch) FreeWindow() {
	if bat == nil {
		return
	}
	for _, vec := range bat.Vecs {
		if vec != nil {
			vec.Free(nil)
		}
	}
	bat.Vecs = nil
	bat.FreeSelection()
	bat.rowCount = 0
}

// CleanOwned frees each vector with the pool it belongs to. It is for test
// fixtures that assemble batches from several pools. Production code must not
// use it: an owner frees only the vectors it created, with its own pool.
func (bat *Batch) CleanOwned() {
	if bat == nil {
		return
	}
	for _, vec := range bat.Vecs {
		if vec != nil {
			vec.Free(vec.MPool())
		}
	}
	bat.Vecs = nil
	bat.FreeSelection()
	bat.rowCount = 0
}

func (bat *Batch) InsertVector(pos int32, vec *vector.Vector) {
	bat.Vecs = append(bat.Vecs, nil)
	copy(bat.Vecs[pos+1:], bat.Vecs[pos:])
	bat.Vecs[pos] = vec
}

func (bat *Batch) SetVector(pos int32, vec *vector.Vector) {
	bat.Vecs[pos] = vec
}

func (bat *Batch) GetVector(pos int32) *vector.Vector {
	return bat.Vecs[pos]
}

func (bat *Batch) CloneSelectedColumns(
	selectCols []int,
	mp *mpool.MPool,
) (cloned *Batch, err error) {
	cloned = NewWithSize(len(selectCols))
	var typ types.Type
	for idx := range selectCols {
		cloned.Vecs[idx] = vector.NewVec(typ, mp)
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
	if err = requireCompacted("CloneSelectedColumnsTo", bat, toBat); err != nil {
		return err
	}
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

// ViewInto makes dst a view of bat: the same vectors, row count
// and selection. dst owns nothing it references (no vectors, selection or
// ExtraBuf); it is valid while bat is, and is released by dropping it.
func (bat *Batch) ViewInto(dst *Batch) {
	dst.Recursive = bat.Recursive
	dst.ShuffleIDX = bat.ShuffleIDX
	dst.Vecs = append(dst.Vecs[:0], bat.Vecs...)
	dst.extraBuf = bat.extraBuf
	dst.mp = nil // a view owns nothing
	dst.rowCount = bat.rowCount
	dst.selection = bat.selection
}

func (bat *Batch) SelectColumns(cols []int) *Batch {
	rbat := NewWithSize(len(cols))
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
	bat.releaseExtraBuf()
	bat.SetRowCount(0)
	bat.FreeSelection()
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
	bat.selection = nil
}

func (bat *Batch) FreeColumns(m *mpool.MPool) {
	for _, vec := range bat.Vecs {
		if vec != nil {
			vec.Free(m)
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

// Types returns a copy of the types of bat's vectors.
func (bat *Batch) Types() []types.Type {
	attrTypes := make([]types.Type, len(bat.Vecs))
	for i, vec := range bat.Vecs {
		attrTypes[i] = *vec.GetType()
	}
	return attrTypes
}

// Clone copies bat into a new batch whose vectors all come from mp.
func (bat *Batch) Clone(mp *mpool.MPool) (*Batch, error) {
	cloned := NewWithTypes(bat.Types(), mp)
	cloned.Recursive = bat.Recursive
	err := bat.CloneTo(cloned, mp)
	if err != nil {
		return nil, err
	}
	return cloned, nil
}

func (bat *Batch) CloneTo(toBat *Batch, mp *mpool.MPool) (err error) {
	if err = requireCompacted("CloneTo", bat, toBat); err != nil {
		return err
	}
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
	return bat.Clone(mp)
}

func (bat *Batch) Union(bat2 *Batch, sels []int64, m *mpool.MPool) error {
	if err := requireCompacted("Union", bat, bat2); err != nil {
		return err
	}
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
	if err := requireCompacted("UnionWindow", bat, bat2); err != nil {
		return err
	}
	for i, vec := range bat.Vecs {
		if err := vec.UnionBatch(bat2.Vecs[i], int64(offset), cnt, nil, m); err != nil {
			return err
		}
	}
	bat.rowCount += cnt
	return nil
}

func (bat *Batch) UnionOne(bat2 *Batch, pos int64, m *mpool.MPool) error {
	if err := requireCompacted("UnionOne", bat, bat2); err != nil {
		return err
	}
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
	if err := requireCompacted("AppendWithCopy", bat, b); err != nil {
		return nil, err
	}
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
	if err := requireCompacted("Append", bat, b); err != nil {
		return nil, err
	}
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
	bat.selection = nil
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
	if bat == nil || start < 0 || end < start || end > bat.RowCount() {
		return nil, moerr.NewInvalidInputNoCtx("invalid batch window")
	}
	b := NewWithSize(len(bat.Vecs))
	var err error
	for i, vec := range bat.Vecs {
		if vec == nil {
			b.FreeWindow()
			return nil, moerr.NewInvalidInputNoCtx("invalid batch vector")
		}
		// Each window is a read-only reference to its source vector.
		b.Vecs[i], err = vec.WindowByLogicalRows(start, end)
		if err != nil {
			b.FreeWindow()
			return nil, err
		}
	}
	b.rowCount = end - start
	return b, nil
}
