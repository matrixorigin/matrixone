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
	"fmt"
	"io"

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
	// reset the buffer if caller wants to.
	if reset {
		w.Reset()
	}

	// row count.
	rl := int64(bat.rowCount)
	w.Write(types.EncodeInt64(&rl))

	// Vecs
	l := int32(len(bat.Vecs))
	w.Write(types.EncodeInt32(&l))
	for i := 0; i < int(l); i++ {
		var size uint32
		offset := w.Len()
		w.Write(types.EncodeUint32(&size))
		err := bat.Vecs[i].MarshalBinaryWithBuffer(w)
		if err != nil {
			return nil, err
		}
		size = uint32(w.Len() - offset - 4)
		buf := w.Bytes()
		copy(buf[offset:], types.EncodeUint32(&size))
	}

	// Attrs
	l = int32(len(bat.Attrs))
	w.Write(types.EncodeInt32(&l))
	for i := 0; i < int(l); i++ {
		size := int32(len(bat.Attrs[i]))
		w.Write(types.EncodeInt32(&size))
		n, _ := w.WriteString(bat.Attrs[i])
		if int32(n) != size {
			panic("unexpected length for string")
		}
	}

	// ExtraBuf
	types.WriteSizeBytes(bat.ExtraBuf, w)

	w.Write(types.EncodeInt32(&bat.Recursive))
	w.Write(types.EncodeInt32(&bat.ShuffleIDX))

	return w.Bytes(), nil
}

func (bat *Batch) UnmarshalBinary(data []byte) (err error) {
	return bat.UnmarshalBinaryWithAnyMp(data, nil)
}

func (bat *Batch) UnmarshalBinaryWithAnyMp(data []byte, mp *mpool.MPool) (err error) {
	bat.rowCount = int(types.DecodeInt64(data[:8]))
	data = data[8:]

	l := types.DecodeInt32(data[:4])
	// Fix for bug #23156: Handle Vecs length changes (from d4b79f12) while maintaining revert version's firstTime logic
	firstTime := bat.Vecs == nil
	vecsLen := int(l)
	vecsLenChanged := !firstTime && vecsLen != len(bat.Vecs)

	// CRITICAL FIX: When batch is reused (not firstTime), always reallocate Vecs if length changed
	// This ensures Vecs are properly reset and prevents stale data from previous unmarshal operations
	if firstTime || vecsLenChanged {
		if vecsLenChanged && len(bat.Vecs) > 0 {
			bat.Clean(mp)
		}
		bat.Vecs = make([]*vector.Vector, vecsLen)
		for i := range bat.Vecs {
			if bat.offHeap {
				bat.Vecs[i] = vector.NewOffHeapVec()
			} else {
				bat.Vecs[i] = vector.NewVecFromReuse()
			}
		}
	}

	vecs := bat.Vecs
	data = data[4:]

	for i := 0; i < vecsLen; i++ {
		size := types.DecodeInt32(data[:4])
		data = data[4:]

		if err := vecs[i].UnmarshalBinary(data[:size]); err != nil {
			return err
		}

		data = data[size:]
	}

	l = types.DecodeInt32(data[:4])
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
	data = data[4:]

	// Read serialized Attrs, but only up to min(serializedAttrsLen, attrsLen)
	// If serialized length > attrsLen: ignore excess (data inconsistency, attrsLen is authoritative)
	// If serialized length < attrsLen: read what's available (remaining will be empty strings, should not happen normally)
	attrsToRead := serializedAttrsLen
	if attrsToRead > attrsLen {
		attrsToRead = attrsLen
	}
	for i := 0; i < attrsToRead; i++ {
		size := types.DecodeInt32(data[:4])
		data = data[4:]
		bat.Attrs[i] = string(data[:size])
		data = data[size:]
	}
	// CRITICAL FIX: Clear remaining Attrs to prevent stale values when serializedAttrsLen < attrsLen
	// This is essential for batch reuse scenarios (e.g., UPDATE operations with IVF index)
	for i := attrsToRead; i < attrsLen; i++ {
		bat.Attrs[i] = ""
	}
	// If serialized Attrs length > vecsLen, skip the excess data
	for i := attrsToRead; i < serializedAttrsLen; i++ {
		size := types.DecodeInt32(data[:4])
		data = data[4:]
		data = data[size:]
	}

	// ExtraBuf
	l = types.DecodeInt32(data[:4])
	data = data[4:]
	bat.ExtraBuf = nil
	bat.ExtraBuf = append(bat.ExtraBuf, data[:l]...)
	data = data[l:]

	bat.Recursive = types.DecodeInt32(data[:4])
	data = data[4:]
	bat.ShuffleIDX = types.DecodeInt32(data[:4])
	return nil
}

func (bat *Batch) UnmarshalFromReader(r io.Reader, mp *mpool.MPool) (err error) {
	i64, err := types.ReadInt64(r)
	if err != nil {
		return err
	}
	bat.rowCount = int(i64)

	l, err := types.ReadInt32AsInt(r)
	if err != nil {
		return err
	}
	if l != len(bat.Vecs) {
		if len(bat.Vecs) > 0 {
			bat.Clean(mp)
		}
		bat.Vecs = make([]*vector.Vector, l)
		for i := range bat.Vecs {
			if bat.offHeap {
				bat.Vecs[i] = vector.NewOffHeapVec()
			} else {
				bat.Vecs[i] = vector.NewVecFromReuse()
			}
		}
	}
	vecs := bat.Vecs

	for i := 0; i < l; i++ {
		_, bs, err := types.ReadSizeBytes(r)
		if err != nil {
			return err
		}
		if err := vecs[i].UnmarshalWithReader(bytes.NewReader(bs), mp); err != nil {
			return err
		}
	}

	l, err = types.ReadInt32AsInt(r)
	if err != nil {
		return err
	}
	if l != len(bat.Attrs) {
		bat.Attrs = make([]string, l)
	}

	for i := 0; i < int(l); i++ {
		_, bs, err := types.ReadSizeBytes(r)
		if err != nil {
			return err
		}
		bat.Attrs[i] = string(bs)
	}

	// ExtraBuf
	if _, bat.ExtraBuf, err = types.ReadSizeBytes(r); err != nil {
		return err
	}

	if bat.Recursive, err = types.ReadInt32(r); err != nil {
		return err
	}
	if bat.ShuffleIDX, err = types.ReadInt32(r); err != nil {
		return err
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

func (bat *Batch) InsertVector(
	pos int32,
	attr string,
	vec *vector.Vector,
) {
	bat.Vecs = append(bat.Vecs, nil)
	copy(bat.Vecs[pos+1:], bat.Vecs[pos:])
	bat.Vecs[pos] = vec
	bat.Attrs = append(bat.Attrs, "")
	copy(bat.Attrs[pos+1:], bat.Attrs[pos:])
	bat.Attrs[pos] = attr
}

func (bat *Batch) SetVector(pos int32, vec *vector.Vector) {
	bat.Vecs[pos] = vec
	if vec != nil {
		vec.SetOffHeap(bat.offHeap)
	}
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
	cloned.offHeap = bat.offHeap
	var typ types.Type
	for idx := range selectCols {
		cloned.Vecs[idx] = vector.NewVec(typ)
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

func (bat *Batch) GetSchema() (attrs []string, attrTypes []types.Type) {
	attrs = make([]string, len(bat.Attrs))
	attrTypes = make([]types.Type, len(bat.Vecs))
	copy(attrs, bat.Attrs)
	for i, vec := range bat.Vecs {
		attrTypes[i] = *vec.GetType()
	}
	return
}

func (bat *Batch) Clone(mp *mpool.MPool, offHeap bool) (*Batch, error) {
	var (
		cloned           *Batch
		attrs, attrTypes = bat.GetSchema()
	)
	cloned = NewWithSchema(offHeap, attrs, attrTypes)
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
	return bat.Clone(mp, bat.offHeap)
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

func (bat *Batch) PreExtend(m *mpool.MPool, rows int) error {
	for i := range bat.Vecs {
		if err := bat.Vecs[i].PreExtend(rows, m); err != nil {
			return err
		}
	}
	return nil
}

// AppendWithCopy is used to append data from batch `b` to another batch `bat`. The function
// ensures that the batch structure is consistent and copies all vector data to the target batch.
// WARING: this function will cause a memory allocation.
func (bat *Batch) AppendWithCopy(ctx context.Context, mh *mpool.MPool, b *Batch) (*Batch, error) {
	if bat == nil {
		return b.Dup(mh)
	}
	if len(bat.Vecs) != len(b.Vecs) {
		return nil, moerr.NewInternalError(ctx, "unexpected error happens in batch append")
	}
	if len(bat.Vecs) == 0 {
		return bat, nil
	}

	for i := range bat.Vecs {
		if err := bat.Vecs[i].UnionBatch(b.Vecs[i], 0, b.Vecs[i].Length(), nil, mh); err != nil {
			return bat, err
		}
		bat.Vecs[i].SetSorted(false)
	}
	bat.rowCount += b.rowCount
	return bat, nil
}

func (bat *Batch) Append(ctx context.Context, mh *mpool.MPool, b *Batch) (*Batch, error) {
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
		if err := bat.Vecs[i].UnionBatch(b.Vecs[i], 0, b.Vecs[i].Length(), nil, mh); err != nil {
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
