// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"bytes"
	io "io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// MergeSplitResult turns the physical chunks returned by AggFuncExec.Flush
// into one logical result vector. The chunks are the aggregate's vectors, all
// created with the aggregate's pool; ownership passes to the caller, so the
// merge works through that pool (the first chunk's) and the strict mpool
// check rejects a chunk of any other pool. It consumes every input vector on all paths:
// on success the caller owns only the returned vector, and on failure all input
// vectors have been freed.
//
// The common single-chunk path is zero-copy. For multiple chunks, the first
// vector is reused and grown once before the remaining chunks are appended in
// order. UnionBatch's full-vector path preserves nulls and copies varlen areas
// in bulk.
func MergeSplitResult(vecs []*vector.Vector) (*vector.Vector, error) {
	if len(vecs) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("aggregate returned no result vectors")
	}

	result := vecs[0]
	if len(vecs) == 1 {
		if result == nil {
			return nil, moerr.NewInternalErrorNoCtx("aggregate returned a nil result vector")
		}
		return result, nil
	}
	if result == nil {
		for _, vec := range vecs {
			if vec != nil {
				// Ownership passed with the chunk: free it with its pool.
				vec.Free(vec.MPool())
			}
		}
		return nil, moerr.NewInternalErrorNoCtx("aggregate returned a nil result vector")
	}
	// The aggregate's pool, which every chunk shares.
	mp := result.MPool()

	additionalRows, additionalArea := 0, 0
	resultType := *result.GetType()
	for i := 1; i < len(vecs); i++ {
		if vecs[i] == nil {
			freeAggResultVectors(vecs, mp)
			return nil, moerr.NewInternalErrorNoCtxf("aggregate returned a nil result vector at chunk %d", i)
		}
		if !resultType.Eq(*vecs[i].GetType()) {
			err := moerr.NewInternalErrorNoCtxf(
				"aggregate returned inconsistent result types %s and %s",
				resultType.String(), vecs[i].GetType().String())
			freeAggResultVectors(vecs, mp)
			return nil, err
		}
		additionalRows += vecs[i].Length()
		additionalArea += len(vecs[i].GetArea())
	}

	if err := result.PreExtendWithArea(additionalRows, additionalArea, mp); err != nil {
		freeAggResultVectors(vecs, mp)
		return nil, err
	}
	for i := 1; i < len(vecs); i++ {
		source := vecs[i]
		if err := result.UnionBatch(source, 0, source.Length(), nil, mp); err != nil {
			result.Free(mp)
			for j := i; j < len(vecs); j++ {
				vecs[j].Free(mp)
			}
			return nil, err
		}
		source.Free(mp)
	}
	return result, nil
}

func freeAggResultVectors(vecs []*vector.Vector, mp *mpool.MPool) {
	for _, vec := range vecs {
		if vec != nil {
			vec.Free(mp)
		}
	}
}

func FromD64ToD128(v types.Decimal64) types.Decimal128 {
	k := types.Decimal128{
		B0_63:   uint64(v),
		B64_127: 0,
	}
	if v.Sign() {
		k.B64_127 = ^k.B64_127
	}
	return k
}

func modifyChunkSizeOfAggregator(a AggFuncExec, n int) {
	r := a.GetOptResult()
	if r != nil {
		r.modifyChunkSize(n)
	}
}

func SyncAggregatorsToChunkSize[T AggFuncExec](as []T, syncLimit int) {
	for _, a := range as {
		modifyChunkSizeOfAggregator(a, syncLimit)
	}
}

type Vectors[T numeric | types.Decimal64 | types.Decimal128 | types.Decimal256] struct {
	vecs []*vector.Vector
	// allocation is the accounted MPool view the vectors grow through, or nil
	// when they grow through the caller's pool.
	allocation *mpool.MPool
	appendAt   int
}

// growMP returns the pool vs grows through: its accounted view, else mp.
func (vs *Vectors[T]) growMP(mp *mpool.MPool) *mpool.MPool {
	if vs.allocation != nil {
		return vs.allocation
	}
	return mp
}

const (
	MaxVectorLength = 262144
)

func NewVectors[T numeric | types.Decimal64 | types.Decimal128 | types.Decimal256](typ types.Type, mp *mpool.MPool) *Vectors[T] {
	vec := vector.NewVec(typ, mp)
	return &Vectors[T]{vecs: []*vector.Vector{vec}}
}

// newAccountedVectors creates vectors that grow through view, an accounted
// view of the owning executor's pool.
func newAccountedVectors[T numeric | types.Decimal64 | types.Decimal128 | types.Decimal256](
	typ types.Type,
	view *mpool.MPool,
) (*Vectors[T], error) {
	if !view.IsView() {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	return &Vectors[T]{
		vecs:       []*vector.Vector{vector.NewVec(typ, view)},
		allocation: view,
	}, nil
}

func (vs *Vectors[T]) MarshalBinary() ([]byte, error) {
	var bbuf bytes.Buffer
	length := int64(len(vs.vecs))
	if _, err := bbuf.Write(types.EncodeInt64(&length)); err != nil {
		return nil, err
	}
	for _, v := range vs.vecs {
		var buf []byte
		var err error
		if buf, err = v.MarshalBinary(); err != nil {
			return nil, err
		}
		if _, err := WriteBytes(buf, &bbuf); err != nil {
			return nil, err
		}
	}
	return bbuf.Bytes(), nil
}

func (vs *Vectors[T]) Unmarshal(data []byte, typ types.Type, mp *mpool.MPool) error {
	return vs.UnmarshalFromReader(bytes.NewReader(data), typ, mp)
}

func (vs *Vectors[T]) UnmarshalFromReader(
	r io.Reader,
	typ types.Type,
	mp *mpool.MPool,
) (retErr error) {
	mp = vs.growMP(mp)
	length := int64(0)
	if _, err := io.ReadFull(r, types.EncodeInt64(&length)); err != nil {
		return err
	}
	if length < 0 {
		return moerr.NewInvalidInputNoCtxf(
			"invalid aggregate vector count %d", length)
	}
	start := len(vs.vecs)
	defer func() {
		if retErr != nil {
			for _, vec := range vs.vecs[start:] {
				vec.Free(mp)
			}
			vs.vecs = vs.vecs[:start]
		}
	}()
	for i := int64(0); i < length; i++ {
		sz, err := types.ReadUint32(r)
		if err != nil {
			return err
		}
		lr := &io.LimitedReader{R: r, N: int64(sz)}
		vec := vector.NewVec(typ, mp)
		if err := unmarshalAggregateVector(vec, lr, mp); err != nil {
			vec.Free(mp)
			return err
		}
		if _, err := io.Copy(io.Discard, lr); err != nil {
			vec.Free(mp)
			return err
		}
		if lr.N != 0 {
			vec.Free(mp)
			return io.ErrUnexpectedEOF
		}
		vs.vecs = append(vs.vecs, vec)
	}
	if len(vs.vecs) != 0 {
		vs.appendAt = len(vs.vecs) - 1
		if vs.vecs[vs.appendAt].Length() >= MaxVectorLength {
			vs.appendAt++
		}
	}
	return nil
}

func (vs *Vectors[T]) Length() int {
	length := 0
	for _, v := range vs.vecs {
		length += v.Length()
	}
	return length
}

func (vs *Vectors[T]) getAppendableVector() (*vector.Vector, error) {
	for vs.appendAt < len(vs.vecs) &&
		vs.vecs[vs.appendAt].Length() >= MaxVectorLength {
		vs.appendAt++
	}
	if vs.appendAt == len(vs.vecs) {
		// A new chunk lives in the pool of the existing chunks.
		vs.vecs = append(vs.vecs, vector.NewVec(*vs.vecs[0].GetType(), vs.vecs[0].MPool()))
	}
	return vs.vecs[vs.appendAt], nil
}

// PreExtend reserves physical fixed-width storage for rows without publishing
// any logical values. Accounted median uses it as the allocation boundary for
// a whole work unit, so the following append loop cannot fail part-way through
// after exposing only a prefix of the input.
func (vs *Vectors[T]) PreExtend(rows int, mp *mpool.MPool) error {
	if vs == nil || rows < 0 || len(vs.vecs) == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if rows == 0 {
		return nil
	}
	remaining := rows
	for index := vs.appendAt; remaining > 0; index++ {
		if index == len(vs.vecs) {
			vs.vecs = append(vs.vecs, vector.NewVec(*vs.vecs[0].GetType(), vs.growMP(mp)))
		}
		vec := vs.vecs[index]
		available := MaxVectorLength - vec.Length()
		reserve := min(remaining, available)
		if reserve > 0 {
			if err := vec.PreExtend(reserve, vs.growMP(mp)); err != nil {
				return err
			}
			remaining -= reserve
		}
	}
	return nil
}

// clone other
func (vs *Vectors[T]) Union(other *Vectors[T], mp *mpool.MPool) error {
	if len(other.vecs) == 0 {
		return nil
	}
	for _, vec := range other.vecs {
		vals := vector.MustFixedColWithTypeCheck[T](vec)
		left := len(vals)
		for {
			vec, err := vs.getAppendableVector()
			if err != nil {
				return err
			}
			appendableCount := MaxVectorLength - vec.Length()
			if appendableCount > left {
				appendableCount = left
			}

			if err := vector.AppendFixedList(vec, vals[:appendableCount], nil, vs.growMP(mp)); err != nil {
				return err
			}
			left -= appendableCount
			vals = vals[appendableCount:]
			if left == 0 {
				break
			}
		}
	}

	return nil
}

func (vs *Vectors[T]) Free(mp *mpool.MPool) {
	mp = vs.growMP(mp)
	for _, vec := range vs.vecs {
		vec.Free(mp)
	}
}

func (vs *Vectors[T]) Size() int64 {
	var size int64
	for _, vec := range vs.vecs {
		size += int64(vec.Allocated())
	}
	// 8 is the size of a pointer.
	size += int64(cap(vs.vecs)) * 8
	return size
}

func AppendMultiFixed[T numeric | types.Decimal64 | types.Decimal128 | types.Decimal256](vecs *Vectors[T], vals T, isNull bool, cnt int, mp *mpool.MPool) error {
	leftRow := cnt
	for {
		vec, err := vecs.getAppendableVector()
		if err != nil {
			return err
		}
		appendCnt := MaxVectorLength - vec.Length()
		if appendCnt > leftRow {
			appendCnt = leftRow
		}
		if err := vector.AppendMultiFixed(vec, vals, isNull, appendCnt, vecs.growMP(mp)); err != nil {
			return err
		}
		leftRow -= appendCnt
		if leftRow == 0 {
			break
		}
	}
	return nil
}

func WriteBytes(b []byte, w io.Writer) (n int64, err error) {
	size := uint32(len(b))
	written, err := w.Write(types.EncodeUint32(&size))
	if err != nil {
		return
	}
	if written != 4 {
		return int64(written), io.ErrShortWrite
	}
	wn, err := w.Write(b)
	if err == nil && wn != len(b) {
		err = io.ErrShortWrite
	}
	return int64(wn + written), err
}

// writeBytesRaw writes b without a length prefix and rejects writers that
// report success after consuming only a prefix.
func writeBytesRaw(b []byte, w io.Writer) (n int, err error) {
	n, err = w.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return n, err
}
