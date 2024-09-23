// Copyright 2022 Matrix Origin
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

package containers

import (
	"bytes"
	"fmt"
	"io"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var EMPTY_VECTOR Vector

func init() {
	EMPTY_VECTOR = &emptyVector{
		Vector: MakeVector(types.T_int8.ToType(), common.DefaultAllocator),
	}
}

type emptyVector struct {
	Vector
}

// do not close
func (v *emptyVector) Close() {}

func (v *emptyVector) Append(x any, isNull bool) {
	panic("not implemented") // TODO: Implement
}

func (v *emptyVector) Compact(_ *roaring.Bitmap) {
	panic("not implemented") // TODO: Implement
}

func (v *emptyVector) Extend(o Vector) {
	panic("not implemented") // TODO: Implement
}

func (v *emptyVector) ExtendWithOffset(src Vector, srcOff int, srcLen int) {
	panic("not implemented") // TODO: Implement
}

func NewBatch() *Batch {
	return &Batch{
		Attrs:   make([]string, 0),
		Nameidx: make(map[string]int),
		Vecs:    make([]Vector, 0),
	}
}

func NewBatchWithCapacity(cap int) *Batch {
	return &Batch{
		Attrs:   make([]string, 0, cap),
		Nameidx: make(map[string]int, cap),
		Vecs:    make([]Vector, 0, cap),
	}
}

func (bat *Batch) AddVector(attr string, vec Vector) {
	if _, exist := bat.Nameidx[attr]; exist {
		panic(moerr.NewInternalErrorNoCtxf("duplicate vector %s", attr))
	}
	idx := len(bat.Vecs)
	bat.Nameidx[attr] = idx
	bat.Attrs = append(bat.Attrs, attr)
	bat.Vecs = append(bat.Vecs, vec)
}

// AddPlaceholder is used to consctruct batch sent to CN.
// The vectors in the batch are sorted by seqnum, if the seqnum was dropped, a
// zero value will be fill as placeholder. This is space-time tradeoff.
func (bat *Batch) AppendPlaceholder() {
	bat.Attrs = append(bat.Attrs, "")
	bat.Vecs = append(bat.Vecs, EMPTY_VECTOR)
}

func (bat *Batch) GetVectorByName(name string) Vector {
	pos, ok := bat.Nameidx[name]
	if !ok {
		panic(fmt.Sprintf("vector %s not found", name))
	}
	return bat.Vecs[pos]
}

func (bat *Batch) RangeDelete(start, end int) {
	if bat.Deletes == nil {
		bat.Deletes = nulls.NewWithSize(end)
	}
	bat.Deletes.AddRange(uint64(start), uint64(end))
}

func (bat *Batch) Delete(i int) {
	if bat.Deletes == nil {
		bat.Deletes = nulls.NewWithSize(i)
	}
	bat.Deletes.Add(uint64(i))
}

func (bat *Batch) HasDelete() bool {
	return !bat.Deletes.IsEmpty()
}

func (bat *Batch) IsDeleted(i int) bool {
	return bat.Deletes.Contains(uint64(i))
}

func (bat *Batch) DeleteCnt() int {
	if !bat.HasDelete() {
		return 0
	}
	return int(bat.Deletes.GetCardinality())
}

func (bat *Batch) Compact() {
	if !bat.HasDelete() {
		return
	}
	for _, vec := range bat.Vecs {
		vec.CompactByBitmap(bat.Deletes)
	}
	bat.Deletes = nil
}

func (bat *Batch) Length() int {
	return bat.Vecs[0].Length()
}

func (bat *Batch) ApproxSize() int {
	size := 0
	for _, vec := range bat.Vecs {
		size += vec.ApproxSize()
	}
	return size
}

func (bat *Batch) Allocated() int {
	allocated := 0
	for _, vec := range bat.Vecs {
		allocated += vec.Allocated()
	}
	return allocated
}

func (bat *Batch) WindowDeletes(offset, length int, deep bool) *nulls.Bitmap {
	if bat.Deletes.IsEmpty() || length <= 0 {
		return nil
	}
	start := offset
	end := offset + length
	if end > bat.Length() {
		panic(fmt.Sprintf("out of range: %d, %d", offset, length))
	}
	if start == 0 && end == bat.Length() && !deep {
		return bat.Deletes
	}
	ret := nulls.NewWithSize(length)
	nulls.Range(bat.Deletes, uint64(start), uint64(end), uint64(start), ret)
	return ret
}

func (bat *Batch) Window(offset, length int) *Batch {
	win := new(Batch)
	win.Attrs = bat.Attrs
	win.Nameidx = bat.Nameidx
	win.Deletes = bat.WindowDeletes(offset, length, false)
	win.Vecs = make([]Vector, len(bat.Vecs))
	for i := range win.Vecs {
		win.Vecs[i] = bat.Vecs[i].Window(offset, length)
	}
	return win
}

func (bat *Batch) CloneWindowWithPool(offset, length int, pool *VectorPool) (cloned *Batch) {
	cloned = new(Batch)
	cloned.Attrs = make([]string, len(bat.Attrs))
	copy(cloned.Attrs, bat.Attrs)
	cloned.Nameidx = make(map[string]int, len(bat.Nameidx))
	for k, v := range bat.Nameidx {
		cloned.Nameidx[k] = v
	}
	cloned.Deletes = bat.WindowDeletes(offset, length, true)
	cloned.Vecs = make([]Vector, len(bat.Vecs))
	for i := range cloned.Vecs {
		cloned.Vecs[i] = bat.Vecs[i].CloneWindowWithPool(offset, length, pool)
	}
	return
}

func (bat *Batch) CloneWindow(offset, length int, allocator ...*mpool.MPool) (cloned *Batch) {
	cloned = new(Batch)
	cloned.Attrs = make([]string, len(bat.Attrs))
	copy(cloned.Attrs, bat.Attrs)
	cloned.Nameidx = make(map[string]int, len(bat.Nameidx))
	for k, v := range bat.Nameidx {
		cloned.Nameidx[k] = v
	}
	cloned.Deletes = bat.WindowDeletes(offset, length, true)
	cloned.Vecs = make([]Vector, len(bat.Vecs))
	for i := range cloned.Vecs {
		cloned.Vecs[i] = bat.Vecs[i].CloneWindow(offset, length, allocator...)
	}
	return
}

func (bat *Batch) String() string {
	return bat.PPString(10)
}

func (bat *Batch) PPString(num int) string {
	var w bytes.Buffer
	for i, vec := range bat.Vecs {
		_, _ = w.WriteString(fmt.Sprintf("[Name=%s]", bat.Attrs[i]))
		_, _ = w.WriteString(vec.PPString(num))
		_ = w.WriteByte('\n')
	}
	return w.String()
}

func (bat *Batch) Close() {
	for i, vec := range bat.Vecs {
		if vec != nil {
			vec.Close()
			bat.Vecs[i] = nil
		}
	}
}

func (bat *Batch) Reset() {
	for i, vec := range bat.Vecs {
		var newVec Vector
		if bat.Pool != nil {
			newVec = bat.Pool.GetVector(vec.GetType())
		} else {
			opts := Options{
				Allocator: vec.GetAllocator(),
			}
			newVec = NewVector(*vec.GetType(), opts)
		}
		vec.Close()
		bat.Vecs[i] = newVec
	}
	bat.Deletes = nil
}

func (bat *Batch) Equals(o *Batch) bool {
	if bat.Length() != o.Length() {
		return false
	}
	if bat.DeleteCnt() != o.DeleteCnt() {
		return false
	}
	if !common.BitmapEqual(bat.Deletes, o.Deletes) {
		return false
	}
	for i := range bat.Vecs {
		if bat.Attrs[i] != o.Attrs[i] {
			return false
		}
		if !bat.Vecs[i].Equals(o.Vecs[i]) {
			return false
		}
	}
	return true
}

func (bat *Batch) WriteTo(w io.Writer) (n int64, err error) {
	var nr int
	var tmpn int64
	var buffer Vector
	if bat.Pool != nil {
		t := types.T_varchar.ToType()
		buffer = bat.Pool.GetVector(&t)
	} else {
		buffer = MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	}
	defer buffer.Close()
	mp := buffer.GetAllocator()
	bufVec := buffer.GetDownstreamVector()
	if err = vector.AppendBytes(bufVec, types.EncodeFixed(uint16(len(bat.Vecs))), false, mp); err != nil {
		return
	}

	// 2. Types and Names
	for i, vec := range bat.Vecs {
		if err = vector.AppendBytes(bufVec, []byte(bat.Attrs[i]), false, mp); err != nil {
			return
		}
		vt := vec.GetType()
		if err = vector.AppendBytes(bufVec, types.EncodeType(vt), false, mp); err != nil {
			return
		}
	}
	if tmpn, err = buffer.WriteTo(w); err != nil {
		return
	}
	n += tmpn

	// 3. Vectors
	for _, vec := range bat.Vecs {
		if tmpn, err = vec.WriteTo(w); err != nil {
			return
		}
		n += tmpn
	}
	// 4. Deletes
	var buf []byte
	if bat.Deletes != nil {
		if buf, err = bat.Deletes.Show(); err != nil {
			return
		}
	}
	if nr, err = w.Write(types.EncodeFixed(uint32(len(buf)))); err != nil {
		return
	}
	n += int64(nr)
	if len(buf) == 0 {
		return
	}
	if nr, err = w.Write(buf); err != nil {
		return
	}
	n += int64(nr)

	return
}

// WriteToV2 in version 2, vector.Nulls.Bitmap is v1
func (bat *Batch) WriteToV2(w io.Writer) (n int64, err error) {
	var nr int
	var tmpn int64
	var buffer Vector
	if bat.Pool != nil {
		t := types.T_varchar.ToType()
		buffer = bat.Pool.GetVector(&t)
	} else {
		buffer = MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	}
	defer buffer.Close()
	mp := buffer.GetAllocator()
	bufVec := buffer.GetDownstreamVector()
	if err = vector.AppendBytes(bufVec, types.EncodeFixed(uint16(len(bat.Vecs))), false, mp); err != nil {
		return
	}

	// 2. Types and Names
	for i, vec := range bat.Vecs {
		if err = vector.AppendBytes(bufVec, []byte(bat.Attrs[i]), false, mp); err != nil {
			return
		}
		vt := vec.GetType()
		if err = vector.AppendBytes(bufVec, types.EncodeType(vt), false, mp); err != nil {
			return
		}
	}
	if tmpn, err = buffer.WriteToV1(w); err != nil {
		return
	}
	n += tmpn

	// 3. Vectors
	for _, vec := range bat.Vecs {
		if tmpn, err = vec.WriteToV1(w); err != nil {
			return
		}
		n += tmpn
	}
	// 4. Deletes
	var buf []byte
	if bat.Deletes != nil {
		if buf, err = bat.Deletes.ShowV1(); err != nil {
			return
		}
	}
	if nr, err = w.Write(types.EncodeFixed(uint32(len(buf)))); err != nil {
		return
	}
	n += int64(nr)
	if len(buf) == 0 {
		return
	}
	if nr, err = w.Write(buf); err != nil {
		return
	}
	n += int64(nr)

	return
}

func (bat *Batch) ReadFrom(r io.Reader) (n int64, err error) {
	var tmpn int64
	buffer := MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	defer buffer.Close()
	if tmpn, err = buffer.ReadFrom(r); err != nil {
		return
	}
	n += tmpn
	pos := 0
	buf := buffer.Get(pos).([]byte)
	pos++
	cnt := types.DecodeFixed[uint16](buf)
	vecTypes := make([]types.Type, cnt)
	bat.Attrs = make([]string, cnt)
	for i := 0; i < int(cnt); i++ {
		buf = buffer.Get(pos).([]byte)
		pos++
		bat.Attrs[i] = string(buf)
		bat.Nameidx[bat.Attrs[i]] = i
		buf = buffer.Get(pos).([]byte)
		vecTypes[i] = types.DecodeType(buf)
		pos++
	}
	for _, vecType := range vecTypes {
		vec := MakeVector(vecType, common.DefaultAllocator)
		if tmpn, err = vec.ReadFrom(r); err != nil {
			return
		}
		bat.Vecs = append(bat.Vecs, vec)
		n += tmpn
	}
	// XXX Fix the following read, it is a very twisted way of reading uint32.
	// Read Deletes
	buf = make([]byte, int(unsafe.Sizeof(uint32(0))))
	if _, err = r.Read(buf); err != nil {
		return
	}
	n += int64(len(buf))
	size := types.DecodeFixed[uint32](buf)
	if size == 0 {
		return
	}
	bat.Deletes = &nulls.Bitmap{}
	buf = make([]byte, size)
	if _, err = r.Read(buf); err != nil {
		return
	}
	if err = bat.Deletes.ReadNoCopy(buf); err != nil {
		return
	}
	n += int64(size)

	return
}

// in version1, batch.Deletes is roaring.Bitmap
func (bat *Batch) ReadFromV1(r io.Reader) (n int64, err error) {
	var tmpn int64
	buffer := MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	defer buffer.Close()
	if tmpn, err = buffer.ReadFromV1(r); err != nil {
		return
	}
	n += tmpn
	pos := 0
	buf := buffer.Get(pos).([]byte)
	pos++
	cnt := types.DecodeFixed[uint16](buf)
	vecTypes := make([]types.Type, cnt)
	bat.Attrs = make([]string, cnt)
	for i := 0; i < int(cnt); i++ {
		buf = buffer.Get(pos).([]byte)
		pos++
		bat.Attrs[i] = string(buf)
		bat.Nameidx[bat.Attrs[i]] = i
		buf = buffer.Get(pos).([]byte)
		vecTypes[i] = types.DecodeType(buf)
		pos++
	}
	for _, vecType := range vecTypes {
		vec := MakeVector(vecType, common.DefaultAllocator)
		if tmpn, err = vec.ReadFromV1(r); err != nil {
			return
		}
		bat.Vecs = append(bat.Vecs, vec)
		n += tmpn
	}
	// XXX Fix the following read, it is a very twisted way of reading uint32.
	// Read Deletes
	buf = make([]byte, int(unsafe.Sizeof(uint32(0))))
	if _, err = r.Read(buf); err != nil {
		return
	}
	n += int64(len(buf))
	size := types.DecodeFixed[uint32](buf)
	if size == 0 {
		return
	}
	deletes := roaring.New()
	if tmpn, err = deletes.ReadFrom(r); err != nil {
		return
	}
	n += tmpn
	bat.Deletes = common.RoaringToMOBitmap(deletes)

	return
}

// ReadFromV2 in version2, vector.nulls.bitmap is v1
func (bat *Batch) ReadFromV2(r io.Reader) (n int64, err error) {
	var tmpn int64
	buffer := MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	defer buffer.Close()
	if tmpn, err = buffer.ReadFromV1(r); err != nil {
		return
	}
	n += tmpn
	pos := 0
	buf := buffer.Get(pos).([]byte)
	pos++
	cnt := types.DecodeFixed[uint16](buf)
	vecTypes := make([]types.Type, cnt)
	bat.Attrs = make([]string, cnt)
	for i := 0; i < int(cnt); i++ {
		buf = buffer.Get(pos).([]byte)
		pos++
		bat.Attrs[i] = string(buf)
		bat.Nameidx[bat.Attrs[i]] = i
		buf = buffer.Get(pos).([]byte)
		vecTypes[i] = types.DecodeType(buf)
		pos++
	}
	for _, vecType := range vecTypes {
		vec := MakeVector(vecType, common.DefaultAllocator)
		if tmpn, err = vec.ReadFromV1(r); err != nil {
			return
		}
		bat.Vecs = append(bat.Vecs, vec)
		n += tmpn
	}
	// XXX Fix the following read, it is a very twisted way of reading uint32.
	// Read Deletes
	buf = make([]byte, int(unsafe.Sizeof(uint32(0))))
	if _, err = r.Read(buf); err != nil {
		return
	}
	n += int64(len(buf))
	size := types.DecodeFixed[uint32](buf)
	if size == 0 {
		return
	}
	bat.Deletes = &nulls.Bitmap{}
	buf = make([]byte, size)
	if _, err = r.Read(buf); err != nil {
		return
	}
	if err = bat.Deletes.ReadNoCopyV1(buf); err != nil {
		return
	}
	n += int64(size)

	return
}

func (bat *Batch) Split(cnt int) []*Batch {
	if cnt == 1 {
		return []*Batch{bat}
	}
	length := bat.Length()
	rows := length / cnt
	if length%cnt == 0 {
		bats := make([]*Batch, 0, cnt)
		for i := 0; i < cnt; i++ {
			newBat := bat.Window(i*rows, rows)
			bats = append(bats, newBat)
		}
		return bats
	}
	rowArray := make([]int, 0)
	if length/cnt == 0 {
		for i := 0; i < length; i++ {
			rowArray = append(rowArray, 1)
		}
	} else {
		left := length
		for i := 0; i < cnt; i++ {
			if left >= rows && i < cnt-1 {
				rowArray = append(rowArray, rows)
			} else {
				rowArray = append(rowArray, left)
			}
			left -= rows
		}
	}
	start := 0
	bats := make([]*Batch, 0, cnt)
	for _, row := range rowArray {
		newBat := bat.Window(start, row)
		start += row
		bats = append(bats, newBat)
	}
	return bats
}

func (bat *Batch) Append(src *Batch) (err error) {
	for i, vec := range bat.Vecs {
		vec.Extend(src.Vecs[i])
	}
	return
}

// extend vector with same name, consume src batch
func (bat *Batch) Extend(src *Batch) {
	for i, vec := range bat.Vecs {
		attr := bat.Attrs[i]
		if idx, ok := src.Nameidx[attr]; ok {
			vec.Extend(src.Vecs[idx])
		}
	}
	src.Close()
}

func (b *BatchWithVersion) Len() int {
	return len(b.Seqnums)
}

func (b *BatchWithVersion) Swap(i, j int) {
	b.Seqnums[i], b.Seqnums[j] = b.Seqnums[j], b.Seqnums[i]
	b.Attrs[i], b.Attrs[j] = b.Attrs[j], b.Attrs[i]
	b.Vecs[i], b.Vecs[j] = b.Vecs[j], b.Vecs[i]
}

// Sort by seqnum
func (b *BatchWithVersion) Less(i, j int) bool {
	return b.Seqnums[i] < b.Seqnums[j]
}

func NewBatchSplitter(bat *Batch, sliceSize int) *BatchSplitter {
	if sliceSize <= 0 || bat == nil {
		panic("sliceSize should not be 0 and bat should not be nil")
	}
	return &BatchSplitter{
		internal:  bat,
		sliceSize: sliceSize,
	}
}

func (bs *BatchSplitter) Next() (*Batch, error) {
	if bs.offset == bs.internal.Length() {
		return nil, moerr.GetOkExpectedEOB()
	}
	length := bs.sliceSize
	nextOffset := bs.offset + bs.sliceSize
	if nextOffset >= bs.internal.Length() {
		nextOffset = bs.internal.Length()
		length = nextOffset - bs.offset
	}
	bat := bs.internal.CloneWindow(bs.offset, length)
	bs.offset = nextOffset
	return bat, nil
}
