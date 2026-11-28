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

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

// EMPTY_VECTOR is the shared placeholder AppendPlaceholder puts in a batch.
// It is never written to, and ReleaseVector leaves it alone.
var EMPTY_VECTOR Vector = vector.NewVec(types.T_int8.ToType(), common.DefaultAllocator)

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

// deletesMP is the pool a delete bitmap of bat allocates from: its vectors'
// allocator, or TAE's default allocator for a batch without vectors.
func (bat *Batch) deletesMP() *mpool.MPool {
	for _, vec := range bat.Vecs {
		if vec != nil {
			if mp := vec.MPool(); mp != nil {
				return mp
			}
		}
	}
	return common.DefaultAllocator
}

// newDeletes returns an empty owned delete bitmap of size rows.
func (bat *Batch) newDeletes(size int) *nulls.Nulls {
	deletes, err := nulls.NewWithSize(size, bat.deletesMP())
	if err != nil {
		panic(err)
	}
	return deletes
}

// ReleaseDeletes frees bat's delete bitmap (a view only drops its reference)
// and clears it. A batch owns its Deletes: every replacement of Deletes must
// release the old one first.
func (bat *Batch) ReleaseDeletes() {
	if bat.Deletes != nil {
		bat.Deletes.Free()
		bat.Deletes = nil
	}
}

// SetDeletes makes deletes bat's delete bitmap, releasing the old one. bat
// takes ownership of deletes.
func (bat *Batch) SetDeletes(deletes *nulls.Nulls) {
	if bat.Deletes != deletes {
		bat.ReleaseDeletes()
	}
	bat.Deletes = deletes
}

func (bat *Batch) RangeDelete(start, end int) {
	if bat.Deletes == nil {
		bat.Deletes = bat.newDeletes(end)
	}
	bat.Deletes.AddRange(uint64(start), uint64(end))
}

func (bat *Batch) Delete(i int) {
	if bat.Deletes == nil {
		bat.Deletes = bat.newDeletes(i)
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
	return int(bat.Deletes.Count())
}

func (bat *Batch) Compact() {
	if !bat.HasDelete() {
		return
	}
	dels := make([]int64, 0, bat.Deletes.Count())
	bat.Deletes.Foreach(func(i uint64) bool {
		dels = append(dels, int64(i))
		return true
	})
	for _, vec := range bat.Vecs {
		if err := vec.Shrink(dels, true, vec.MPool()); err != nil {
			panic(err)
		}
	}
	bat.ReleaseDeletes()
}

func (bat *Batch) Length() int {
	return bat.Vecs[0].Length()
}

func (bat *Batch) ApproxSize() int {
	size := 0
	for _, vec := range bat.Vecs {
		size += approxSize(vec)
	}
	return size
}

func (bat *Batch) Allocated() int {
	allocated := 0
	for _, vec := range bat.Vecs {
		allocated += vectorAllocated(vec)
	}
	return allocated
}

// WindowDeletes returns the deletes of rows [offset, offset+length): a
// read-only view of bat.Deletes, or with deep an owned copy. Either way the
// result is a separate Nulls the caller releases with Free.
func (bat *Batch) WindowDeletes(offset, length int, deep bool) *nulls.Nulls {
	if bat.Deletes.IsEmpty() || length <= 0 {
		return nil
	}
	start := offset
	end := offset + length
	if end > bat.Length() {
		panic(fmt.Sprintf("out of range: %d, %d", offset, length))
	}
	ret := new(nulls.Nulls)
	if !deep {
		ret.InitView(bat.Deletes, start, end)
		return ret
	}
	if err := nulls.Range(bat.Deletes, uint64(start), uint64(end), uint64(start), ret, bat.deletesMP()); err != nil {
		panic(err)
	}
	return ret
}

func (bat *Batch) Window(offset, length int) *Batch {
	win := new(Batch)
	win.Attrs = bat.Attrs
	win.Nameidx = bat.Nameidx
	win.Deletes = bat.WindowDeletes(offset, length, false)
	win.Vecs = make([]Vector, len(bat.Vecs))
	for i := range win.Vecs {
		vec, err := bat.Vecs[i].Window(offset, offset+length)
		if err != nil {
			panic(err)
		}
		win.Vecs[i] = vec
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
		vec, err := pool.CloneWindow(bat.Vecs[i], offset, length)
		if err != nil {
			panic(err)
		}
		cloned.Vecs[i] = vec
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
		mp := common.DefaultAllocator
		if len(allocator) > 0 {
			mp = allocator[0]
		} else if vmp := bat.Vecs[i].MPool(); vmp != nil {
			mp = vmp
		}
		vec, err := bat.Vecs[i].CloneWindow(offset, offset+length, mp)
		if err != nil {
			panic(err)
		}
		cloned.Vecs[i] = vec
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
		_, _ = w.WriteString(ppString(vec, num))
		_ = w.WriteByte('\n')
	}
	return w.String()
}

func (bat *Batch) Close() {
	bat.ReleaseDeletes()
	if bat.DataRelease != nil {
		bat.DataRelease()
		bat.DataRelease = nil
	}
	for i, vec := range bat.Vecs {
		if vec != nil {
			ReleaseVector(vec)
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
			mp := vec.MPool()
			if mp == nil {
				mp = common.DefaultAllocator
			}
			newVec = vector.NewVec(*vec.GetType(), mp)
		}
		ReleaseVector(vec)
		bat.Vecs[i] = newVec
	}
	bat.ReleaseDeletes()
}

func (bat *Batch) CleanOnlyData() {
	bat.ReleaseDeletes()
	for i := range bat.Vecs {
		bat.Vecs[i].CleanOnlyData()
	}
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
		if !bat.Vecs[i].Equal(o.Vecs[i]) {
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
	defer ReleaseVector(buffer)
	mp := buffer.MPool()
	bufVec := buffer
	if err = vector.AppendBytes(bufVec, types.EncodeFixed(uint16(len(bat.Vecs))), false, mp); err != nil {
		return
	}

	// 2. Types and Names
	for i, vec := range bat.Vecs {
		// Use UnsafeStringToBytes to avoid allocation: string -> []byte conversion
		// This is safe because:
		// 1. bat.Attrs[i] is valid during Batch's lifetime
		// 2. vector.AppendBytes copies data to vector, doesn't depend on buf's lifetime
		if err = vector.AppendBytes(bufVec, util.UnsafeStringToBytes(bat.Attrs[i]), false, mp); err != nil {
			return
		}
		vt := vec.GetType()
		if err = vector.AppendBytes(bufVec, types.EncodeType(vt), false, mp); err != nil {
			return
		}
	}
	if tmpn, err = writeVector(buffer, w); err != nil {
		return
	}
	n += tmpn

	// 3. Vectors
	for _, vec := range bat.Vecs {
		if tmpn, err = writeVector(vec, w); err != nil {
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
	defer ReleaseVector(buffer)
	mp := buffer.MPool()
	bufVec := buffer
	if err = vector.AppendBytes(bufVec, types.EncodeFixed(uint16(len(bat.Vecs))), false, mp); err != nil {
		return
	}

	// 2. Types and Names
	for i, vec := range bat.Vecs {
		// Use UnsafeStringToBytes to avoid allocation: string -> []byte conversion
		// This is safe because:
		// 1. bat.Attrs[i] is valid during Batch's lifetime
		// 2. vector.AppendBytes copies data to vector, doesn't depend on buf's lifetime
		if err = vector.AppendBytes(bufVec, util.UnsafeStringToBytes(bat.Attrs[i]), false, mp); err != nil {
			return
		}
		vt := vec.GetType()
		if err = vector.AppendBytes(bufVec, types.EncodeType(vt), false, mp); err != nil {
			return
		}
	}
	if tmpn, err = writeVectorV1(buffer, w); err != nil {
		return
	}
	n += tmpn

	// 3. Vectors
	for _, vec := range bat.Vecs {
		if tmpn, err = writeVectorV1(vec, w); err != nil {
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
	defer ReleaseVector(buffer)
	if tmpn, err = readVector(buffer, r); err != nil {
		return
	}
	if err = validateBatchTypeMetadata(buffer); err != nil {
		return
	}
	n += tmpn
	pos := 0
	buf := buffer.GetValue(pos).([]byte)
	pos++
	cnt := types.DecodeFixed[uint16](buf)
	vecTypes := make([]types.Type, cnt)
	bat.Attrs = make([]string, cnt)
	for i := 0; i < int(cnt); i++ {
		buf = buffer.GetValue(pos).([]byte)
		pos++
		bat.Attrs[i] = string(buf)
		bat.Nameidx[bat.Attrs[i]] = i
		buf = buffer.GetValue(pos).([]byte)
		vecTypes[i] = types.DecodeType(buf)
		pos++
	}
	for _, vecType := range vecTypes {
		vec := MakeVector(vecType, common.DefaultAllocator)
		if tmpn, err = readVector(vec, r); err != nil {
			ReleaseVector(vec)
			return
		}
		bat.Vecs = append(bat.Vecs, vec)
		n += tmpn
	}
	// XXX Fix the following read, it is a very twisted way of reading uint32.
	// Read Deletes
	buf = make([]byte, int(unsafe.Sizeof(uint32(0))))
	if _, err = io.ReadFull(r, buf); err != nil {
		return
	}
	n += int64(len(buf))
	size := types.DecodeFixed[uint32](buf)
	if size == 0 {
		return
	}
	buf = make([]byte, size)
	if _, err = io.ReadFull(r, buf); err != nil {
		return
	}
	bat.SetDeletes(&nulls.Nulls{})
	bat.Deletes.Init(bat.deletesMP())
	if err = bat.Deletes.Read(buf); err != nil {
		return
	}
	n += int64(size)

	return
}

// Validate the shared name/type envelope before any versioned reader allocates
// vectors. The unchecked copies below consume only these validated slots.
func validateBatchTypeMetadata(buffer Vector) error {
	if buffer.Length() == 0 {
		return moerr.NewInvalidInputNoCtx("missing batch type metadata")
	}
	countBytes, ok := buffer.GetValue(0).([]byte)
	if !ok || len(countBytes) != 2 {
		return moerr.NewInvalidInputNoCtx("invalid batch type count")
	}
	count := int(types.DecodeFixed[uint16](countBytes))
	if buffer.Length() < 1+2*count {
		return moerr.NewInvalidInputNoCtx("short batch type metadata")
	}
	for i := 0; i < count; i++ {
		if _, ok := buffer.GetValue(1 + 2*i).([]byte); !ok || buffer.IsNull(uint64(1+2*i)) {
			return moerr.NewInvalidInputNoCtx("invalid batch column name")
		}
		data, ok := buffer.GetValue(2 + 2*i).([]byte)
		if !ok || buffer.IsNull(uint64(2+2*i)) {
			return moerr.NewInvalidInputNoCtx("invalid batch type metadata")
		}
		if _, err := types.DecodeTypeChecked(data); err != nil {
			return err
		}
	}
	return nil
}

// in version1, batch.Deletes is roaring.Bitmap
func (bat *Batch) ReadFromV1(r io.Reader) (n int64, err error) {
	var tmpn int64
	buffer := MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	defer ReleaseVector(buffer)
	if tmpn, err = readVectorV1(buffer, r); err != nil {
		return
	}
	if err = validateBatchTypeMetadata(buffer); err != nil {
		return
	}
	n += tmpn
	pos := 0
	buf := buffer.GetValue(pos).([]byte)
	pos++
	cnt := types.DecodeFixed[uint16](buf)
	vecTypes := make([]types.Type, cnt)
	bat.Attrs = make([]string, cnt)
	for i := 0; i < int(cnt); i++ {
		buf = buffer.GetValue(pos).([]byte)
		pos++
		bat.Attrs[i] = string(buf)
		bat.Nameidx[bat.Attrs[i]] = i
		buf = buffer.GetValue(pos).([]byte)
		vecTypes[i] = types.DecodeType(buf)
		pos++
	}
	for _, vecType := range vecTypes {
		vec := MakeVector(vecType, common.DefaultAllocator)
		if tmpn, err = readVectorV1(vec, r); err != nil {
			ReleaseVector(vec)
			return
		}
		bat.Vecs = append(bat.Vecs, vec)
		n += tmpn
	}
	// XXX Fix the following read, it is a very twisted way of reading uint32.
	// Read Deletes
	buf = make([]byte, int(unsafe.Sizeof(uint32(0))))
	if _, err = io.ReadFull(r, buf); err != nil {
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
	var nbm *nulls.Nulls
	if nbm, err = common.RoaringToMOBitmap(deletes, bat.deletesMP()); err != nil {
		return
	}
	bat.SetDeletes(nbm)

	return
}

// ReadFromV2 in version2, vector.nulls.bitmap is v1
func (bat *Batch) ReadFromV2(r io.Reader) (n int64, err error) {
	var tmpn int64
	buffer := MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	defer ReleaseVector(buffer)
	if tmpn, err = readVectorV1(buffer, r); err != nil {
		return
	}
	if err = validateBatchTypeMetadata(buffer); err != nil {
		return
	}
	n += tmpn
	pos := 0
	buf := buffer.GetValue(pos).([]byte)
	pos++
	cnt := types.DecodeFixed[uint16](buf)
	vecTypes := make([]types.Type, cnt)
	bat.Attrs = make([]string, cnt)
	for i := 0; i < int(cnt); i++ {
		buf = buffer.GetValue(pos).([]byte)
		pos++
		bat.Attrs[i] = string(buf)
		bat.Nameidx[bat.Attrs[i]] = i
		buf = buffer.GetValue(pos).([]byte)
		vecTypes[i] = types.DecodeType(buf)
		pos++
	}
	for _, vecType := range vecTypes {
		vec := MakeVector(vecType, common.DefaultAllocator)
		if tmpn, err = readVectorV1(vec, r); err != nil {
			ReleaseVector(vec)
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
	buf = make([]byte, size)
	if _, err = r.Read(buf); err != nil {
		return
	}
	bat.SetDeletes(&nulls.Nulls{})
	bat.Deletes.Init(bat.deletesMP())
	if err = bat.Deletes.GetBitmap().UnmarshalV1(buf); err != nil {
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
		if err = extendVector(vec, src.Vecs[i]); err != nil {
			return
		}
	}
	return
}

// extendVector appends every row of src to dst.
func extendVector(dst, src Vector) error {
	if src.Length() == 0 {
		return nil
	}
	return dst.UnionBatch(src, 0, src.Length(), nil, dst.MPool())
}

// extend vector with same name, consume src batch
func (bat *Batch) Extend(src *Batch) {
	for i, vec := range bat.Vecs {
		attr := bat.Attrs[i]
		if idx, ok := src.Nameidx[attr]; ok {
			if err := extendVector(vec, src.Vecs[idx]); err != nil {
				panic(err)
			}
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

// approxSize is the vector's data size, 0 for a read-only vector.
func approxSize(vec Vector) int {
	if vec.IsReadOnly() {
		return 0
	}
	return vec.Size()
}

// vectorAllocated is the memory vec owns, 0 for a read-only vector (whose
// referenced capacity vector.Vector.Allocated reports).
func vectorAllocated(vec Vector) int {
	if vec.IsReadOnly() {
		return 0
	}
	return vec.Allocated()
}

// writeVector writes vec as an 8-byte size followed by its binary encoding.
func writeVector(vec Vector, w io.Writer) (n int64, err error) {
	return marshalFramed(vec, w, vec.MarshalBinaryWithBuffer)
}

// writeVectorV1 is writeVector in version 1, whose null bitmap is v1.
func writeVectorV1(vec Vector, w io.Writer) (n int64, err error) {
	return marshalFramed(vec, w, vec.MarshalBinaryWithBufferV1)
}

func marshalFramed(vec Vector, w io.Writer, marshal func(*bytes.Buffer) error) (n int64, err error) {
	estimatedSize := approxSize(vec)
	if estimatedSize < 0 {
		estimatedSize = 0
	}
	estimatedSize += 8 // size header
	if estimatedSize < 256 {
		estimatedSize = 256
	}

	// Optimization: write directly into a *bytes.Buffer
	if buf, ok := w.(*bytes.Buffer); ok {
		if buf.Cap()-buf.Len() < estimatedSize {
			buf.Grow(estimatedSize)
		}
		sizeOffset := buf.Len()
		var sizePlaceholder int64
		if _, err = buf.Write(types.EncodeInt64(&sizePlaceholder)); err != nil {
			return
		}
		if err = marshal(buf); err != nil {
			return
		}
		size := int64(buf.Len() - sizeOffset - 8)
		copy(buf.Bytes()[sizeOffset:sizeOffset+8], types.EncodeInt64(&size))
		n = int64(buf.Len() - sizeOffset)
		return
	}

	var bs bytes.Buffer
	bs.Grow(estimatedSize)
	var size int64
	_, _ = bs.Write(types.EncodeInt64(&size))
	if err = marshal(&bs); err != nil {
		return
	}
	size = int64(bs.Len() - 8)
	buf := bs.Bytes()
	copy(buf[:8], types.EncodeInt64(&size))
	if _, err = w.Write(buf); err != nil {
		return
	}
	n = int64(len(buf))
	return
}

// readVector reads what writeVector wrote into vec: vec's memory is
// released, and vec becomes a read-only vector of its type over the bytes
// read.
func readVector(vec Vector, r io.Reader) (n int64, err error) {
	buf := make([]byte, 8)
	if _, err = io.ReadFull(r, buf); err != nil {
		return
	}
	n += 8
	buf = make([]byte, types.DecodeInt64(buf[:]))
	if _, err = io.ReadFull(r, buf); err != nil {
		return
	}
	n += int64(len(buf))
	resetReadOnly(vec)
	err = vec.UnmarshalBinary(buf)
	return
}

// readVectorV1 reads what writeVectorV1 wrote, like readVector.
func readVectorV1(vec Vector, r io.Reader) (n int64, err error) {
	buf := make([]byte, 8)
	if _, err = r.Read(buf); err != nil {
		return
	}
	n += 8
	buf = make([]byte, types.DecodeInt64(buf[:]))
	if _, err = r.Read(buf); err != nil {
		return
	}
	n += int64(len(buf))
	resetReadOnly(vec)
	err = vec.UnmarshalBinaryV1(buf)
	return
}

// resetReadOnly releases vec's memory and makes it an empty read-only vector
// of the same type, ready to decode into.
func resetReadOnly(vec Vector) {
	t := *vec.GetType()
	vec.Free(vec.MPool())
	*vec = *vector.NewVec(t, nil)
}

// ppString pretty-prints up to num values of vec.
func ppString(vec Vector, num int) string {
	var w bytes.Buffer
	limit := vec.Length()
	if num > 0 && num < limit {
		limit = num
	}
	_, _ = w.WriteString(fmt.Sprintf("[T=%s]%s", vec.GetType().String(), common.MoVectorToString(vec, limit)))
	if vec.Length() > num {
		_, _ = w.WriteString("...")
	}
	_, _ = w.WriteString(fmt.Sprintf("[%v]", isLent(vec)))
	return w.String()
}
