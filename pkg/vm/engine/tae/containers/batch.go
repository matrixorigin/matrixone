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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

func NewBatch() *Batch {
	return &Batch{
		Attrs:   make([]string, 0),
		nameidx: make(map[string]int),
		Vecs:    make([]Vector, 0),
	}
}

func (bat *Batch) AddVector(attr string, vec Vector) {
	if _, exist := bat.nameidx[attr]; exist {
		panic(moerr.NewInternalErrorNoCtx("duplicate vector %s", attr))
	}
	idx := len(bat.Vecs)
	bat.nameidx[attr] = idx
	bat.Attrs = append(bat.Attrs, attr)
	bat.Vecs = append(bat.Vecs, vec)
}

func (bat *Batch) GetVectorByName(name string) Vector {
	pos := bat.nameidx[name]
	return bat.Vecs[pos]
}

func (bat *Batch) RangeDelete(start, end int) {
	if bat.Deletes == nil {
		bat.Deletes = roaring.New()
	}
	bat.Deletes.AddRange(uint64(start), uint64(end))
}

func (bat *Batch) Delete(i int) {
	if bat.Deletes == nil {
		bat.Deletes = roaring.BitmapOf(uint32(i))
	} else {
		bat.Deletes.Add(uint32(i))
	}
}

func (bat *Batch) HasDelete() bool {
	return bat.Deletes != nil && !bat.Deletes.IsEmpty()
}

func (bat *Batch) IsDeleted(i int) bool {
	if !bat.HasDelete() {
		return false
	}
	return bat.Deletes.ContainsInt(i)
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
		vec.Compact(bat.Deletes)
	}
	bat.Deletes = nil
}

func (bat *Batch) Length() int {
	return bat.Vecs[0].Length()
}

func (bat *Batch) Allocated() int {
	allocated := 0
	for _, vec := range bat.Vecs {
		allocated += vec.Allocated()
	}
	return allocated
}

func (bat *Batch) Window(offset, length int) *Batch {
	win := new(Batch)
	win.Attrs = bat.Attrs
	win.nameidx = bat.nameidx
	if bat.Deletes != nil && offset+length != bat.Length() {
		win.Deletes = common.BM32Window(bat.Deletes, offset, offset+length)
	} else {
		win.Deletes = bat.Deletes
	}
	win.Vecs = make([]Vector, len(bat.Vecs))
	for i := range win.Vecs {
		win.Vecs[i] = bat.Vecs[i].Window(offset, length)
	}
	return win
}

func (bat *Batch) CloneWindow(offset, length int, allocator ...*mpool.MPool) (cloned *Batch) {
	cloned = new(Batch)
	cloned.Attrs = make([]string, len(bat.Attrs))
	copy(cloned.Attrs, bat.Attrs)
	cloned.nameidx = make(map[string]int, len(bat.nameidx))
	for k, v := range bat.nameidx {
		cloned.nameidx[k] = v
	}
	if bat.Deletes != nil {
		cloned.Deletes = common.BM32Window(bat.Deletes, offset, offset+length)
	}
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
	for _, vec := range bat.Vecs {
		vec.Close()
	}
}

func (bat *Batch) Equals(o *Batch) bool {
	if bat.Length() != o.Length() {
		return false
	}
	if bat.DeleteCnt() != o.DeleteCnt() {
		return false
	}
	if bat.HasDelete() {
		if !bat.Deletes.Equals(o.Deletes) {
			return false
		}
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
	buffer := MakeVector(types.T_varchar.ToType(), false)
	defer buffer.Close()
	// 1. Vector cnt
	// if nr, err = w.Write(types.EncodeFixed(uint16(len(bat.Vecs)))); err != nil {
	// 	return
	// }
	// n += int64(nr)
	buffer.Append(types.EncodeFixed(uint16(len(bat.Vecs))))

	// 2. Types and Names
	for i, vec := range bat.Vecs {
		buffer.Append([]byte(bat.Attrs[i]))
		vt := vec.GetType()
		buffer.Append(types.EncodeType(&vt))
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
		if buf, err = bat.Deletes.ToBytes(); err != nil {
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
	buffer := MakeVector(types.T_varchar.ToType(), false)
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
		bat.nameidx[bat.Attrs[i]] = i
		buf = buffer.Get(pos).([]byte)
		vecTypes[i] = types.DecodeType(buf)
		pos++
	}
	for _, vecType := range vecTypes {
		vec := MakeVector(vecType, true)
		if tmpn, err = vec.ReadFrom(r); err != nil {
			return
		}
		bat.Vecs = append(bat.Vecs, vec)
		n += tmpn
	}
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
	bat.Deletes = roaring.New()
	if tmpn, err = bat.Deletes.ReadFrom(r); err != nil {
		return
	}
	n += tmpn

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
		if idx, ok := src.nameidx[attr]; ok {
			vec.Extend(src.Vecs[idx])
		}
	}
	src.Close()
}
