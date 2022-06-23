package containers

import (
	"fmt"
	"io"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
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
		panic(fmt.Errorf("duplicate vector %s", attr))
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

func (bat *Batch) Capacity() int {
	return bat.Vecs[0].Capacity()
}

func (bat *Batch) Allocated() int {
	allocated := 0
	for _, vec := range bat.Vecs {
		allocated += vec.Allocated()
	}
	return allocated
}

func (bat *Batch) Close() {
	for _, vec := range bat.Vecs {
		vec.Close()
	}
}

func (bat *Batch) WriteTo(w io.Writer) (n int64, err error) {
	var nr int
	var tmpn int64
	buffer := containers.NewVector[[]byte]()
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
		buffer.Append(types.EncodeType(vec.GetType()))
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
	buffer := containers.NewVector[[]byte]()
	defer buffer.Close()
	if tmpn, err = buffer.ReadFrom(r); err != nil {
		return
	}
	n += tmpn
	pos := 0
	buf := buffer.Get(pos)
	pos++
	cnt := types.DecodeFixed[uint16](buf)
	vecTypes := make([]types.Type, cnt)
	bat.Attrs = make([]string, cnt)
	for i := 0; i < int(cnt); i++ {
		buf = buffer.Get(pos)
		pos++
		bat.Attrs[i] = string(buf)
		bat.nameidx[bat.Attrs[i]] = i
		buf = buffer.Get(pos)
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
