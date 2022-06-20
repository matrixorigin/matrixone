package adaptor

import (
	"fmt"

	"github.com/RoaringBitmap/roaring"
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

// func (bat *Batch) AddRefVector(id int, vec Vector) {
// 	if _, exist := bat.refidx[id]; exist {
// 		panic(fmt.Errorf("duplicate vector ref %d", id))
// 	}
// 	idx := len(bat.Vecs)
// 	bat.refidx[id] = idx
// 	bat.AttrRef = append(bat.AttrRef, id)
// 	bat.Vecs = append(bat.Vecs, vec)
// }

func (bat *Batch) GetVectorByName(name string) Vector {
	pos := bat.nameidx[name]
	return bat.Vecs[pos]
}

// func (bat *Batch) GetVectorByRef(id int) Vector {
// 	pos := bat.refidx[id]
// 	return bat.Vecs[pos]
// }

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
