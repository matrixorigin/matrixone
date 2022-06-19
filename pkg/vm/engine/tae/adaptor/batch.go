package adaptor

import "fmt"

func NewBatch() *Batch {
	return &Batch{
		AttrName: make([]string, 0),
		AttrRef:  make([]int, 0),
		nameidx:  make(map[string]int),
		refidx:   make(map[int]int),
		Vecs:     make([]Vector, 0),
	}
}

func (bat *Batch) AddNamedVector(name string, vec Vector) {
	if _, exist := bat.nameidx[name]; exist {
		panic(fmt.Errorf("duplicate vector %s", name))
	}
	idx := len(bat.Vecs)
	bat.nameidx[name] = idx
	bat.AttrName = append(bat.AttrName, name)
	bat.Vecs = append(bat.Vecs, vec)
}

func (bat *Batch) AddRefVector(id int, vec Vector) {
	if _, exist := bat.refidx[id]; exist {
		panic(fmt.Errorf("duplicate vector ref %d", id))
	}
	idx := len(bat.Vecs)
	bat.refidx[id] = idx
	bat.AttrRef = append(bat.AttrRef, id)
	bat.Vecs = append(bat.Vecs, vec)
}

func (bat *Batch) GetVectorByName(name string) Vector {
	pos := bat.nameidx[name]
	return bat.Vecs[pos]
}

func (bat *Batch) GetVectorByRef(id int) Vector {
	pos := bat.refidx[id]
	return bat.Vecs[pos]
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
