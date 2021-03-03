package top

import (
	"matrixbase/pkg/compare"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
)

// Direction for ordering results.
type Direction int8

// Direction values.
const (
	DefaultDirection Direction = iota
	Ascending
	Descending
)

type Container struct {
	n        int // number of attributes involved in sorting
	sels     []int64
	selsData []byte
	vecs     []*vector.Vector
	cmps     []compare.Compare
}

type Field struct {
	Oid  types.T
	Attr string
	Type Direction
}

type Argument struct {
	Limit int64
	Fs    []Field
	Ctr   Container
}

func (ctr *Container) compare(i, j int64) int {
	for k := 0; k < ctr.n; k++ {
		if r := ctr.cmps[k].Compare(0, 0, i, j); r != 0 {
			return r
		}
	}
	return 0
}

// maximum heap
func (ctr *Container) Len() int {
	return len(ctr.sels)
}

func (ctr *Container) Less(i, j int) bool {
	return ctr.compare(ctr.sels[i], ctr.sels[j]) > 0
}

func (ctr *Container) Swap(i, j int) {
	ctr.sels[i], ctr.sels[j] = ctr.sels[j], ctr.sels[i]
}

func (ctr *Container) Push(x interface{}) {
	ctr.sels = append(ctr.sels, x.(int64))
}

func (ctr *Container) Pop() interface{} {
	n := len(ctr.sels) - 1
	x := ctr.sels[n]
	ctr.sels = ctr.sels[:n]
	return x
}
