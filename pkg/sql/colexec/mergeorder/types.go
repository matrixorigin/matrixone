package mergeorder

import (
	"fmt"
	"matrixone/pkg/compare"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/metadata"
)

const (
	Build = iota
	Eval
	Merge
	MergeEval
	EvalPrepare
)

// Direction for ordering results.
type Direction int8

// Direction values.
const (
	DefaultDirection Direction = iota
	Ascending
	Descending
)

type input struct {
	seg string
	bat *batch.Batch
}

type partition struct {
	rows int64
	heap []int
	lens []int64
	mins []int64
	bat  *batch.Batch
	bats []*batch.Batch
	r    engine.Relation
	cmps []compare.Compare
	segs []engine.SegmentInfo
}

type Container struct {
	state int
	heap  []int
	lens  []int64
	mins  []int64
	attrs []string
	ptn   *partition
	bat   *batch.Batch
	ptns  []*partition
	cmps  []compare.Compare
	spill struct {
		id    string
		cs    []uint64
		attrs []string
		e     engine.SpillEngine
		md    []metadata.Attribute
	}
}

type Field struct {
	Attr string
	Type Direction
}

type Argument struct {
	Fs  []Field
	Ctr Container
	E   engine.SpillEngine
}

var directionName = [...]string{
	DefaultDirection: "",
	Ascending:        "ASC",
	Descending:       "DESC",
}

func (n Field) String() string {
	s := n.Attr
	if n.Type != DefaultDirection {
		s += " " + n.Type.String()
	}
	return s
}

func (i Direction) String() string {
	if i < 0 || i > Direction(len(directionName)-1) {
		return fmt.Sprintf("Direction(%d)", i)
	}
	return directionName[i]
}

func (ptn *partition) compare(i, j int) int {
	vi, vj := ptn.mins[i], ptn.mins[j]
	if len(ptn.bats[i].Sels) > 0 {
		vi = ptn.bats[i].Sels[vi]
	}
	if len(ptn.bats[j].Sels) > 0 {
		vj = ptn.bats[j].Sels[vj]
	}
	for k, cmp := range ptn.cmps {
		cmp.Set(0, ptn.bats[i].Vecs[k])
		cmp.Set(1, ptn.bats[j].Vecs[k])
		if r := cmp.Compare(0, 1, vi, vj); r != 0 {
			return r
		}
	}
	return 0
}

func (ptn *partition) Len() int {
	return len(ptn.heap)
}

func (ptn *partition) Less(i, j int) bool {
	return ptn.compare(ptn.heap[i], ptn.heap[j]) < 0
}

func (ptn *partition) Swap(i, j int) {
	ptn.heap[i], ptn.heap[j] = ptn.heap[j], ptn.heap[i]
}

func (ptn *partition) Push(x interface{}) {
	ptn.heap = append(ptn.heap, x.(int))
}

func (ptn *partition) Pop() interface{} {
	n := len(ptn.heap) - 1
	x := ptn.heap[n]
	ptn.heap = ptn.heap[:n]
	return x
}

func (ctr *Container) compare(i, j int) int {
	vi, vj := ctr.mins[i], ctr.mins[j]
	if len(ctr.ptns[i].bat.Sels) > 0 {
		vi = ctr.ptns[i].bat.Sels[vi]
	}
	if len(ctr.ptns[j].bat.Sels) > 0 {
		vj = ctr.ptns[j].bat.Sels[vj]
	}
	for k, cmp := range ctr.cmps {
		cmp.Set(0, ctr.ptns[i].bat.Vecs[k])
		cmp.Set(1, ctr.ptns[j].bat.Vecs[k])
		if r := cmp.Compare(0, 1, vi, vj); r != 0 {
			return r
		}
	}
	return 0
}

// minimum heap
func (ctr *Container) Len() int {
	return len(ctr.heap)
}

func (ctr *Container) Less(i, j int) bool {
	return ctr.compare(ctr.heap[i], ctr.heap[j]) < 0
}

func (ctr *Container) Swap(i, j int) {
	ctr.heap[i], ctr.heap[j] = ctr.heap[j], ctr.heap[i]
}

func (ctr *Container) Push(x interface{}) {
	ctr.heap = append(ctr.heap, x.(int))
}

func (ctr *Container) Pop() interface{} {
	n := len(ctr.heap) - 1
	x := ctr.heap[n]
	ctr.heap = ctr.heap[:n]
	return x
}
