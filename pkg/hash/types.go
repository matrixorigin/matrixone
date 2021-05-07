package hash

import "matrixone/pkg/sql/colexec/aggregation"

type Group struct {
	Is   []int
	Idx  int64
	Sel  int64
	Aggs []aggregation.Aggregation
}

type SetGroup struct {
	Idx int64
	Sel int64
}

type BagGroup struct {
	Idx  int64
	Sel  int64
	Is   []int64
	Sels []int64
}

type DedupGroup struct {
	Sel int64
}
