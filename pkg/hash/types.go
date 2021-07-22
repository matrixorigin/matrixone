package hash

import "matrixone/pkg/sql/colexec/aggregation"

type Group struct {
	Is   []int
	Sel  int64
	Aggs []aggregation.Aggregation
}

type SetGroup struct {
	Sel int64
}

type BagGroup struct {
	Sel  int64
	Sels []int64
}
