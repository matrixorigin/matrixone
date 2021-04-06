package aggregation

import (
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/vm/process"
)

const (
	// user function
	Avg = iota
	Max
	Min
	Sum
	Count
	StarCount
	// system function
	SumCount
)

var AggName = [...]string{
	Avg:       "avg",
	Max:       "max",
	Min:       "min",
	Sum:       "sum",
	Count:     "count",
	StarCount: "starCount",
	SumCount:  "sumCount",
}

type Extend struct {
	Op    int
	Name  string
	Alias string
	Agg   Aggregation
}

type Aggregation interface {
	Reset()
	Type() types.Type
	Dup() Aggregation
	Eval() interface{}
	Fill([]int64, *vector.Vector) error
	EvalCopy(*process.Process) (*vector.Vector, error)
}
