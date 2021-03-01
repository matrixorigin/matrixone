package extend

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/vm/process"
)

type Extend interface {
	String() string
	IsLogical() bool
	IsConstant() bool
	ReturnType() types.T
	Attributes() []string
	Eval(*batch.Batch, *process.Process) (*vector.Vector, types.T, error)
}

type UnaryExtend struct {
	Op int
	E  Extend
}

type BinaryExtend struct {
	Op          int
	Left, Right Extend
}

type MultiExtend struct {
	Op   int
	Args []Extend
}

type ParenExtend struct {
	E Extend
}

type ValueExtend struct {
	V *vector.Vector
}

type Attribute struct {
	Name string  `json:"name"`
	Type types.T `json:"type"`
}
