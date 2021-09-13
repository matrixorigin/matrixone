package extend

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/process"
)

type Extend interface {
	Eq(Extend) bool
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
