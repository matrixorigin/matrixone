package vector

import (
	"matrixbase/pkg/container/nulls"
	"matrixbase/pkg/container/types"
)

/*
type Vector interface {
	Reset()

    Type() types.T
    Bools() []bool
    Ints() []int64
    Sels() []int64
    Floats() []float64
    Bytes() Bytes
    Tuple() [][]interface{}

    Col() interface{}
    SetCol(interface{})

    Nulls() nulls.Nulls
    SetNulls(nulls.Nulls)

    Window(int, int) Vector

    Length() int

	Append(interface{}, nulls.Nulls)

    Filter([]int64) Vector

    Read([]byte) error
    Show() ([]byte, error)

    String() string
}
*/

type Vector struct {
	Data []byte // raw data
	Typ  types.T
	Col  interface{}
	Nsp  *nulls.Nulls
}

type Bytes struct {
	Data []byte
	Os   []uint32
	Ns   []uint32
}
