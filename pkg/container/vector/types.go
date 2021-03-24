package vector

import (
	"matrixbase/pkg/container/nulls"
	"matrixbase/pkg/container/types"
)

/*
type Vector interface {
	Reset()

    Col() interface{}
    SetCol(interface{})

    Length() int

    Window(int, int) Vector

	Append(interface{})

    Shuffle([]int64) Vector

	UnionOne(Vector, int64) error

    Read([]byte) error
    Show() ([]byte, error)

    String() string
}
*/

/*
 * origin true:
 * 				count || type || bitmap size || bitmap || vector
 * origin false:
 *  			count || vector
 */
type Vector struct {
	Or   bool   // true: origin
	Data []byte // raw data
	Typ  types.Type
	Col  interface{}
	Nsp  *nulls.Nulls
}
