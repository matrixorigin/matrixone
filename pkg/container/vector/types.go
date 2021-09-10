package vector

import (
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/types"
	"unsafe"
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
	Ref  uint64 // reference count
	Data []byte // raw data
	Typ  types.Type
	Col  interface{}  // column data
	Nsp  *nulls.Nulls // nulls list
}

// emptyInterface is the header for an interface{} value.
type emptyInterface struct {
	_    *int
	word unsafe.Pointer
}
