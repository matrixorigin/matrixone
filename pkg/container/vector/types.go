// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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

/* Vector vector
 * origin true:
 * 				count || type || bitmap size || bitmap || vector
 * origin false:
 *  			count || vector
 */
type Vector struct {
	Or   bool   // true: origin
	Ref  uint64 // reference count
	Link uint64 // link count
	Data []byte // raw data
	Typ  types.Type
	Col  interface{}  // column data, encoded Data
	Nsp  *nulls.Nulls // nulls list

	// some attributes for const vector (a vector with a lot of rows of a same const value)
	IsConst bool
	Length  int
}

// emptyInterface is the header for an interface{} value.
type emptyInterface struct {
	_    *int
	word unsafe.Pointer
}
