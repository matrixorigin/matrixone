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
	"github.com/matrixorigin/matrixone/pkg/container/bitmap"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

// Vector represent a column
type Vector[T types.Element] struct {
	// Col represent the decoding column data
	Col []T
	// Data represent the encoding column data
	Data []byte
	// Type represent the type of column
	Typ types.Type
	Nsp *bitmap.Bitmap

	// Const used for const vector (a vector with a lot of rows of a same const value)
	IsConst bool
	Const   struct {
		Size int
	}

	// used for array and string
	Array struct {
		Offsets []uint64
		Lengths []uint64
	}
}

// Vector represent a memory column
type AnyVector interface {
	Reset()
	Length() int
	SetLength(n int)
	Type() types.Type
	NewNulls(int)
	Free(*mheap.Mheap)
	Nulls() *bitmap.Bitmap
	Realloc(size int, m *mheap.Mheap) error
}
