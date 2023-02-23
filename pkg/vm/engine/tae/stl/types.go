// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stl

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func Sizeof[T any]() int {
	var v T
	return int(unsafe.Sizeof(v))
}

func SizeOfMany[T any](cnt int) int {
	var v T
	return int(unsafe.Sizeof(v)) * cnt
}

type Bytes struct {
	// Specify type size
	// Positive if it is fixed type
	// Negtive if it is varlen type
	TypeSize int

	// Specify whether it retains a window
	AsWindow bool
	// Window offset and length
	WinOffset int
	WinLength int

	// Used only when IsFixedType is false
	// Header store data if the size is less than VarlenaSize
	Header []types.Varlena

	// When IsFixedType is true, here is the data storage
	// When IsFixedType is false, here is the data storage for big data
	// When AsWindow is true, here stores all big data
	Storage []byte
}
