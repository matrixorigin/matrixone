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

package ring

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

type Ring interface {
	Count() int

	Size() int

	Dup() Ring
	Type() types.Type

	String() string

	Free(*mheap.Mheap)
	Grow(*mheap.Mheap) error

	Grows(int, *mheap.Mheap) error

	SetLength(int)
	Shrink([]int64)

	Shuffle([]int64, *mheap.Mheap) error

	Eval([]int64) *vector.Vector

	Fill(int64, int64, int64, *vector.Vector)

	BulkFill(int64, []int64, *vector.Vector)

	BatchFill(int64, []uint8, []uint64, []int64, *vector.Vector)

	Add(interface{}, int64, int64)
	Mul(interface{}, int64, int64, int64)

	BatchAdd(interface{}, int64, []uint8, []uint64)
}
