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

package any

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

type Int8Ring struct {
	// Data store all the value's bytes
	Data []byte
	// Values store value of each group,its memory address is same to Data
	Values []int8
	// NullCounts is group to record number of the null value
	NullCounts []int64
	// Typ is vector's value type
	Typ types.Type
}

type Int16Ring struct {
	// Data store all the value's bytes
	Data []byte
	// Values store value of each group,its memory address is same to Data
	Values []int16
	// NullCounts is group to record number of the null value
	NullCounts []int64
	// Typ is vector's value type
	Typ types.Type
}

type Int32Ring struct {
	// Data store all the value's bytes
	Data []byte
	// Values store value of each group,its memory address is same to Data
	Values []int32
	// NullCounts is group to record number of the null value
	NullCounts []int64
	// Typ is vector's value type
	Typ types.Type
}

type Int64Ring struct {
	// Data store all the value's bytes
	Data []byte
	// Values store value of each group,its memory address is same to Data
	Values []int64
	// NullCounts is group to record number of the null value
	NullCounts []int64
	// Typ is vector's value type
	Typ types.Type
}

type UInt8Ring struct {
	// Data store all the value's bytes
	Data []byte
	// NullCounts is group to record number of the null value
	NullCounts []int64
	// Values store value of each group,its memory address is same to Data
	Values []uint8
	// Typ is vector's value type
	Typ types.Type
}

type UInt16Ring struct {
	// Data store all the value's bytes
	Data []byte
	// NullCounts is group to record number of the null value
	NullCounts []int64
	// Values store value of each group,its memory address is same to Data
	Values []uint16
	// Typ is vector's value type
	Typ types.Type
}

type UInt32Ring struct {
	// Data store all the value's bytes
	Data []byte
	// NullCounts is group to record number of the null value
	NullCounts []int64
	// Values store value of each group,its memory address is same to Data
	Values []uint32
	// Typ is vector's value type
	Typ types.Type
}

type UInt64Ring struct {
	// Data store all the value's bytes
	Data []byte
	// NullCounts is group to record number of the null value
	NullCounts []int64
	// Values store value of each group,its memory address is same to Data
	Values []uint64
	// Typ is vector's value type
	Typ types.Type
}

type Float32Ring struct {
	// Data store all the value's bytes
	Data []byte
	// NullCounts is group to record number of the null value
	NullCounts []int64
	// Values store value of each group,its memory address is same to Data
	Values []float32
	// Typ is vector's value type
	Typ types.Type
}

type Float64Ring struct {
	// Data store all the value's bytes
	Data []byte
	// NullCounts is group to record number of the null value
	NullCounts []int64
	// Values store value of each group,its memory address is same to Data
	Values []float64
	// Typ is vector's value type
	Typ types.Type
}

type StrRing struct {
	Empty []bool // isEmpty
	// NullCounts is group to record number of the null value
	NullCounts []int64
	// Values store value of each group
	Values [][]byte
	// Typ is vector's value type
	Typ types.Type
	Mp  *mheap.Mheap
}
type DateRing struct {
	// Data store all the value's bytes
	Data []byte
	// Values store value of each group,its memory address is same to Data
	Values []types.Date
	// NullCounts is group to record number of the null value
	NullCounts []int64
	// Typ is vector's value type
	Typ types.Type
}

type DatetimeRing struct {
	// Data store all the value's bytes
	Data []byte
	// Values store value of each group,its memory address is same to Data
	Values []types.Datetime
	// NullCounts is group to record number of the null value
	NullCounts []int64
	// Typ is vector's value type
	Typ types.Type
}
