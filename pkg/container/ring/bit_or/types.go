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

package bit_or

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type UInt8Ring struct {
	Data  []byte
	NullCounts  []int64
	Values  []uint8
	Typ types.Type
}

type UInt16Ring struct {
	Data  []byte
	NullCounts  []int64
	Values  []uint16
	Typ types.Type
}

type UInt32Ring struct {
	Data  []byte
	NullCounts  []int64
	Values  []uint32
	Typ types.Type
}

type UInt64Ring struct {
	Data  []byte
	NullCounts  []int64
	Values  []uint64
	Typ types.Type
}

type Int8Ring struct {
	Data  []byte
	NullCounts  []int64
	Values  []int8
	Typ types.Type
}

type Int16Ring struct {
	Data  []byte
	NullCounts  []int64
	Values  []int16
	Typ types.Type
}

type Int32Ring struct {
	Data  []byte
	NullCounts  []int64
	Values  []int32
	Typ types.Type
}

type Int64Ring struct {
	Data  []byte
	NullCounts  []int64
	Values  []int64
	Typ types.Type
}

type FloatRing struct {
	Data  []byte
	NullCounts  []int64
	Values  []uint64
	Typ types.Type
}