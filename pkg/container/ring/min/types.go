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

package min

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/mheap"
)

type Int8Ring struct {
	Da  []byte
	Vs  []int8
	Ns  []int64
	Typ types.Type
}

type Int16Ring struct {
	Da  []byte
	Vs  []int16
	Ns  []int64
	Typ types.Type
}

type Int32Ring struct {
	Da  []byte
	Vs  []int32
	Ns  []int64
	Typ types.Type
}

type Int64Ring struct {
	Da  []byte
	Vs  []int64
	Ns  []int64
	Typ types.Type
}

type UInt8Ring struct {
	Da  []byte
	Ns  []int64
	Vs  []uint8
	Typ types.Type
}

type UInt16Ring struct {
	Da  []byte
	Ns  []int64
	Vs  []uint16
	Typ types.Type
}

type UInt32Ring struct {
	Da  []byte
	Ns  []int64
	Vs  []uint32
	Typ types.Type
}

type UInt64Ring struct {
	Da  []byte
	Ns  []int64
	Vs  []uint64
	Typ types.Type
}

type Float32Ring struct {
	Da  []byte
	Ns  []int64
	Vs  []float32
	Typ types.Type
}

type Float64Ring struct {
	Da  []byte
	Ns  []int64
	Vs  []float64
	Typ types.Type
}

type StrRing struct {
	Ns  []int64
	Vs  [][]byte
	Typ types.Type
	Mp  *mheap.Mheap
}
