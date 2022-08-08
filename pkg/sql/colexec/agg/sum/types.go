// Copyright 2022 Matrix Origin
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

package sum

import "github.com/matrixorigin/matrixone/pkg/container/types"

type Numeric interface {
	types.Ints | types.UInts | types.Floats
}

// if input type is int8/int16/int32/int64, the return type is int64
// if input type is uint8/uint16/uint32/uint64, the return type is uint64
// f input type is float32/float64, the return type is float64
type ReturnTyp interface {
	uint64 | int64 | float64
}

type Sum[T1 Numeric, T2 ReturnTyp] struct {
}

type Decimal64Sum struct {
}

type Decimal128Sum struct {
}
