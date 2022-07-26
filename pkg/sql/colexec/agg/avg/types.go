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

package avg

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type Numeric interface {
	types.Ints | types.UInts | types.Floats
}

type Avg[T Numeric] struct {
	cnt int64
}

type Decimal64Avg struct {
	cnt int64
}

type Decimal128Avg struct {
	cnt int64
}
