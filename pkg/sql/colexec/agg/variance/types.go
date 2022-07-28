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

package variance

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type Variance[T1 types.Floats | types.Ints | types.UInts] struct {
	sum   []float64
	count []float64
}

//for decimal64
type Variance2 struct {
	inputType types.Type
	sum       []float64
	count     []float64
}

//for deimal128
type Variance3 struct {
	inputType types.Type
	sum       []float64
	count     []float64
}
