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

package stddevpop

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	Variance "github.com/matrixorigin/matrixone/pkg/sql/colexec/agg/variance"
)

type Stddevpop[T1 types.Floats | types.Ints | types.UInts] struct {
	variance *Variance.Variance[T1]
}

type Stddevpop2 struct {
	variance *Variance.Variance2
}

type Stddevpop3 struct {
	variance *Variance.Variance3
}
