// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package generate_series

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type Type interface {
	types.Datetime | types.Timestamp | int
}

type Step interface {
	types.IntervalType | int
}

type Param[T1 Type, T2 Step] struct {
	Attrs []string
	Cols  []*plan.ColDef
	Start T1
	Stop  T2
}

type Argument[T1 Type, T2 Step] struct {
	Es *Param[T1, T2]
}
