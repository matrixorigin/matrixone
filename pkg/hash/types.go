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

package hash

import "github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregation"

type Group struct {
	Is   []int
	Sel  int64
	Aggs []aggregation.Aggregation
}

type SetGroup struct {
	Sel int64
}

type BagGroup struct {
	Sel  int64
	Sels []int64
}
