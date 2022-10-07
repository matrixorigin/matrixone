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

package hashbuild

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/index"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

const (
	Build = iota
	End
)

type evalVector struct {
	needFree bool
	vec      *vector.Vector
}

type container struct {
	state int

	hasNull bool

	sels [][]int64

	bat *batch.Batch

	evecs []evalVector
	vecs  []*vector.Vector

	mp *hashmap.StrHashMap

	idx *index.LowCardinalityIndex
}

type Argument struct {
	ctr *container
	// need to generate a push-down filter expression
	NeedExpr    bool
	NeedHashMap bool
	Ibucket     uint64
	Nbucket     uint64
	Typs        []types.Type
	Conditions  []*plan.Expr
}
