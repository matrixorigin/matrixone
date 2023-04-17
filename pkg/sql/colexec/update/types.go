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

package update

import (
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Argument struct {
	Ts           uint64
	affectedRows uint64
	Engine       engine.Engine
	UpdateCtx    *UpdateCtx
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
}

type UpdateCtx struct {
	Source       []engine.Relation
	UniqueSource [][]engine.Relation
	Idxs         [][]int32
	Ref          []*plan.ObjectRef
	TableDefs    []*plan.TableDef
	HasAutoCol   []bool
	UpdateCol    []map[string]int32

	IdxSource []engine.Relation
	IdxIdx    []int32

	OnRestrictIdx []int32

	OnCascadeSource       []engine.Relation
	OnCascadeUniqueSource [][]engine.Relation
	OnCascadeIdx          [][]int32
	OnCascadeRef          []*plan.ObjectRef
	OnCascadeTableDef     []*plan.TableDef
	OnCascadeUpdateCol    []map[string]int32

	OnSetSource       []engine.Relation
	OnSetUniqueSource [][]engine.Relation
	OnSetIdx          [][]int32
	OnSetRef          []*plan.ObjectRef
	OnSetTableDef     []*plan.TableDef
	OnSetUpdateCol    []map[string]int32

	ParentIdx []map[string]int32
}

func (arg *Argument) AffectedRows() uint64 {
	return arg.affectedRows
}
