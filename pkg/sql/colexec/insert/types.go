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

package insert

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	INSERT = iota
	DELETE
)

type Argument struct {
	// Ts is not used
	Ts        uint64
	Affected  uint64
	Engine    engine.Engine
	IsRemote  bool // mark if this insert is cn2s3 directly
	Container *colexec.WriteS3Container
	InsertCtx *InsertCtx
}

type InsertCtx struct {
	Source       engine.Relation
	Ref          *plan.ObjectRef
	TableDef     *plan.TableDef
	UniqueSource []engine.Relation

	ParentIdx    map[string]int32
	ClusterTable *plan.ClusterTable
}

// The Argument for insert data directly to s3 can not be free when this function called as some datastructure still needed.
// therefore, those argument in remote CN will be free in connector operator, and local argument will be free in mergeBlock operator
func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {

}
