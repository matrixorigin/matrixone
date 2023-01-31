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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	INSERT = iota
	DELETE
)

type Container struct {
	writer        objectio.Writer
	unique_writer []objectio.Writer
	pkIndex       []int
	// record every batch's Length
	lengths []uint64
	// record unique batch's Length
	unique_lengths   [][]uint64
	cacheBat         *batch.Batch
	nameToNullablity map[string]bool
	pk               map[string]bool
}

type Argument struct {
	// Ts is not used
	Ts                   uint64
	TargetTable          engine.Relation
	TargetColDefs        []*plan.ColDef
	Affected             uint64
	Engine               engine.Engine
	DB                   engine.Database
	TableID              uint64
	CPkeyColDef          *plan.ColDef
	DBName               string
	TableName            string
	UniqueIndexTables    []engine.Relation
	UniqueIndexDef       *plan.UniqueIndexDef
	SecondaryIndexTables []engine.Relation
	SecondaryIndexDef    *plan.SecondaryIndexDef
	ClusterTable         *plan.ClusterTable
	ClusterByDef         *plan.ClusterByDef
	IsRemote             bool // mark if this insert is cn2s3 directly
	HasAutoCol           bool
	container            *Container
}

// The Argument for insert data directly to s3 can not be free when this function called as some datastructure still needed.
// therefore, those argument in remote CN will be free in connector operator, and local argument will be free in mergeBlock operator
func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {

}

func (arg *Argument) GetPkIndexes() {
	arg.container.pkIndex = make([]int, 0, 1)
	// Get CPkey index
	if arg.CPkeyColDef != nil {
		names := util.SplitCompositePrimaryKeyColumnName(arg.CPkeyColDef.Name)
		for num, colDef := range arg.TargetColDefs {
			for _, name := range names {
				if colDef.Name == name {
					arg.container.pkIndex = append(arg.container.pkIndex, num)
				}
			}
		}
	} else {
		// Get Single Col pk index
		for num, colDef := range arg.TargetColDefs {
			if colDef.Primary {
				arg.container.pkIndex = append(arg.container.pkIndex, num)
				break
			}
		}
	}
}

func (arg *Argument) GetNameNullAbility() bool {
	for i := range arg.TargetColDefs {
		def := arg.TargetColDefs[i]
		arg.container.nameToNullablity[def.Name] = def.Default.NullAbility
		if def.Primary {
			arg.container.pk[def.Name] = true
		}
	}
	if arg.CPkeyColDef != nil {
		def := arg.CPkeyColDef
		arg.container.nameToNullablity[def.Name] = def.Default.NullAbility
		arg.container.pk[def.Name] = true
	}
	if arg.UniqueIndexDef != nil {
		for i := range arg.UniqueIndexDef.Fields {
			for j := range arg.UniqueIndexDef.Fields[i].Cols {
				def := arg.UniqueIndexDef.Fields[i].Cols[j]
				arg.container.nameToNullablity[def.Name] = def.Default.NullAbility
			}
		}
	}
	if arg.ClusterByDef != nil {
		arg.container.nameToNullablity[arg.ClusterByDef.Name] = true
	}
	return false
}
