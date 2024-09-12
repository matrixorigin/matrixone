// Copyright 2021-2024 Matrix Origin
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

package multi_update

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(MultiUpdate)

const opName = "MultiUpdate"

type updateTableType int

const (
	MainTable           updateTableType = iota
	UniqueIndexTable    updateTableType = iota
	SecondaryIndexTable updateTableType = iota
)

func init() {
	reuse.CreatePool[MultiUpdate](
		func() *MultiUpdate {
			return &MultiUpdate{}
		},
		func(a *MultiUpdate) {
			*a = MultiUpdate{}
		},
		reuse.DefaultOptions[MultiUpdate]().
			WithEnableChecker(),
	)
}

type MultiUpdate struct {
	ctr            container
	MultiUpdateCtx []*MultiUpdateCtx

	ToWriteS3              bool
	IsOnduplicateKeyUpdate bool

	Engine engine.Engine

	vm.OperatorBase
}

type container struct {
	state        vm.CtrState
	s3Writer     *colexec.S3Writer
	affectedRows uint64

	insertBuf []*batch.Batch
	deleteBuf []*batch.Batch
}

type MultiUpdateCtx struct {
	ref      *plan.ObjectRef
	tableDef *plan.TableDef

	tableType updateTableType

	insertCols []int
	deleteCols []int

	partitionTableIDs   []int32  // Align array index with the partition number
	partitionTableNames []string // Align array index with the partition number
	partitionIdx        int      // The array index position of the partition expression column

	source           engine.Relation
	partitionSources []engine.Relation // Align array index with the partition number
}

func (update MultiUpdate) TypeName() string {
	return opName
}

func NewArgument() *MultiUpdate {
	return reuse.Alloc[MultiUpdate](nil)
}

func (update *MultiUpdate) Release() {
	if update != nil {
		reuse.Free[MultiUpdate](update, nil)
	}
}

func (update *MultiUpdate) GetOperatorBase() *vm.OperatorBase {
	return &update.OperatorBase
}

func (update *MultiUpdate) Reset(proc *process.Process, pipelineFailed bool, err error) {
}

func (update *MultiUpdate) Free(proc *process.Process, pipelineFailed bool, err error) {
}

func (update *MultiUpdate) GetAffectedRows() *uint64 {
	return &update.ctr.affectedRows
}
