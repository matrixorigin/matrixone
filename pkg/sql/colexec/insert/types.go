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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Insert)

// const (
// 	Process = iota
// 	End
// )

type container struct {
	state              vm.CtrState
	s3Writer           *colexec.S3Writer
	partitionS3Writers []*colexec.S3Writer // The array is aligned with the partition number array
	buf                *batch.Batch
	affectedRows       uint64

	source           engine.Relation
	partitionSources []engine.Relation // Align array index with the partition number
}

type Insert struct {
	ctr       container
	ToWriteS3 bool // mark if this insert's target is S3 or not.
	InsertCtx *InsertCtx

	vm.OperatorBase
}

func (insert *Insert) GetOperatorBase() *vm.OperatorBase {
	return &insert.OperatorBase
}

func init() {
	reuse.CreatePool[Insert](
		func() *Insert {
			return &Insert{}
		},
		func(a *Insert) {
			*a = Insert{}
		},
		reuse.DefaultOptions[Insert]().
			WithEnableChecker(),
	)
}

func (insert Insert) TypeName() string {
	return opName
}

func NewArgument() *Insert {
	return reuse.Alloc[Insert](nil)
}

func (insert *Insert) Release() {
	if insert != nil {
		reuse.Free[Insert](insert, nil)
	}
}

type InsertCtx struct {
	// insert data into Rel.
	Engine                engine.Engine
	Ref                   *plan.ObjectRef
	AddAffectedRows       bool     // for hidden table, should not update affect Rows
	Attrs                 []string // letter case: origin
	PartitionTableIDs     []uint64 // Align array index with the partition number
	PartitionTableNames   []string // Align array index with the partition number
	PartitionIndexInBatch int      // The array index position of the partition expression column
	TableDef              *plan.TableDef
}

func (insert *Insert) Reset(proc *process.Process, pipelineFailed bool, err error) {
	//@todo need add Reset method for s3Writer
	if insert.ctr.s3Writer != nil {
		insert.ctr.s3Writer.Free(proc)
		insert.ctr.s3Writer = nil
	}
	if insert.ctr.partitionS3Writers != nil {
		for _, writer := range insert.ctr.partitionS3Writers {
			writer.Free(proc)
		}
		insert.ctr.partitionS3Writers = nil
	}
	insert.ctr.state = vm.Build

	if insert.ctr.buf != nil {
		insert.ctr.buf.CleanOnlyData()
	}
}

// The Argument for insert data directly to s3 can not be free when this function called as some datastructure still needed.
// therefore, those argument in remote CN will be free in connector operator, and local argument will be free in mergeBlock operator
func (insert *Insert) Free(proc *process.Process, pipelineFailed bool, err error) {
	if insert.ctr.s3Writer != nil {
		insert.ctr.s3Writer.Free(proc)
		insert.ctr.s3Writer = nil
	}

	// Free the partition table S3writer object resources
	if insert.ctr.partitionS3Writers != nil {
		for _, writer := range insert.ctr.partitionS3Writers {
			writer.Free(proc)
		}
		insert.ctr.partitionS3Writers = nil
	}

	if insert.ctr.buf != nil {
		insert.ctr.buf.Clean(proc.Mp())
		insert.ctr.buf = nil
	}
}

func (insert *Insert) AffectedRows() uint64 {
	return insert.ctr.affectedRows
}

func (insert *Insert) GetAffectedRows() *uint64 {
	return &insert.ctr.affectedRows
}

func (insert *Insert) initBufForS3() {
	attrs := []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}
	insert.ctr.buf = batch.NewWithSize(len(attrs))
	insert.ctr.buf.SetAttributes(attrs)
	insert.ctr.buf.Vecs[0] = vector.NewVec(types.T_int16.ToType())
	insert.ctr.buf.Vecs[1] = vector.NewVec(types.T_text.ToType())
	insert.ctr.buf.Vecs[2] = vector.NewVec(types.T_binary.ToType())
}
