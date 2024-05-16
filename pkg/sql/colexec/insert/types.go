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
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Argument)

// const (
// 	Process = iota
// 	End
// )

type container struct {
	state              vm.CtrState
	s3Writer           *colexec.S3Writer
	partitionS3Writers []*colexec.S3Writer // The array is aligned with the partition number array
	buf                *batch.Batch
}

type Argument struct {
	ctr          *container
	affectedRows uint64
	ToWriteS3    bool // mark if this insert's target is S3 or not.
	InsertCtx    *InsertCtx

	vm.OperatorBase
}

func (arg *Argument) GetOperatorBase() *vm.OperatorBase {
	return &arg.OperatorBase
}

func init() {
	reuse.CreatePool[Argument](
		func() *Argument {
			return &Argument{}
		},
		func(a *Argument) {
			*a = Argument{}
		},
		reuse.DefaultOptions[Argument]().
			WithEnableChecker(),
	)
}

func (arg Argument) TypeName() string {
	return argName
}

func NewArgument() *Argument {
	return reuse.Alloc[Argument](nil)
}

func (arg *Argument) Release() {
	if arg != nil {
		reuse.Free[Argument](arg, nil)
	}
}

type InsertCtx struct {
	// insert data into Rel.
	Rel                   engine.Relation
	Ref                   *plan.ObjectRef
	AddAffectedRows       bool
	Attrs                 []string
	PartitionTableIDs     []uint64          // Align array index with the partition number
	PartitionTableNames   []string          // Align array index with the partition number
	PartitionIndexInBatch int               // The array index position of the partition expression column
	PartitionSources      []engine.Relation // Align array index with the partition number
	TableDef              *plan.TableDef
}

func (arg *Argument) Reset(proc *process.Process, pipelineFailed bool, err error) {
	arg.Free(proc, pipelineFailed, err)
}

// The Argument for insert data directly to s3 can not be free when this function called as some datastructure still needed.
// therefore, those argument in remote CN will be free in connector operator, and local argument will be free in mergeBlock operator
func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	if arg.ctr != nil {
		if arg.ctr.s3Writer != nil {
			arg.ctr.s3Writer.Free(proc)
			arg.ctr.s3Writer = nil
		}

		// Free the partition table S3writer object resources
		if arg.ctr.partitionS3Writers != nil {
			for _, writer := range arg.ctr.partitionS3Writers {
				writer.Free(proc)
			}
			arg.ctr.partitionS3Writers = nil
		}

		if arg.ctr.buf != nil {
			arg.ctr.buf.Clean(proc.Mp())
			arg.ctr.buf = nil
		}

		arg.ctr = nil
	}
}

func (arg *Argument) AffectedRows() uint64 {
	return arg.affectedRows
}

func (arg *Argument) GetAffectedRows() *uint64 {
	return &arg.affectedRows
}
