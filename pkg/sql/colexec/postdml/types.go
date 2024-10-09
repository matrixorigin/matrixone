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

package postdml

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(PostDml)

type container struct {
	state            vm.CtrState
	source           engine.Relation
	partitionSources []engine.Relation // Align array index with the partition number
	affectedRows     uint64
}

type PostDml struct {
	ctr        container
	PostDmlCtx *PostDmlCtx

	vm.OperatorBase
}

func (postdml *PostDml) GetOperatorBase() *vm.OperatorBase {
	return &postdml.OperatorBase
}

func init() {
	reuse.CreatePool[PostDml](
		func() *PostDml {
			return &PostDml{}
		},
		func(a *PostDml) {
			*a = PostDml{}
		},
		reuse.DefaultOptions[PostDml]().
			WithEnableChecker(),
	)
}

func (postdml PostDml) TypeName() string {
	return opName
}

func NewArgument() *PostDml {
	return reuse.Alloc[PostDml](nil)
}

func (postdml *PostDml) Release() {
	if postdml != nil {
		reuse.Free[PostDml](postdml, nil)
	}
}

type PostDmlFullTextCtx struct {
	IsDelete        bool
	IsInsert        bool
	SourceTableName string
	IndexTableName  string
	PkeyIdx         int
	PkeyName        string
	Parts           []string
	AlgoParams      string
}

type PostDmlCtx struct {
	Ref                   *plan.ObjectRef
	AddAffectedRows       bool
	Engine                engine.Engine
	PartitionTableIDs     []uint64 // Align array index with the partition number
	PartitionTableNames   []string // Align array index with the partition number
	PartitionIndexInBatch int      // The array index position of the partition expression column

	// define various context for different tasks
	FullText *PostDmlFullTextCtx
}

func (postdml *PostDml) Reset(proc *process.Process, pipelineFailed bool, err error) {

	postdml.ctr.state = vm.Build
}

func (postdml *PostDml) Free(proc *process.Process, pipelineFailed bool, err error) {
}

func (postdml *PostDml) AffectedRows() uint64 {
	return postdml.ctr.affectedRows
}

func (postdml *PostDml) GetAffectedRows() *uint64 {
	return &postdml.ctr.affectedRows
}
