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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// PostDml is Post action right after pipeline of INSERT/UPDATE/DELETE finished.
// For fulltext search, it will grab all primary keys from the scan sink and generate a DELETE and/or INSERT SQL for index table to proc.Base.PostDmlSqlList
// Eventually, All SQLs in proc.Base.PostDmlSqlList will be executed after the pipeline finished.
// You can define your own context and add the post action handler in PostDml for your own Post DML action in this Node.
var _ vm.Operator = new(PostDml)

type container struct {
	affectedRows uint64
}

type PostDml struct {
	ctr        container
	PostDmlCtx *PostDmlCtx
	cycleCheck *replaceCycleCheckConfig

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
	SourceTableName string
	IndexTableName  string
	Parts           []string
	AlgoParams      string
}

type PostDmlCtx struct {
	Ref                    *plan.ObjectRef
	AddAffectedRows        bool
	PrimaryKeyIdx          int32
	PrimaryKeyName         string
	IsDelete               bool
	IsInsert               bool
	IsDeleteWithoutFilters bool
	ReplaceCycleCheck      string

	// define various context for different tasks
	FullText *PostDmlFullTextCtx
}

type replaceCycleCheckColumn struct {
	Name string `json:"name"`
	Pos  int32  `json:"pos"`
}

type replaceCycleCheckFK struct {
	ParentSchema string   `json:"parent_schema"`
	ParentTable  string   `json:"parent_table"`
	ChildCols    []string `json:"child_cols"`
	ParentCols   []string `json:"parent_cols"`
}

type replaceCycleCheckConfig struct {
	ChildSchema string                    `json:"child_schema"`
	ChildTable  string                    `json:"child_table"`
	PrimaryKey  []replaceCycleCheckColumn `json:"primary_key"`
	ForeignKeys []replaceCycleCheckFK     `json:"foreign_keys"`
}

func (postdml *PostDml) Reset(proc *process.Process, pipelineFailed bool, err error) {
	postdml.cycleCheck = nil
}

func (postdml *PostDml) Free(proc *process.Process, pipelineFailed bool, err error) {
}

func (postdml *PostDml) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (postdml *PostDml) AffectedRows() uint64 {
	return postdml.ctr.affectedRows
}

func (postdml *PostDml) GetAffectedRows() *uint64 {
	return &postdml.ctr.affectedRows
}
