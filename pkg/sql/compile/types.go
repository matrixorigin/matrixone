// Copyright 2023 Matrix Origin
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

package compile

import (
	"context"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type (
	TxnOperator = client.TxnOperator
)

const (
	MinBlockNum = 200
)

type magicType int

// type of scope
const (
	Merge magicType = iota
	Normal
	Remote
	Parallel
	Pushdown
	CreateDatabase
	CreateTable
	CreateIndex
	DropDatabase
	DropTable
	DropIndex
	Deletion
	Insert
	InsertValues
	TruncateTable
	AlterView
	AlterTable
	MergeInsert
	MergeDelete
	CreateSequence
	DropSequence
	AlterSequence
	MagicDelete
)

// Source contains information of a relation which will be used in execution,
type Source struct {
	PushdownId             uint64
	PushdownAddr           string
	SchemaName             string
	RelationName           string
	PartitionRelationNames []string
	Attributes             []string
	R                      engine.Reader
	Bat                    *batch.Batch
	Expr                   *plan.Expr
	TableDef               *plan.TableDef
	Timestamp              timestamp.Timestamp
	AccountId              *plan.PubInfo

	RuntimeFilterReceivers []*colexec.RuntimeFilterChan
}

// Col is the information of attribute
type Col struct {
	Typ  types.T
	Name string
}

// Scope is the output of the compile process.
// Each sql will be compiled to one or more execution unit scopes.
type Scope struct {
	// Magic specifies the type of Scope.
	// 0 -  execution unit for reading data.
	// 1 -  execution unit for processing intermediate results.
	// 2 -  execution unit that requires remote call.
	Magic magicType

	// IsEnd means the pipeline is join
	IsJoin bool

	// IsEnd means the pipeline is end
	IsEnd bool

	// IsRemote means the pipeline is remote
	IsRemote bool

	// IsRemote means the pipeline is load
	IsLoad bool

	Plan *plan.Plan
	// DataSource stores information about data source.
	DataSource *Source
	// PreScopes contains children of this scope will inherit and execute.
	PreScopes []*Scope
	// NodeInfo contains the information about the remote node.
	NodeInfo engine.Node
	// Instructions contains command list of this scope.
	Instructions vm.Instructions
	// Proc contains the execution context.
	Proc *process.Process

	Reg *process.WaitRegister

	RemoteReceivRegInfos []RemoteReceivRegInfo
}

// scopeContext contextual information to assist in the generation of pipeline.Pipeline.
type scopeContext struct {
	id       int32
	plan     *plan.Plan
	scope    *Scope
	root     *scopeContext
	parent   *scopeContext
	children []*scopeContext
	pipe     *pipeline.Pipeline
	regs     map[*process.WaitRegister]int32

	runtimeFilterReceiverMap map[int32]chan *pipeline.RuntimeFilter
}

// anaylze information
type anaylze struct {
	// curr is the current index of plan
	curr      int
	isFirst   bool
	qry       *plan.Query
	analInfos []*process.AnalyzeInfo
}

func (a *anaylze) S3IOInputCount(idx int, count int64) {
	atomic.AddInt64(&a.analInfos[idx].S3IOInputCount, count)
}

func (a *anaylze) S3IOOutputCount(idx int, count int64) {
	atomic.AddInt64(&a.analInfos[idx].S3IOOutputCount, count)
}

func (a *anaylze) Nodes() []*process.AnalyzeInfo {
	return a.analInfos
}

// Compile contains all the information needed for compilation.
type Compile struct {
	scope []*Scope

	pn   *plan.Plan
	info plan2.ExecInfo

	u any
	//fill is a result writer runs a callback function.
	//fill will be called when result data is ready.
	fill func(any, *batch.Batch) error
	//affectRows stores the number of rows affected while insert / update / delete
	affectRows atomic.Uint64
	// cn address
	addr string
	// db current database name.
	db string
	// tenant is the account name.
	tenant string
	// uid the user who initiated the sql.
	uid string
	// sql sql text.
	sql string

	anal *anaylze
	// e db engine instance.
	e   engine.Engine
	ctx context.Context
	// proc stores the execution context.
	proc *process.Process

	cnList engine.Nodes
	// ast
	stmt tree.Statement

	s3CounterSet perfcounter.CounterSet

	stepRegs map[int32][]*process.WaitRegister

	runtimeFilterReceiverMap map[int32]chan *pipeline.RuntimeFilter

	isInternal bool
	// cnLabel is the CN labels which is received from proxy when build connection.
	cnLabel map[string]string
}

type RemoteReceivRegInfo struct {
	Idx      int
	Uuid     uuid.UUID
	FromAddr string
}
