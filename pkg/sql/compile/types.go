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
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
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

type magicType int

// type of scope
const (
	Merge magicType = iota
	Normal
	Remote
	Parallel
	CreateDatabase
	CreateTable
	CreateIndex
	DropDatabase
	DropTable
	DropIndex
	TruncateTable
	AlterView
	AlterTable
	MergeInsert
	MergeDelete
	CreateSequence
	DropSequence
	AlterSequence
	Replace
)

// Source contains information of a relation which will be used in execution.
type Source struct {
	isConst bool

	PushdownId             uint64
	PushdownAddr           string
	SchemaName             string
	RelationName           string
	PartitionRelationNames []string
	Attributes             []string
	R                      engine.Reader
	Bat                    *batch.Batch
	FilterExpr             *plan.Expr // todo: change this to []*plan.Expr
	node                   *plan.Node
	TableDef               *plan.TableDef
	Timestamp              timestamp.Timestamp
	AccountId              *plan.PubInfo

	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
	OrderBy            []*plan.OrderBySpec // for ordered scan
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

	// IsJoin means the pipeline is join
	IsJoin bool

	// IsEnd means the pipeline is end
	IsEnd bool

	// IsRemote means the pipeline is remote
	IsRemote bool

	// IsLoad means the pipeline is load
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

	BuildIdx   int
	ShuffleCnt int

	PartialResults     []any
	PartialResultTypes []types.T
}

// canRemote checks whether the current scope can be executed remotely.
func (s *Scope) canRemote(c *Compile, checkAddr bool) bool {
	// check the remote address.
	// if it was empty or equal to the current address, return false.
	if checkAddr {
		if len(s.NodeInfo.Addr) == 0 || len(c.addr) == 0 {
			return false
		}
		if isSameCN(c.addr, s.NodeInfo.Addr) {
			return false
		}
	}

	// some operators cannot be remote.
	// todo: it is not a good way to check the operator type here.
	//  cannot generate this remote pipeline if the operator type is not supported.
	for _, op := range s.Instructions {
		if op.CannotRemote() {
			return false
		}
	}
	for _, pre := range s.PreScopes {
		if !pre.canRemote(c, false) {
			return false
		}
	}
	return true
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

func (a anaylze) TypeName() string {
	return "compile.anaylze"
}

func newAnaylze() *anaylze {
	return reuse.Alloc[anaylze](nil)
}

func (a *anaylze) release() {
	// there are 3 situations to release analyzeInfo
	// 1 is free analyzeInfo of Local CN when release analyze
	// 2 is free analyzeInfo of remote CN before transfer back
	// 3 is free analyzeInfo of remote CN when errors happen before transfer back
	// this is situation 1
	for i := range a.analInfos {
		reuse.Free[process.AnalyzeInfo](a.analInfos[i], nil)
	}
	reuse.Free[anaylze](a, nil)
}

// Compile contains all the information needed for compilation.
type Compile struct {
	scope []*Scope

	pn *plan.Plan

	execType plan2.ExecType

	// fill is a result writer runs a callback function.
	// fill will be called when result data is ready.
	fill func(*batch.Batch) error
	// affectRows stores the number of rows affected while insert / update / delete
	affectRows *atomic.Uint64
	// cn address
	addr string
	// db current database name.
	db string
	// tenant is the account name.
	tenant string
	// uid the user who initiated the sql.
	uid string
	// sql sql text.
	sql       string
	originSQL string

	anal *anaylze
	// e db engine instance.
	e   engine.Engine
	ctx context.Context
	// proc stores the execution context.
	proc *process.Process

	MessageBoard *process.MessageBoard

	cnList engine.Nodes
	// ast
	stmt tree.Statement

	counterSet *perfcounter.CounterSet

	nodeRegs map[[2]int32]*process.WaitRegister
	stepRegs map[int32][][2]int32

	lock *sync.RWMutex

	isInternal bool

	// cnLabel is the CN labels which is received from proxy when build connection.
	cnLabel map[string]string

	buildPlanFunc func() (*plan2.Plan, error)
	startAt       time.Time
	// use for duplicate check
	fuzzys []*fuzzyCheck

	needLockMeta bool
	metaTables   map[string]struct{}
	lockTables   map[uint64]*plan.LockTarget
	disableRetry bool

	lastAllocID int32
}

type RemoteReceivRegInfo struct {
	Idx      int
	Uuid     uuid.UUID
	FromAddr string
}

type fuzzyCheck struct {
	db        string
	tbl       string
	attr      string
	condition string

	// handle with primary key(a, b, ...) or unique key (a, b, ...)
	isCompound bool

	// handle with cases like create a unique index for existed table, or alter add unique key
	// and the type of unique key is compound
	onlyInsertHidden bool

	col          *plan.ColDef
	compoundCols []*plan.ColDef

	cnt int
}

type MultiTableIndex struct {
	IndexAlgo string
	IndexDefs map[string]*plan.IndexDef
}
