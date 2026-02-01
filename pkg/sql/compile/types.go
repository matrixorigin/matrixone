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
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	pipeline2 "github.com/matrixorigin/matrixone/pkg/vm/pipeline"
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
	CreateDatabase
	CreateTable
	CreatePitr
	CreateCDC
	CreateView
	CreateIndex
	DropDatabase
	DropTable
	DropPitr
	DropCDC
	DropIndex
	TruncateTable
	AlterView
	AlterTable
	RenameTable
	MergeInsert
	MergeDelete
	CreateSequence
	DropSequence
	AlterSequence
	Replace
	TableClone
)

func (m magicType) String() string {
	switch m {
	case Merge:
		return "Merge"
	case Normal:
		return "Normal"
	case Remote:
		return "Remote"
	case CreateDatabase:
		return "CreateDatabase"
	case CreateTable:
		return "CreateTable"
	case CreateView:
		return "CreateView"
	case CreateIndex:
		return "CreateIndex"
	case DropDatabase:
		return "DropDatabase"
	case DropTable:
		return "DropTable"
	case DropIndex:
		return "DropIndex"
	case TruncateTable:
		return "TruncateTable"
	case AlterView:
		return "AlterView"
	case AlterTable:
		return "AlterTable"
	case MergeInsert:
		return "MergeInsert"
	case MergeDelete:
		return "MergeDelete"
	case CreateSequence:
		return "CreateSequence"
	case DropSequence:
		return "DropSequence"
	case AlterSequence:
		return "AlterSequence"
	case Replace:
		return "Replace"
	default:
		return "Unknown"
	}
}

// Source contains information of a relation which will be used in execution.
type Source struct {
	isConst bool

	PushdownId      uint64
	PushdownAddr    string
	SchemaName      string
	RelationName    string
	Attributes      []string
	R               engine.Reader
	Rel             engine.Relation
	FilterExpr      *plan.Expr   // todo: change this to []*plan.Expr,  is FilterList + RuntimeFilter
	FilterList      []*plan.Expr //from node.FilterList, use for reader
	BlockFilterList []*plan.Expr //from node.BlockFilterList, use for range
	node            *plan.Node
	TableDef        *plan.TableDef
	Timestamp       timestamp.Timestamp
	AccountId       *plan.PubInfo

	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
	OrderBy            []*plan.OrderBySpec // for ordered scan

	IndexReaderParam *plan.IndexReaderParam

	RecvMsgList []plan.MsgHeader
	BloomFilter []byte
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

	// IsEnd means the pipeline is end
	IsEnd bool

	// IsRemote means the pipeline is remote
	IsRemote bool

	// IsLoad means the pipeline is load
	IsLoad bool

	// IsTbFunc means the leaf op of pipeline is tablefunction, tablefunction is src op
	IsTbFunc bool

	HasPartialResults bool
	// StarCountOnly: when true, aggOptimize took the single-starcount fast path.
	// buildReaders should return EmptyReaders and no data should flow.
	StarCountOnly bool
	// StarCountMergeGroup: set when StarCountOnly is true; resetForReuse clears its PartialResults.
	StarCountMergeGroup *group.MergeGroup

	Plan *plan.Plan
	// DataSource stores information about data source.
	DataSource *Source
	// PreScopes contains children of this scope will inherit and execute.
	PreScopes []*Scope
	// NodeInfo contains the information about the remote node.
	NodeInfo engine.Node
	// TxnOffset represents the transaction's write offset, specifying the starting position for reading data.
	TxnOffset int
	// Instructions contains command list of this scope.
	// Instructions vm.Instructions
	RootOp vm.Operator
	// Proc contains the execution context.
	Proc *process.Process

	ScopeAnalyzer *ScopeAnalyzer

	RemoteReceivRegInfos []RemoteReceivRegInfo
}

// ipAddrMatch return true if the node-addr of the scope matches to local address.
//
// once node-addr is just empty, it means local.
func (s *Scope) ipAddrMatch(local string) bool {
	if len(s.NodeInfo.Addr) == 0 {
		return true
	}
	return isSameCN(s.NodeInfo.Addr, local)
}

// holdAnyCannotRemoteOperator returns error message
// if this pipeline holds any operator that cannot send to a remote node for running.
//
// For now,
// we are only not support to run recursiveCTE on remote node.
// so we do a quick check here.
// If more operators need to be rejected in the future, please use recursion honestly to check each operator.
func (s *Scope) holdAnyCannotRemoteOperator() error {
	if _, isCTE := pipeline2.IsCtePipelineAtLoop(s.RootOp); isCTE {
		return moerr.NewInternalErrorNoCtx("remote running of cyclic CTE is not supported.")
	}

	for _, pre := range s.PreScopes {
		if err := pre.holdAnyCannotRemoteOperator(); err != nil {
			return err
		}
	}
	return nil
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

// Compile contains all the information needed for compilation.
type Compile struct {
	scopes []*Scope

	pn *plan.Plan

	execType plan2.ExecType

	// fill is a result writer runs a callback function.
	// fill will be called when result data is ready.
	fill func(*batch.Batch, *perfcounter.CounterSet) error
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

	retryTimes int
	anal       *AnalyzeModule
	// e db engine instance.
	e engine.Engine

	// proc stores the execution context.
	proc *process.Process
	// runSqlToken tracks the current statement in txn operator coordination.
	runSqlToken uint64
	// TxnOffset read starting offset position within the transaction during the execute current statement
	TxnOffset int

	MessageBoard *message.MessageBoard

	cnList engine.Nodes
	// ast
	stmt tree.Statement

	counterSet *perfcounter.CounterSet

	nodeRegs map[[2]int32]*process.WaitRegister
	stepRegs map[int32][][2]int32

	// cnLabel is the CN labels which is received from proxy when build connection.
	cnLabel map[string]string

	buildPlanFunc func(ctx context.Context) (*plan2.Plan, error)
	startAt       time.Time
	// use for duplicate check
	fuzzys []*fuzzyCheck

	lockMeta   *LockMeta
	lockTables map[uint64]*plan.LockTarget

	filterExprExes []colexec.ExpressionExecutor

	needLockMeta bool
	needBlock    bool
	isPrepare    bool
	disableRetry bool
	isInternal   bool
	hasMergeOp   bool

	// ncpu set as system.GoRoutines() while NewCompile, instead of global static value.
	ncpu int

	adjustTableExtraFunc     func(*api.SchemaExtra) error
	disableDropAutoIncrement bool
	keepAutoIncrement        uint64
	ignorePublish            bool
	ignoreCheckExperimental  bool
	disableLock              bool
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

// ----------------------------------------------------------------------------------------------------------------

type ScopeAnalyzer struct {
	start        time.Time // Records the start time when the analyzer begins
	isStarted    bool      // Indicates whether the analyzer has started
	isStoped     bool      // Indicates whether the analyzer has stopped
	TimeConsumed int64     // Stores the total time consumed between Start and Stop in nanoseconds
}

// Start begins the time tracking. It will not start if it has already started or if it has been stopped.
func (sa *ScopeAnalyzer) Start() {
	if sa.isStarted {
		return
	}
	// Set the start time to the current time and mark the analyzer as started
	sa.start = time.Now()
	sa.isStarted = true
}

// Stop halts the time tracking and calculates the duration.
// It won't perform any actions if it has not started or if it has already been stopped.
func (sa *ScopeAnalyzer) Stop() {
	if sa.isStoped || !sa.isStarted {
		return
	}
	// Calculate the time duration since start and store it in TimeConsumed
	duration := time.Since(sa.start)
	sa.TimeConsumed = duration.Nanoseconds()
	sa.isStoped = true
}

// Reset clears the analyzer's state, allowing it to start again.
// Both isStarted and isStoped flags are reset.
func (sa *ScopeAnalyzer) Reset() {
	sa.TimeConsumed = 0
	sa.isStoped = false
	sa.isStarted = false
}

func NewScopeAnalyzer() *ScopeAnalyzer {
	return &ScopeAnalyzer{}
}
