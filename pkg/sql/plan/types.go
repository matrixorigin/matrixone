// Copyright 2021 - 2022 Matrix Origin
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

package plan

import (
	"context"
	"math"

	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	JoinSideNone       int8 = 0
	JoinSideLeft            = 1 << iota
	JoinSideRight           = 1 << iota
	JoinSideBoth            = JoinSideLeft | JoinSideRight
	JoinSideMark            = 1 << iota
	JoinSideCorrelated      = 1 << iota
)

type TableDefType = plan.TableDef_DefType
type TableDef = plan.TableDef
type ColDef = plan.ColDef
type ObjectRef = plan.ObjectRef
type ColRef = plan.ColRef
type Stats = plan.Stats
type Const = plan.Const
type MaxValue = plan.MaxValue
type Expr = plan.Expr
type Node = plan.Node
type RowsetData = plan.RowsetData
type Query = plan.Query
type Plan = plan.Plan
type Type = plan.Type
type Plan_Query = plan.Plan_Query
type Property = plan.Property
type TableDef_DefType_Properties = plan.TableDef_DefType_Properties
type PropertiesDef = plan.PropertiesDef
type ViewDef = plan.ViewDef
type PartitionByDef = plan.PartitionByDef
type ClusterByDef = plan.ClusterByDef
type OrderBySpec = plan.OrderBySpec
type FkColName = plan.FkColName
type ForeignKeyDef = plan.ForeignKeyDef
type ClusterTable = plan.ClusterTable
type PrimaryKeyDef = plan.PrimaryKeyDef
type IndexDef = plan.IndexDef
type SubscriptionMeta = plan.SubscriptionMeta

type CompilerContext interface {
	// Default database/schema in context
	DefaultDatabase() string
	// check if database exist
	DatabaseExists(name string) bool
	// get table definition by database/schema
	Resolve(schemaName string, tableName string) (*ObjectRef, *TableDef)
	// get table definition by table id
	ResolveById(tableId uint64) (*ObjectRef, *TableDef)
	// get the value of variable
	ResolveVariable(varName string, isSystemVar, isGlobalVar bool) (interface{}, error)
	// get the list of the account id
	ResolveAccountIds(accountNames []string) ([]uint32, error)
	// get the relevant information of udf
	ResolveUdf(name string, args []*Expr) (string, error)
	// get the definition of primary key
	GetPrimaryKeyDef(dbName string, tableName string) []*ColDef
	// get needed info for stats by table
	Stats(obj *ObjectRef) bool
	// get origin sql string of the root
	GetRootSql() string
	// get username of current session
	GetUserName() string
	GetAccountId() uint32
	// GetContext get raw context.Context
	GetContext() context.Context
	// GetDatabaseId Get database id
	GetDatabaseId(dbName string) (uint64, error)

	GetProcess() *process.Process

	GetQueryResultMeta(uuid string) ([]*ColDef, string, error)
	SetBuildingAlterView(yesOrNo bool, dbName, viewName string)
	// is building the alter view or not
	// return: yes or no, dbName, viewName
	GetBuildingAlterView() (bool, string, string)
	GetStatsCache() *StatsCache
	GetSubscriptionMeta(dbName string) (*SubscriptionMeta, error)
	CheckSubscriptionValid(subName, accName string, pubName string) error
	SetQueryingSubscription(meta *SubscriptionMeta)
	GetQueryingSubscription() *SubscriptionMeta
	IsPublishing(dbName string) (bool, error)
}

type Optimizer interface {
	Optimize(stmt tree.Statement) (*Query, error)
	CurrentContext() CompilerContext
}

type Rule interface {
	Match(*Node) bool                      // rule match?
	Apply(*Node, *Query, *process.Process) // apply the rule
}

// BaseOptimizer is base optimizer, capable of handling only a few simple rules
type BaseOptimizer struct {
	qry   *Query
	rules []Rule
	ctx   CompilerContext
}

type ViewData struct {
	Stmt            string
	DefaultDatabase string
}

type ExecType int

const (
	ExecTypeAP ExecType = iota
	ExecTypeTP
)

type ExecInfo struct {
	Typ        ExecType
	WithGPU    bool
	WithBigMem bool
	CnNumbers  int
}

///////////////////////////////
// Data structures for refactor
///////////////////////////////

type QueryBuilder struct {
	qry     *plan.Query
	compCtx CompilerContext

	ctxByNode    []*BindContext
	nameByColRef map[[2]int32]string

	tag2Table map[int32]*TableDef

	nextTag int32

	isPrepareStatement bool
	mysqlCompatible    bool
	haveOnDuplicateKey bool // if it's a plan contain onduplicate key node, we can not use some optmize rule
	isForUpdate        bool // if it's a query plan for update
}

type CTERef struct {
	defaultDatabase string
	ast             *tree.CTE
	maskedCTEs      map[string]any
}

type BindContext struct {
	binder Binder

	cteByName  map[string]*CTERef
	maskedCTEs map[string]any

	cteName  string
	headings []string

	groupTag     int32
	aggregateTag int32
	projectTag   int32
	resultTag    int32
	windowTag    int32

	groups     []*plan.Expr
	aggregates []*plan.Expr
	projects   []*plan.Expr
	results    []*plan.Expr
	windows    []*plan.Expr

	groupByAst     map[string]int32
	aggregateByAst map[string]int32
	windowByAst    map[string]int32
	projectByExpr  map[string]int32

	aliasMap map[string]int32

	bindings       []*Binding
	bindingByTag   map[int32]*Binding //rel_pos
	bindingByTable map[string]*Binding
	bindingByCol   map[string]*Binding

	// for join tables
	bindingTree *BindingTreeNode

	isDistinct   bool
	isCorrelated bool
	hasSingleRow bool

	parent     *BindContext
	leftChild  *BindContext
	rightChild *BindContext

	defaultDatabase string
}

type NameTuple struct {
	table string
	col   string
}

type BindingTreeNode struct {
	using []NameTuple

	binding *Binding

	left  *BindingTreeNode
	right *BindingTreeNode
}

type Binder interface {
	BindExpr(tree.Expr, int32, bool) (*plan.Expr, error)
	BindColRef(*tree.UnresolvedName, int32, bool) (*plan.Expr, error)
	BindAggFunc(string, *tree.FuncExpr, int32, bool) (*plan.Expr, error)
	BindWinFunc(string, *tree.FuncExpr, int32, bool) (*plan.Expr, error)
	BindSubquery(*tree.Subquery, bool) (*plan.Expr, error)
	GetContext() context.Context
}

type baseBinder struct {
	sysCtx    context.Context
	builder   *QueryBuilder
	ctx       *BindContext
	impl      Binder
	boundCols []string
}

type DefaultBinder struct {
	baseBinder
	typ  *Type
	cols []string
}

type UpdateBinder struct {
	baseBinder
	cols []*ColDef
}

type TableBinder struct {
	baseBinder
}

type WhereBinder struct {
	baseBinder
}

type GroupBinder struct {
	baseBinder
}

type HavingBinder struct {
	baseBinder
	insideAgg bool
}

type ProjectionBinder struct {
	baseBinder
	havingBinder *HavingBinder
}

type OrderBinder struct {
	*ProjectionBinder
	selectList tree.SelectExprs
}

type LimitBinder struct {
	baseBinder
}

type PartitionBinder struct {
	baseBinder
}

// SetBinder for 'set @var = expr'
type SetBinder struct {
	baseBinder
}

var _ Binder = (*TableBinder)(nil)
var _ Binder = (*WhereBinder)(nil)
var _ Binder = (*GroupBinder)(nil)
var _ Binder = (*HavingBinder)(nil)
var _ Binder = (*ProjectionBinder)(nil)
var _ Binder = (*LimitBinder)(nil)
var _ Binder = (*PartitionBinder)(nil)
var _ Binder = (*UpdateBinder)(nil)

var Sequence_cols_name = []string{"last_seq_num", "min_value", "max_value", "start_value", "increment_value", "cycle", "is_called"}

const (
	NotFound      int32 = math.MaxInt32
	AmbiguousName int32 = math.MinInt32
)

type Binding struct {
	tag            int32
	nodeId         int32
	table          string
	tableID        uint64
	cols           []string
	colIsHidden    []bool
	types          []*plan.Type
	refCnts        []uint
	colIdByName    map[string]int32
	isClusterTable bool
}

const (
	maxLengthOfTableComment  int = 2048
	maxLengthOfColumnComment int = 1024
)
