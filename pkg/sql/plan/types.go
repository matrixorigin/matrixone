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
	"math"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	JoinSideNone       int8 = 0
	JoinSideLeft            = 1 << iota
	JoinSideRight           = 1 << iota
	JoinSideBoth            = JoinSideLeft | JoinSideRight
	JoinSideCorrelated      = 1 << iota
)

type TableDef = plan.TableDef
type ColDef = plan.ColDef
type ObjectRef = plan.ObjectRef
type ColRef = plan.ColRef
type Cost = plan.Cost
type Const = plan.Const
type Expr = plan.Expr
type Node = plan.Node
type RowsetData = plan.RowsetData
type Query = plan.Query
type Plan = plan.Plan
type Type = plan.Type
type Plan_Query = plan.Plan_Query

type CompilerContext interface {
	// Default database/schema in context
	DefaultDatabase() string
	// check if database exist
	DatabaseExists(name string) bool
	// get table definition by database/schema
	Resolve(schemaName string, tableName string) (*ObjectRef, *TableDef)
	// get the value of variable
	ResolveVariable(varName string, isSystemVar, isGlobalVar bool) (interface{}, error)
	// get the definition of primary key
	GetPrimaryKeyDef(dbName string, tableName string) []*ColDef
	// get the definition of hide key
	GetHideKeyDef(dbName string, tableName string) *ColDef
	// get estimated cost by table & expr
	Cost(obj *ObjectRef, e *Expr) *Cost
}

type Optimizer interface {
	Optimize(stmt tree.Statement) (*Query, error)
	CurrentContext() CompilerContext
}

type Rule interface {
	Match(*Node) bool    // rule match?
	Apply(*Node, *Query) // apply the rule
}

// BaseOptimizer is base optimizer, capable of handling only a few simple rules
type BaseOptimizer struct {
	qry   *Query
	rules []Rule
	ctx   CompilerContext
}

///////////////////////////////
// Data structures for refactor
///////////////////////////////

type QueryBuilder struct {
	qry     *plan.Query
	compCtx CompilerContext

	ctxByNode    []*BindContext
	nameByColRef map[[2]int32]string

	nextTag int32
}

type CTERef struct {
	ast        *tree.CTE
	maskedCTEs map[string]any
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

	groups     []*plan.Expr
	aggregates []*plan.Expr
	projects   []*plan.Expr
	results    []*plan.Expr

	groupByAst     map[string]int32
	aggregateByAst map[string]int32
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
}

type baseBinder struct {
	builder   *QueryBuilder
	ctx       *BindContext
	impl      Binder
	boundCols []string
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

var _ Binder = (*TableBinder)(nil)
var _ Binder = (*WhereBinder)(nil)
var _ Binder = (*GroupBinder)(nil)
var _ Binder = (*HavingBinder)(nil)
var _ Binder = (*ProjectionBinder)(nil)
var _ Binder = (*LimitBinder)(nil)

const (
	NotFound      int32 = math.MaxInt32
	AmbiguousName int32 = math.MinInt32
)

type Binding struct {
	tag         int32
	nodeId      int32
	table       string
	cols        []string
	types       []*plan.Type
	refCnts     []uint
	colIdByName map[string]int32
}

const (
	maxLengthOfTableComment  int = 2048
	maxLengthOfColumnComment int = 1024
)
