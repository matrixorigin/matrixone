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

package plan2

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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

//use for build select
type BinderContext struct {
	// when build_projection we may set columnAlias and then use in build_orderby
	columnAlias map[string]*Expr
	// when build_cte will set cteTables and use in build_from
	cteTables map[string]*TableDef

	// use for build subquery
	subqueryIsCorrelated bool
	// unused, commented out for now.
	// subqueryIsScalar     bool

	subqueryParentIds []int32

	// use to storage the using columns.
	// select R.*, S.* from R, S using(a) where S.a > 10
	// then we store {'a':'S'},
	// when we use buildUnresolvedName(), and the colName = 'a' and tableName = 'S', we reset tableName=''
	// because the ProjectNode(after JoinNode) had coalesced the using cols
	usingCols map[string]string
}
