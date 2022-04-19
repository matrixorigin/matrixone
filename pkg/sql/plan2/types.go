package plan2

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type TableDef plan.TableDef
type ObjectRef plan.ObjectRef
type Cost plan.Cost
type Const plan.Const
type Expr plan.Expr
type Node plan.Node
type RowsetData plan.RowsetData
type Query plan.Query

type CompilerContext interface {
	Resolve(name string) (*plan.ObjectRef, *plan.TableDef)
	Cost(obj *ObjectRef, e *Expr) Cost
}

type Optimizer interface {
	Optimize(stmt tree.Statement) (*Query, error) //todo confirm interface change
	CurrentContext() CompilerContext
}

type AliasContext struct {
	tableAlias  map[string]*plan.TableDef
	columnAlias map[string]*plan.Expr
}
