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
	Cost(obj *ObjectRef, e *Expr) *Cost //change Cost to *Cost to fixed "return copies lock value" warning in new proto code generated
}

type Optimizer interface {
	Optimize(stmt tree.Statement) (*Query, error) //todo confirm interface change
	CurrentContext() CompilerContext
}

//use for build select
type SelectContext struct {
	//when build_from we may set tableAlias and then use in build_where
	//when build_projection we may set columnAlias and then use in build_orderby
	tableAlias  map[string]string
	columnAlias map[string]*plan.Expr

	//use for build subquery
	subQueryIsCorrelated bool
	subQueryParentId     int32
}
