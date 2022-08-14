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

package compile

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// type of scope
const (
	Merge = iota
	Normal
	Remote
	Parallel
	CreateDatabase
	CreateTable
	CreateIndex
	DropDatabase
	DropTable
	DropIndex
	Deletion
	Insert
	Update
	InsertValues
)

type EncodeSource struct {
	SchemaName   string
	RelationName string
	Attributes   []string
	Bat          *batch.Batch
}

// Source contains information of a relation which will be used in execution,
type Source struct {
	SchemaName   string
	RelationName string
	Attributes   []string
	R            engine.Reader
	Bat          *batch.Batch
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
	Magic int

	// IsEnd means the pipeline is end
	IsEnd bool

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
}

// anaylze information
type anaylze struct {
	// curr is the current index of plan
	curr      int
	qry       *plan.Query
	analInfos []*process.AnalyzeInfo
}

// Compile contains all the information needed for compilation.
type Compile struct {
	scope *Scope

	info plan2.ExecInfo

	u any
	//fill is a result writer runs a callback function.
	//fill will be called when result data is ready.
	fill func(any, *batch.Batch) error
	//affectRows stores the number of rows affected while insert / update / delete
	affectRows uint64
	// db current database name.
	db string
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
}
