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
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/relation"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"
)

const (
	Normal = iota
	Merge
	Remote
	Insert
	Explain
	DropTable
	DropDatabase
	CreateTable
	CreateDatabase
	ShowTables
	ShowDatabases
)

// Source contains information of a relation which will be used in execution,
type Source struct {
	RelationName string
	DBName       string
	// RefCount records the reference of the columns in current relation.
	RefCount map[string]uint64
	// Segments contains the segment list of input data.
	Segments []*relation.Segment
}

// Scope is the output of the compile process.
// Each sql will be compiled to one or more execution unit scopes.
type Scope struct {
	// Magic specifies the type of Scope.
	// 0 -  execution unit for reading data.
	// 1 -  execution unit for processing intermediate results.
	// 2 -  execution unit that requires remote call.
	Magic int
	// Operator Algebra operator.
	Operator op.OP
	// DataSource stores information about data source.
	DataSource *Source
	// PreScopes contains children of this scope will inherit and execute.
	PreScopes []*Scope
	// NodeInfo contains the information about the remote node.
	NodeInfo metadata.Node
	// Instructions contains command list of this scope.
	Instructions vm.Instructions
	// Proc contains the execution context.
	Proc *process.Process
}

type Col struct {
	Typ  types.T
	Name string
}

// Exec stores all information related to the execution phase of a single sql.
type Exec struct {
	//err stores err information if error occurred during execution.
	err error
	//resultCols stores the column information of result.
	resultCols []*Col
	scopes     []*Scope
	c          *compile
	//affectRows stores the number of rows affected while insert / update / delete
	affectRows uint64
	//e a dbengine instance
	e engine.Engine
	//stmt ast of a single sql
	stmt tree.Statement
	u    interface{}
	//fill is a result writer runs a callback function.
	//fill will be called when result data is ready.
	fill func(interface{}, *batch.Batch) error
}

// compile contains all the information needed for compilation.
type compile struct {
	// db current database name.
	db string
	// uid the user who initiated the sql.
	uid string
	// sql sql text.
	sql string
	// e db engine instance.
	e engine.Engine
	// proc stores the execution context.
	proc *process.Process
}
