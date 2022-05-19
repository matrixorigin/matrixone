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

package compile2

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	Merge = iota
	CreateDatabase
)

// Address is the ip:port of local node
var Address string

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

	Plan *plan.Plan
}

// Exec stores all information related to the execution phase of a single sql.
type Exec struct {
	//err stores err information if error occurred during execution.
	//	err error
	//resultCols stores the column information of result.
	resultCols []*Col
	scope      *Scope
	c          *compile
	//affectRows stores the number of rows affected while insert / update / delete
	affectRows uint64
	//e is a db engine instance
	e engine.Engine
	//stmt ast of a single sql
	stmt tree.Statement

	u interface{}
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
