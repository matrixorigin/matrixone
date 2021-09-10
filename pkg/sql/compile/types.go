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

type Source struct {
	ID   string
	DB   string
	Refs map[string]uint64
	Segs []*relation.Segment
}

type Scope struct {
	Magic int
	O     op.OP
	Data  *Source
	Ss    []*Scope
	N     metadata.Node
	Ins   vm.Instructions
	Proc  *process.Process
}

type Col struct {
	Typ  types.T
	Name string
}

type Exec struct {
	err  error
	cs   []*Col
	ss   []*Scope
	c    *compile
	e    engine.Engine
	stmt tree.Statement
	u    interface{}
	fill func(interface{}, *batch.Batch) error
}

type compile struct {
	port int
	db   string
	uid  string
	sql  string
	e    engine.Engine
	ns   metadata.Nodes
	proc *process.Process
}
