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

package tree

import "fmt"

type CreateSequence struct {
	statementImpl

	Temporary   bool
	Name        *TableName
	IfNotExists bool
	IncrementBy *IncrementByOption
	MinValue    *MinValueOption
	MaxValue    *MaxValueOption
	StartWith   *StartWithOption
	Cache       *CacheOption
	Cycle       bool
	OwnedBy     *OwnedByOption
}

func (node *CreateSequence) Format(ctx *FmtCtx) {
	ctx.WriteString("create ")
	if node.Temporary {
		ctx.WriteString("temporary ")
	}

	ctx.WriteString("sequence ")

	if node.IfNotExists {
		ctx.WriteString("if not exists ")
	}

	node.Name.Format(ctx)
}

func (node *CreateSequence) GetStatementType() string { return "Create Sequence" }
func (node *CreateSequence) GetQueryType() string     { return QueryTypeDDL }

type IncrementByOption struct {
	Step int
}

func (node *IncrementByOption) Format(ctx *FmtCtx) {
	ctx.WriteString("increment by ")
	ctx.WriteString(fmt.Sprintf("%v ", node.Step))
}

type MinValueOption struct {
	MinV int
}

type MaxValueOption struct {
	MaxV int
}

type StartWithOption struct {
	StartV int
}

type CacheOption struct {
	CacheSize int
}

type OwnedByOption struct {
	TableName  string
	ColumnName string
}
