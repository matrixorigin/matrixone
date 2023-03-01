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

import (
	"fmt"
)

type CreateSequence struct {
	statementImpl

	Name        *TableName
	Type        ResolvableTypeReference
	IfNotExists bool
	IncrementBy *IncrementByOption
	MinValue    *MinValueOption
	MaxValue    *MaxValueOption
	StartWith   *StartWithOption
	Cycle       bool
}

func (node *CreateSequence) Format(ctx *FmtCtx) {
	ctx.WriteString("create sequence ")

	if node.IfNotExists {
		ctx.WriteString("if not exists ")
	}

	node.Name.Format(ctx)

	ctx.WriteString(" as datatype ")
	node.Type.(*T).InternalType.Format(ctx)
	ctx.WriteString(" ")
	if node.IncrementBy != nil {
		node.IncrementBy.Format(ctx)
	}
	if node.MinValue != nil {
		node.MinValue.Format(ctx)
	}
	if node.MaxValue != nil {
		node.MaxValue.Format(ctx)
	}
	if node.StartWith != nil {
		node.StartWith.Format(ctx)
	}
	if node.Cycle {
		ctx.WriteString("cycle")
	} else {
		ctx.WriteString("no cycle")
	}
}

func (node *CreateSequence) GetStatementType() string { return "Create Sequence" }
func (node *CreateSequence) GetQueryType() string     { return QueryTypeDDL }

type IncrementByOption struct {
	Step int64
}

func (node *IncrementByOption) Format(ctx *FmtCtx) {
	ctx.WriteString("increment by ")
	ctx.WriteString(fmt.Sprintf("%v ", node.Step))
}

type MinValueOption struct {
	MinV int64
}

func (node *MinValueOption) Format(ctx *FmtCtx) {
	ctx.WriteString("minvalue ")
	ctx.WriteString(fmt.Sprintf("%v ", node.MinV))
}

type MaxValueOption struct {
	MaxV int64
}

func (node *MaxValueOption) Format(ctx *FmtCtx) {
	ctx.WriteString("maxvalue ")
	ctx.WriteString(fmt.Sprintf("%v ", node.MaxV))
}

type StartWithOption struct {
	StartV int64
}

func (node *StartWithOption) Format(ctx *FmtCtx) {
	ctx.WriteString("start with ")
	ctx.WriteString(fmt.Sprintf("%v ", node.StartV))
}
