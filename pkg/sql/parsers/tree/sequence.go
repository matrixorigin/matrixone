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

	ctx.WriteString(" as ")
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
	Minus bool
	Num   any
}

func (node *IncrementByOption) Format(ctx *FmtCtx) {
	ctx.WriteString("increment by ")
	formatAny(node.Minus, node.Num, ctx)
}

type MinValueOption struct {
	Minus bool
	Num   any
}

func (node *MinValueOption) Format(ctx *FmtCtx) {
	ctx.WriteString("minvalue ")
	formatAny(node.Minus, node.Num, ctx)
}

type MaxValueOption struct {
	Minus bool
	Num   any
}

func (node *MaxValueOption) Format(ctx *FmtCtx) {
	ctx.WriteString("maxvalue ")
	formatAny(node.Minus, node.Num, ctx)
}

type StartWithOption struct {
	Minus bool
	Num   any
}

func (node *StartWithOption) Format(ctx *FmtCtx) {
	ctx.WriteString("start with ")
	formatAny(node.Minus, node.Num, ctx)
}

func formatAny(minus bool, num any, ctx *FmtCtx) {
	switch num := num.(type) {
	case uint64:
		ctx.WriteString(fmt.Sprintf("%v ", num))
	case int64:
		var v int64
		if minus {
			v = -num
		} else {
			v = num
		}
		ctx.WriteString(fmt.Sprintf("%v ", v))
	}
}

type DropSequence struct {
	statementImpl
	IfExists bool
	Names    TableNames
}

func (node *DropSequence) Format(ctx *FmtCtx) {
	ctx.WriteString("drop sequence")
	if node.IfExists {
		ctx.WriteString(" if exists")
	}
	ctx.WriteByte(' ')
	node.Names.Format(ctx)
}

func (node *DropSequence) GetStatementType() string { return "Drop Sequence" }
func (node *DropSequence) GetQueryType() string     { return QueryTypeDDL }
