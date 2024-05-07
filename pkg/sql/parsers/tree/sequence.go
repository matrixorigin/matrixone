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

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

func init() {
	reuse.CreatePool[AlterSequence](
		func() *AlterSequence { return &AlterSequence{} },
		func(a *AlterSequence) { a.reset() },
		reuse.DefaultOptions[AlterSequence](), //.
	) //WithEnableChecker()

	reuse.CreatePool[CreateSequence](
		func() *CreateSequence { return &CreateSequence{} },
		func(c *CreateSequence) { c.reset() },
		reuse.DefaultOptions[CreateSequence](), //.
	) //WithEnableChecker()

	reuse.CreatePool[DropSequence](
		func() *DropSequence { return &DropSequence{} },
		func(a *DropSequence) { a.reset() },
		reuse.DefaultOptions[DropSequence](), //.
	) //WithEnableChecker()
}

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

func NewCreateSequence(name *TableName, typ ResolvableTypeReference, ifnotexists bool, incrementby *IncrementByOption, minvalue *MinValueOption, maxvalue *MaxValueOption, startwith *StartWithOption, cycle bool) *CreateSequence {
	create := reuse.Alloc[CreateSequence](nil)
	create.Name = name
	create.Type = typ
	create.IfNotExists = ifnotexists
	create.IncrementBy = incrementby
	create.MinValue = minvalue
	create.MaxValue = maxvalue
	create.StartWith = startwith
	create.Cycle = cycle
	return create
}

func (node *CreateSequence) Free() {
	reuse.Free[CreateSequence](node, nil)
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

func (node *CreateSequence) reset() {
	// if node.Name != nil {
	// node.Name.Free()
	// }
	// if node.IncrementBy != nil {
	// node.IncrementBy.Free()
	// }
	// if node.MinValue != nil {
	// node.MinValue.Free()
	// }
	// if node.MaxValue != nil {
	// node.MaxValue.Free()
	// }
	// if node.StartWith != nil {
	// node.StartWith.Free()
	// }
	*node = CreateSequence{}
}

func (node CreateSequence) TypeName() string { return "tree.CreateSequence" }

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

type CycleOption struct {
	Cycle bool
}

func (node *CycleOption) Format(ctx *FmtCtx) {
	if node.Cycle {
		ctx.WriteString("cycle")
	} else {
		ctx.WriteString("no cycle")
	}
}

type TypeOption struct {
	Type ResolvableTypeReference
}

func (node *TypeOption) Format(ctx *FmtCtx) {
	ctx.WriteString(" as ")
	node.Type.(*T).InternalType.Format(ctx)
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

func (node *DropSequence) Free() { reuse.Free[DropSequence](node, nil) }

func (node DropSequence) TypeName() string { return "tree.DropSequence" }

func (node *DropSequence) reset() {
	*node = DropSequence{}
}

func NewDropSequence(ifexists bool, names TableNames) *DropSequence {
	drop := reuse.Alloc[DropSequence](nil)
	drop.IfExists = ifexists
	drop.Names = names
	return drop
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

type AlterSequence struct {
	statementImpl

	Name        *TableName
	Type        *TypeOption
	IfExists    bool
	IncrementBy *IncrementByOption
	MinValue    *MinValueOption
	MaxValue    *MaxValueOption
	StartWith   *StartWithOption
	Cycle       *CycleOption
}

func NewAlterSequence(ifexists bool, name *TableName, typ *TypeOption, incrementby *IncrementByOption, minvalue *MinValueOption, maxvalue *MaxValueOption, startwith *StartWithOption, cycle *CycleOption) *AlterSequence {
	alter := reuse.Alloc[AlterSequence](nil)
	alter.IfExists = ifexists
	alter.Name = name
	alter.Type = typ
	alter.IncrementBy = incrementby
	alter.MinValue = minvalue
	alter.MaxValue = maxvalue
	alter.StartWith = startwith
	alter.Cycle = cycle
	return alter
}

func (node *AlterSequence) Free() { reuse.Free[AlterSequence](node, nil) }

func (node *AlterSequence) Format(ctx *FmtCtx) {
	ctx.WriteString("alter sequence ")

	if node.IfExists {
		ctx.WriteString("if exists ")
	}

	node.Name.Format(ctx)

	if node.Type != nil {
		node.Type.Format(ctx)
	}
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
	if node.Cycle != nil {
		node.Cycle.Format(ctx)
	}
}

func (node AlterSequence) TypeName() string          { return "tree.AlterSequence" }
func (node *AlterSequence) GetStatementType() string { return "Alter Sequence" }
func (node *AlterSequence) GetQueryType() string     { return QueryTypeDDL }
func (node *AlterSequence) reset() {
	*node = AlterSequence{}
}
