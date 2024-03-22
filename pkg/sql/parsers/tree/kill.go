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
	reuse.CreatePool[KillOption](
		func() *KillOption { return &KillOption{} },
		func(k *KillOption) { k.reset() },
		reuse.DefaultOptions[KillOption](),
	)

	reuse.CreatePool[StatementOption](
		func() *StatementOption { return &StatementOption{} },
		func(s *StatementOption) { s.reset() },
		reuse.DefaultOptions[StatementOption](),
	)

	reuse.CreatePool[Kill](
		func() *Kill { return &Kill{} },
		func(k *Kill) { k.reset() },
		reuse.DefaultOptions[Kill](),
	)
}

type KillType int

const (
	KillTypeConnection KillType = iota
	KillTypeQuery
)

func (k KillType) String() string {
	switch k {
	case KillTypeConnection:
		return "connection"
	case KillTypeQuery:
		return "query"
	default:
		return ""
	}
}

type KillOption struct {
	Exist bool
	Typ   KillType
}

func NewKillOption() *KillOption {
	ko := reuse.Alloc[KillOption](nil)
	return ko
}

func (node KillOption) Format(ctx *FmtCtx) {
	if node.Exist {
		ctx.WriteString(node.Typ.String())
	}
}

func (node *KillOption) reset() {
	*node = KillOption{}
}

func (node *KillOption) Free() {
	reuse.Free[KillOption](node, nil)
}

func (node KillOption) TypeName() string { return "tree.KillOption" }

type StatementOption struct {
	Exist       bool
	StatementId string
}

func NewStatementOption() *StatementOption {
	so := reuse.Alloc[StatementOption](nil)
	return so
}

func (node *StatementOption) reset() {
	*node = StatementOption{}
}

func (node StatementOption) Format(ctx *FmtCtx) {
	if node.Exist {
		ctx.WriteString(node.StatementId)
	}
}

func (node *StatementOption) Free() {
	reuse.Free[StatementOption](node, nil)
}

func (node StatementOption) TypeName() string { return "tree.StatementOption" }

type Kill struct {
	statementImpl
	Option       KillOption
	ConnectionId uint64
	StmtOption   StatementOption
}

func NewKill(o KillOption, id uint64, so StatementOption) *Kill {
	k := reuse.Alloc[Kill](nil)
	k.Option = o
	k.ConnectionId = id
	k.StmtOption = so
	return k
}

func (node *Kill) Format(ctx *FmtCtx) {
	ctx.WriteString("kill")
	if node.Option.Exist {
		ctx.WriteByte(' ')
		node.Option.Format(ctx)
	}
	ctx.WriteByte(' ')
	ctx.WriteString(fmt.Sprintf("%d", node.ConnectionId))
	if node.StmtOption.Exist {
		ctx.WriteByte(' ')
		ctx.WriteByte('"')
		ctx.WriteString(node.StmtOption.StatementId)
		ctx.WriteByte('"')
	}
}

func (node *Kill) reset() {
	node.Option.Free()
	node.StmtOption.Free()
	*node = Kill{}
}

func (node *Kill) Free() {
	reuse.Free[Kill](node, nil)
}

func (node *Kill) GetStatementType() string { return "kill" }

func (node *Kill) GetQueryType() string { return QueryTypeOth }

func (node Kill) TypeName() string { return "tree.Kill" }
