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

func (ko KillOption) Format(ctx *FmtCtx) {
	if ko.Exist {
		ctx.WriteString(ko.Typ.String())
	}
}

type StatementOption struct {
	Exist       bool
	StatementId string
}

func (so StatementOption) Format(ctx *FmtCtx) {
	if so.Exist {
		ctx.WriteString(so.StatementId)
	}
}

type Kill struct {
	statementImpl
	Option       KillOption
	ConnectionId uint64
	StmtOption   StatementOption
}

func (k *Kill) Format(ctx *FmtCtx) {
	ctx.WriteString("kill")
	if k.Option.Exist {
		ctx.WriteByte(' ')
		k.Option.Format(ctx)
	}
	ctx.WriteByte(' ')
	ctx.WriteString(fmt.Sprintf("%d", k.ConnectionId))
	if k.StmtOption.Exist {
		ctx.WriteByte(' ')
		ctx.WriteByte('"')
		ctx.WriteString(k.StmtOption.StatementId)
		ctx.WriteByte('"')
	}
}

func (k *Kill) GetStatementType() string { return "kill" }
func (k *Kill) GetQueryType() string     { return QueryTypeDCL }
