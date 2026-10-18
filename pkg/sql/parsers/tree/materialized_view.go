// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

import "strconv"

// A zero interval means manual refresh. Positive intervals are normalized seconds.
type CreateMaterializedView struct {
	statementImpl
	Name            *TableName
	AsSource        *Select
	IfNotExists     bool
	IntervalSeconds int64
}

func formatMVPolicy(ctx *FmtCtx, seconds int64) {
	if seconds == 0 {
		ctx.WriteString("refresh manual")
	} else {
		ctx.WriteString("refresh every interval ")
		ctx.WriteString(strconv.FormatInt(seconds, 10))
		ctx.WriteString(" second")
	}
}

func (n *CreateMaterializedView) Format(ctx *FmtCtx) {
	ctx.WriteString("create materialized view ")
	if n.IfNotExists {
		ctx.WriteString("if not exists ")
	}
	n.Name.Format(ctx)
	ctx.WriteByte(' ')
	formatMVPolicy(ctx, n.IntervalSeconds)
	ctx.WriteString(" as ")
	n.AsSource.Format(ctx)
}
func (*CreateMaterializedView) GetStatementType() string { return "Create Materialized View" }
func (*CreateMaterializedView) GetQueryType() string     { return QueryTypeDDL }
func (*CreateMaterializedView) StmtKind() StmtKind       { return defaultStatusTyp }

type RefreshMaterializedView struct {
	statementImpl
	Name *TableName
}

func (n *RefreshMaterializedView) Format(ctx *FmtCtx) {
	ctx.WriteString("refresh materialized view ")
	n.Name.Format(ctx)
}
func (*RefreshMaterializedView) GetStatementType() string { return "Refresh Materialized View" }
func (*RefreshMaterializedView) GetQueryType() string     { return QueryTypeDDL }
func (*RefreshMaterializedView) StmtKind() StmtKind       { return frontendStatusTyp }

type DropMaterializedView struct {
	statementImpl
	Name     *TableName
	IfExists bool
}

func (n *DropMaterializedView) Format(ctx *FmtCtx) {
	ctx.WriteString("drop materialized view ")
	if n.IfExists {
		ctx.WriteString("if exists ")
	}
	n.Name.Format(ctx)
}
func (*DropMaterializedView) GetStatementType() string { return "Drop Materialized View" }
func (*DropMaterializedView) GetQueryType() string     { return QueryTypeDDL }
func (*DropMaterializedView) StmtKind() StmtKind       { return frontendStatusTyp }

type AlterMaterializedView struct {
	statementImpl
	Name            *TableName
	IntervalSeconds int64
}

func (n *AlterMaterializedView) Format(ctx *FmtCtx) {
	ctx.WriteString("alter materialized view ")
	n.Name.Format(ctx)
	ctx.WriteByte(' ')
	formatMVPolicy(ctx, n.IntervalSeconds)
}
func (*AlterMaterializedView) GetStatementType() string { return "Alter Materialized View" }
func (*AlterMaterializedView) GetQueryType() string     { return QueryTypeDDL }
func (*AlterMaterializedView) StmtKind() StmtKind       { return frontendStatusTyp }

type ShowMaterializedViews struct {
	showImpl
	Name   *TableName
	Runs   bool
	Create bool
}

func (n *ShowMaterializedViews) Format(ctx *FmtCtx) {
	if n.Create {
		ctx.WriteString("show create materialized view ")
		n.Name.Format(ctx)
	} else if n.Runs {
		ctx.WriteString("show materialized view refreshes ")
		n.Name.Format(ctx)
	} else {
		ctx.WriteString("show materialized views")
	}
}
func (*ShowMaterializedViews) GetStatementType() string { return "Show Materialized Views" }
func (*ShowMaterializedViews) GetQueryType() string     { return QueryTypeOth }
func (*ShowMaterializedViews) StmtKind() StmtKind       { return compositeResRowType }
