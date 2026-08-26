// Copyright 2026 Matrix Origin
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

// MultiInsertTarget is one INTO clause of a multi-table INSERT:
//
//	INTO <table> [(col, ...)] [VALUES (expr, ...)]
//
// The VALUES expressions are evaluated against the output columns of the
// statement's source query. When Values is nil, the source columns are
// inserted positionally.
type MultiInsertTarget struct {
	Table       *TableName
	Columns     IdentifierList
	ColumnNames []*UnresolvedName
	Values      Exprs
}

func (node *MultiInsertTarget) Format(ctx *FmtCtx) {
	ctx.WriteString("into ")
	node.Table.Format(ctx)
	if node.ColumnNames != nil {
		ctx.WriteString(" (")
		formatUnresolvedNames(ctx, node.ColumnNames)
		ctx.WriteByte(')')
	} else if node.Columns != nil {
		ctx.WriteString(" (")
		node.Columns.Format(ctx)
		ctx.WriteByte(')')
	}
	if node.Values != nil {
		ctx.WriteString(" values (")
		node.Values.Format(ctx)
		ctx.WriteByte(')')
	}
}

// MultiInsertWhen is one conditional branch of a multi-table INSERT:
//
//	WHEN <cond> THEN INTO ... [INTO ...]
type MultiInsertWhen struct {
	Cond    Expr
	Targets []*MultiInsertTarget
}

func (node *MultiInsertWhen) Format(ctx *FmtCtx) {
	ctx.WriteString("when ")
	node.Cond.Format(ctx)
	ctx.WriteString(" then")
	for _, target := range node.Targets {
		ctx.WriteByte(' ')
		target.Format(ctx)
	}
}

// MultiInsert is the Snowflake-style multi-table INSERT:
//
//	INSERT ALL INTO t1 [(cols)] [VALUES (...)] INTO t2 ... SELECT ...
//	INSERT {ALL | FIRST}
//	    WHEN cond1 THEN INTO t1 ... [INTO ...]
//	    WHEN cond2 THEN INTO t2 ...
//	    [ELSE INTO t3 ...]
//	SELECT ...
//
// Every source row is evaluated once. In the unconditional form it is written
// to every target. In the conditional form, INSERT ALL writes it to the targets
// of every WHEN whose condition is true, while INSERT FIRST writes it to the
// targets of only the first true WHEN; rows matching no WHEN go to the ELSE
// targets, if any.
type MultiInsert struct {
	statementImpl
	With *With
	// First is true for INSERT FIRST, false for INSERT ALL.
	First bool
	// Targets holds the unconditional INTO clauses (no WHEN branches).
	Targets []*MultiInsertTarget
	// Whens holds the conditional branches; Else the targets for rows that
	// match none of them.
	Whens []*MultiInsertWhen
	Else  []*MultiInsertTarget
	// Source is the query every row comes from.
	Source *Select
}

func (node *MultiInsert) Format(ctx *FmtCtx) {
	if node.With != nil {
		node.With.Format(ctx)
		ctx.WriteByte(' ')
	}
	if node.First {
		ctx.WriteString("insert first")
	} else {
		ctx.WriteString("insert all")
	}
	for _, target := range node.Targets {
		ctx.WriteByte(' ')
		target.Format(ctx)
	}
	for _, when := range node.Whens {
		ctx.WriteByte(' ')
		when.Format(ctx)
	}
	if len(node.Else) > 0 {
		ctx.WriteString(" else")
		for _, target := range node.Else {
			ctx.WriteByte(' ')
			target.Format(ctx)
		}
	}
	if node.Source != nil {
		ctx.WriteByte(' ')
		node.Source.Format(ctx)
	}
}

// AllTargets returns every INTO clause of the statement in source order:
// the unconditional targets, then each WHEN branch's targets, then ELSE.
func (node *MultiInsert) AllTargets() []*MultiInsertTarget {
	var all []*MultiInsertTarget
	all = append(all, node.Targets...)
	for _, when := range node.Whens {
		all = append(all, when.Targets...)
	}
	all = append(all, node.Else...)
	return all
}

func (node *MultiInsert) GetStatementType() string { return "Insert" }
func (node *MultiInsert) GetQueryType() string     { return QueryTypeDML }

func (node *MultiInsert) StmtKind() StmtKind { return defaultStatusTyp }
