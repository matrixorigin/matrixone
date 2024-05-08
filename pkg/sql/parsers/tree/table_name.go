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

type TableName struct {
	TableExpr
	objName
	AtTsExpr *AtTimeStamp
}

func (tn TableName) Format(ctx *FmtCtx) {
	if tn.ExplicitCatalog {
		ctx.WriteString(string(tn.CatalogName))
		ctx.WriteByte('.')
	}
	if tn.ExplicitSchema {
		ctx.WriteString(string(tn.SchemaName))
		ctx.WriteByte('.')
	}
	ctx.WriteString(string(tn.ObjectName))
	if tn.AtTsExpr != nil {
		ctx.WriteString("{")
		tn.AtTsExpr.Format(ctx)
		ctx.WriteString("}")
	}
}

func (tn *TableName) Name() Identifier {
	return tn.ObjectName
}

func (tn *TableName) Schema() Identifier {
	return tn.SchemaName
}

func (tn *TableName) Catalog() Identifier {
	return tn.CatalogName
}

var _ TableExpr = &TableName{}

// table name array
type TableNames []*TableName

func (node *TableNames) Format(ctx *FmtCtx) {
	prefix := ""
	for _, t := range *node {
		ctx.WriteString(prefix)
		t.Format(ctx)
		prefix = ", "
	}
}

func NewTableName(name Identifier, prefix ObjectNamePrefix, AtTsExpr *AtTimeStamp) *TableName {
	return &TableName{
		objName: objName{
			ObjectName:       name,
			ObjectNamePrefix: prefix,
		},
		AtTsExpr: AtTsExpr,
	}
}

type AtTimeStamp struct {
	Type ATTimeStampType
	Expr Expr
}

func (node *AtTimeStamp) Format(ctx *FmtCtx) {
	ctx.WriteString(node.Type.String())
	ctx.WriteString(" = ")
	node.Expr.Format(ctx)
}

type ATTimeStampType int

const (
	ATTIMESTAMPNONE ATTimeStampType = iota
	ATTIMESTAMPTIME
	ATTIMESTAMPSNAPSHOT
	ATMOTIMESTAMP
)

func (a ATTimeStampType) String() string {
	switch a {
	case ATTIMESTAMPNONE: // none
		return "none"
	case ATTIMESTAMPTIME: // format: {timestamp = expr}
		return "timestamp"
	case ATTIMESTAMPSNAPSHOT: // format: {snapshot = expr}
		return "snapshot"
	case ATMOTIMESTAMP: // format: {mo-timestamp = expr}
		return "mo-timestamp"
	}
	return "unknown"
}
