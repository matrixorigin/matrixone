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

import "strings"

type IcebergOption struct {
	Key Identifier
	Val string
}

func NewIcebergOption(key Identifier, val string) *IcebergOption {
	return &IcebergOption{Key: key, Val: val}
}

func (node *IcebergOption) Format(ctx *FmtCtx) {
	ctx.WriteString("\"")
	ctx.WriteString(string(node.Key))
	ctx.WriteString("\" = ")
	ctx.WriteString("'")
	ctx.WriteString(icebergOptionFormatValue(string(node.Key), node.Val))
	ctx.WriteString("'")
}

type IcebergOptions []*IcebergOption

func (node IcebergOptions) Format(ctx *FmtCtx) {
	prefix := ""
	for _, opt := range node {
		ctx.WriteString(prefix)
		opt.Format(ctx)
		prefix = ", "
	}
}

type IcebergTableParam struct {
	Options IcebergOptions
}

func NewIcebergTableParam(opts IcebergOptions) *IcebergTableParam {
	return &IcebergTableParam{Options: opts}
}

func (node *IcebergTableParam) Format(ctx *FmtCtx) {
	ctx.WriteString("engine = iceberg")
	if len(node.Options) > 0 {
		ctx.WriteString(" with (")
		node.Options.Format(ctx)
		ctx.WriteByte(')')
	}
}

type IcebergRefType int

const (
	IcebergRefNone IcebergRefType = iota
	IcebergRefSnapshot
	IcebergRefTimestamp
	IcebergRefNamedRef
)

type IcebergRefSpec struct {
	Type      IcebergRefType
	Snapshot  Expr
	Timestamp Expr
	RefName   Identifier
}

func NewIcebergSnapshotRef(snapshot Expr) *IcebergRefSpec {
	return &IcebergRefSpec{Type: IcebergRefSnapshot, Snapshot: snapshot}
}

func NewIcebergTimestampRef(ts Expr) *IcebergRefSpec {
	return &IcebergRefSpec{Type: IcebergRefTimestamp, Timestamp: ts}
}

func NewIcebergNamedRef(ref Identifier) *IcebergRefSpec {
	return &IcebergRefSpec{Type: IcebergRefNamedRef, RefName: ref}
}

func (node *IcebergRefSpec) Format(ctx *FmtCtx) {
	if node == nil || node.Type == IcebergRefNone {
		return
	}
	ctx.WriteString(" for iceberg ")
	switch node.Type {
	case IcebergRefSnapshot:
		ctx.WriteString("snapshot ")
		node.Snapshot.Format(ctx)
	case IcebergRefTimestamp:
		ctx.WriteString("timestamp as of ")
		node.Timestamp.Format(ctx)
	case IcebergRefNamedRef:
		ctx.WriteString("ref ")
		ctx.WriteIdentifier(node.RefName)
	}
}

type CreateIcebergCatalog struct {
	statementImpl
	Name        Identifier
	IfNotExists bool
	Options     IcebergOptions
}

func (node *CreateIcebergCatalog) Format(ctx *FmtCtx) {
	ctx.WriteString("create iceberg catalog ")
	if node.IfNotExists {
		ctx.WriteString("if not exists ")
	}
	ctx.WriteIdentifier(node.Name)
	if len(node.Options) > 0 {
		ctx.WriteString(" with (")
		node.Options.Format(ctx)
		ctx.WriteByte(')')
	}
}

func (node *CreateIcebergCatalog) GetStatementType() string { return "Create Iceberg Catalog" }
func (node *CreateIcebergCatalog) GetQueryType() string     { return QueryTypeDDL }
func (node *CreateIcebergCatalog) StmtKind() StmtKind       { return frontendStatusTyp }
func (node *CreateIcebergCatalog) Free()                    {}

type AlterIcebergCatalog struct {
	statementImpl
	Name    Identifier
	Options IcebergOptions
}

func (node *AlterIcebergCatalog) Format(ctx *FmtCtx) {
	ctx.WriteString("alter iceberg catalog ")
	ctx.WriteIdentifier(node.Name)
	if len(node.Options) > 0 {
		ctx.WriteString(" set (")
		node.Options.Format(ctx)
		ctx.WriteByte(')')
	}
}

func (node *AlterIcebergCatalog) GetStatementType() string { return "Alter Iceberg Catalog" }
func (node *AlterIcebergCatalog) GetQueryType() string     { return QueryTypeDDL }
func (node *AlterIcebergCatalog) StmtKind() StmtKind       { return frontendStatusTyp }
func (node *AlterIcebergCatalog) Free()                    {}

type DropIcebergCatalog struct {
	statementImpl
	Name     Identifier
	IfExists bool
}

func (node *DropIcebergCatalog) Format(ctx *FmtCtx) {
	ctx.WriteString("drop iceberg catalog ")
	if node.IfExists {
		ctx.WriteString("if exists ")
	}
	ctx.WriteIdentifier(node.Name)
}

func (node *DropIcebergCatalog) GetStatementType() string { return "Drop Iceberg Catalog" }
func (node *DropIcebergCatalog) GetQueryType() string     { return QueryTypeDDL }
func (node *DropIcebergCatalog) StmtKind() StmtKind       { return frontendStatusTyp }
func (node *DropIcebergCatalog) Free()                    {}

type ShowIcebergCatalogs struct {
	statementImpl
	Like  *ComparisonExpr
	Where *Where
}

func (node *ShowIcebergCatalogs) Format(ctx *FmtCtx) {
	ctx.WriteString("show iceberg catalogs")
	if node.Like != nil {
		ctx.WriteByte(' ')
		node.Like.Format(ctx)
	}
	if node.Where != nil {
		ctx.WriteByte(' ')
		node.Where.Format(ctx)
	}
}

func (node *ShowIcebergCatalogs) GetStatementType() string { return "Show Iceberg Catalogs" }
func (node *ShowIcebergCatalogs) GetQueryType() string     { return QueryTypeOth }
func (node *ShowIcebergCatalogs) StmtKind() StmtKind       { return compositeResRowType }
func (node *ShowIcebergCatalogs) Free()                    {}

type ShowIcebergNamespaces struct {
	statementImpl
	Catalog Identifier
	Like    *ComparisonExpr
	Where   *Where
}

func (node *ShowIcebergNamespaces) Format(ctx *FmtCtx) {
	ctx.WriteString("show iceberg namespaces")
	if node.Catalog != "" {
		ctx.WriteString(" from ")
		ctx.WriteIdentifier(node.Catalog)
	}
	if node.Like != nil {
		ctx.WriteByte(' ')
		node.Like.Format(ctx)
	}
	if node.Where != nil {
		ctx.WriteByte(' ')
		node.Where.Format(ctx)
	}
}

func (node *ShowIcebergNamespaces) GetStatementType() string { return "Show Iceberg Namespaces" }
func (node *ShowIcebergNamespaces) GetQueryType() string     { return QueryTypeOth }
func (node *ShowIcebergNamespaces) StmtKind() StmtKind       { return compositeResRowType }
func (node *ShowIcebergNamespaces) Free()                    {}

type ShowIcebergTables struct {
	statementImpl
	Catalog   Identifier
	Namespace string
	Like      *ComparisonExpr
	Where     *Where
}

func (node *ShowIcebergTables) Format(ctx *FmtCtx) {
	ctx.WriteString("show iceberg tables")
	if node.Catalog != "" {
		ctx.WriteString(" from ")
		ctx.WriteIdentifier(node.Catalog)
		if node.Namespace != "" {
			ctx.WriteByte('.')
			ctx.WriteString(node.Namespace)
		}
	} else if node.Namespace != "" {
		ctx.WriteString(" in namespace ")
		ctx.WriteString(node.Namespace)
	}
	if node.Like != nil {
		ctx.WriteByte(' ')
		node.Like.Format(ctx)
	}
	if node.Where != nil {
		ctx.WriteByte(' ')
		node.Where.Format(ctx)
	}
}

func (node *ShowIcebergTables) GetStatementType() string { return "Show Iceberg Tables" }
func (node *ShowIcebergTables) GetQueryType() string     { return QueryTypeOth }
func (node *ShowIcebergTables) StmtKind() StmtKind       { return compositeResRowType }
func (node *ShowIcebergTables) Free()                    {}

func icebergOptionFormatValue(key string, value string) string {
	if icebergOptionIsSensitive(key) && value != "" {
		return "<redacted>"
	}
	return strings.ReplaceAll(FormatString(value), "'", "''")
}

func icebergOptionIsSensitive(key string) bool {
	k := normalizedIcebergOptionKey(key)
	return strings.Contains(k, "secret") ||
		strings.Contains(k, "token") ||
		strings.Contains(k, "password") ||
		strings.Contains(k, "credential") ||
		strings.Contains(k, "authorization") ||
		strings.Contains(k, "accesskey") ||
		strings.Contains(k, "apikey") ||
		strings.Contains(k, "signature") ||
		strings.Contains(k, "bearer")
}

func normalizedIcebergOptionKey(key string) string {
	k := strings.ToLower(key)
	replacer := strings.NewReplacer("_", "", "-", "", ".", "", " ", "")
	return replacer.Replace(k)
}
