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

type MongoDBOption struct {
	Key Identifier
	Val string
}

type MongoDBOptions []*MongoDBOption

func NewMongoDBOption(key Identifier, value string) *MongoDBOption {
	return &MongoDBOption{Key: key, Val: value}
}

func (option *MongoDBOption) Format(ctx *FmtCtx) {
	ctx.WriteString("\"")
	ctx.WriteString(string(option.Key))
	ctx.WriteString("\" = '")
	value := option.Val
	key := strings.ToLower(strings.ReplaceAll(string(option.Key), "_", ""))
	if key == "hosts" || key == "srvhost" || strings.Contains(key, "password") || strings.Contains(key, "credential") ||
		strings.Contains(key, "token") || strings.Contains(key, "secret") || key == "optionsjson" {
		value = "<redacted>"
	}
	ctx.WriteString(strings.ReplaceAll(FormatString(value), "'", "''"))
	ctx.WriteByte('\'')
}

func (options MongoDBOptions) Format(ctx *FmtCtx) {
	for i, option := range options {
		if i > 0 {
			ctx.WriteString(", ")
		}
		option.Format(ctx)
	}
}

type MongoDBTableParam struct {
	Options MongoDBOptions
}

func NewMongoDBTableParam(options MongoDBOptions) *MongoDBTableParam {
	return &MongoDBTableParam{Options: options}
}

func (param *MongoDBTableParam) Format(ctx *FmtCtx) {
	ctx.WriteString("engine = mongodb")
	if len(param.Options) > 0 {
		ctx.WriteString(" with (")
		param.Options.Format(ctx)
		ctx.WriteByte(')')
	}
}

type AttributeMongoDBPath struct {
	columnAttributeImpl
	Path string
}

func NewAttributeMongoDBPath(path string) *AttributeMongoDBPath {
	return &AttributeMongoDBPath{Path: path}
}

func (attribute *AttributeMongoDBPath) Format(ctx *FmtCtx) {
	ctx.WriteString("mongodb_path '")
	ctx.WriteString(strings.ReplaceAll(FormatString(attribute.Path), "'", "''"))
	ctx.WriteByte('\'')
}

func (attribute *AttributeMongoDBPath) Free() {}

type AttributeMongoDBConvert struct {
	columnAttributeImpl
	Mode string
}

func NewAttributeMongoDBConvert(mode string) *AttributeMongoDBConvert {
	return &AttributeMongoDBConvert{Mode: mode}
}

func (attribute *AttributeMongoDBConvert) Format(ctx *FmtCtx) {
	ctx.WriteString("mongodb_convert '")
	ctx.WriteString(strings.ReplaceAll(FormatString(attribute.Mode), "'", "''"))
	ctx.WriteByte('\'')
}

func (attribute *AttributeMongoDBConvert) Free() {}

type CreateMongoDBConnection struct {
	statementImpl
	Name        Identifier
	IfNotExists bool
	Options     MongoDBOptions
}

func (stmt *CreateMongoDBConnection) Format(ctx *FmtCtx) {
	ctx.WriteString("create mongodb connection ")
	if stmt.IfNotExists {
		ctx.WriteString("if not exists ")
	}
	ctx.WriteIdentifier(stmt.Name)
	if len(stmt.Options) > 0 {
		ctx.WriteString(" with (")
		stmt.Options.Format(ctx)
		ctx.WriteByte(')')
	}
}
func (*CreateMongoDBConnection) GetStatementType() string { return "Create MongoDB Connection" }
func (*CreateMongoDBConnection) GetQueryType() string     { return QueryTypeDDL }
func (*CreateMongoDBConnection) StmtKind() StmtKind       { return frontendStatusTyp }
func (*CreateMongoDBConnection) Free()                    {}

type AlterMongoDBConnection struct {
	statementImpl
	Name    Identifier
	Action  AlterMongoDBConnectionAction
	Options MongoDBOptions
}

type AlterMongoDBConnectionAction uint8

const (
	AlterMongoDBConnectionSet AlterMongoDBConnectionAction = iota
	AlterMongoDBConnectionEnable
	AlterMongoDBConnectionDisable
)

func (stmt *AlterMongoDBConnection) Format(ctx *FmtCtx) {
	ctx.WriteString("alter mongodb connection ")
	ctx.WriteIdentifier(stmt.Name)
	switch stmt.Action {
	case AlterMongoDBConnectionEnable:
		ctx.WriteString(" enable")
	case AlterMongoDBConnectionDisable:
		ctx.WriteString(" disable")
	default:
		ctx.WriteString(" set (")
		stmt.Options.Format(ctx)
		ctx.WriteByte(')')
	}
}
func (*AlterMongoDBConnection) GetStatementType() string { return "Alter MongoDB Connection" }
func (*AlterMongoDBConnection) GetQueryType() string     { return QueryTypeDDL }
func (*AlterMongoDBConnection) StmtKind() StmtKind       { return frontendStatusTyp }
func (*AlterMongoDBConnection) Free()                    {}

type DropMongoDBConnection struct {
	statementImpl
	Name     Identifier
	IfExists bool
}

func (stmt *DropMongoDBConnection) Format(ctx *FmtCtx) {
	ctx.WriteString("drop mongodb connection ")
	if stmt.IfExists {
		ctx.WriteString("if exists ")
	}
	ctx.WriteIdentifier(stmt.Name)
}
func (*DropMongoDBConnection) GetStatementType() string { return "Drop MongoDB Connection" }
func (*DropMongoDBConnection) GetQueryType() string     { return QueryTypeDDL }
func (*DropMongoDBConnection) StmtKind() StmtKind       { return frontendStatusTyp }
func (*DropMongoDBConnection) Free()                    {}

type ShowMongoDBConnections struct {
	statementImpl
	Like  *ComparisonExpr
	Where *Where
}

func (stmt *ShowMongoDBConnections) Format(ctx *FmtCtx) {
	ctx.WriteString("show mongodb connections")
	if stmt.Like != nil {
		ctx.WriteByte(' ')
		stmt.Like.Format(ctx)
	}
	if stmt.Where != nil {
		ctx.WriteByte(' ')
		stmt.Where.Format(ctx)
	}
}
func (*ShowMongoDBConnections) GetStatementType() string { return "Show MongoDB Connections" }
func (*ShowMongoDBConnections) GetQueryType() string     { return QueryTypeOth }
func (*ShowMongoDBConnections) StmtKind() StmtKind       { return compositeResRowType }
func (*ShowMongoDBConnections) Free()                    {}
