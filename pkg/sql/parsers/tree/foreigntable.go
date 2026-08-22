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

// ForeignTableOption is one option of CREATE EXTERNAL TABLE ... ENGINE =
// ESQL|SQL WITH (...). See docs/cn/esql_sql_exttab.md.
type ForeignTableOption struct {
	Key Identifier
	Val string
}

type ForeignTableOptions []ForeignTableOption

func NewForeignTableOption(key Identifier, value string) ForeignTableOption {
	return ForeignTableOption{Key: key, Val: value}
}

func (option ForeignTableOption) Format(ctx *FmtCtx) {
	ctx.WriteString("\"")
	ctx.WriteString(string(option.Key))
	ctx.WriteString("\" = '")
	value := option.Val
	// An inline config JSON carries credentials (ES password, DSN password);
	// redact it wherever the statement is re-rendered (statement logs, SHOW
	// CREATE). An "env:NAME" reference is not a secret and stays verbatim.
	if strings.EqualFold(strings.TrimSpace(string(option.Key)), "config") &&
		!strings.HasPrefix(strings.TrimSpace(value), "env:") {
		value = "<redacted>"
	}
	ctx.WriteString(strings.ReplaceAll(FormatString(value), "'", "''"))
	ctx.WriteByte('\'')
}

func (options ForeignTableOptions) Format(ctx *FmtCtx) {
	for i, option := range options {
		if i > 0 {
			ctx.WriteString(", ")
		}
		option.Format(ctx)
	}
}

// ForeignTableParam is the ENGINE = ESQL|SQL clause of CREATE EXTERNAL TABLE.
type ForeignTableParam struct {
	// Kind is "esql" or "sql" (case-insensitive as parsed).
	Kind    string
	Options ForeignTableOptions
}

func NewForeignTableParam(kind string, options ForeignTableOptions) *ForeignTableParam {
	return &ForeignTableParam{Kind: kind, Options: options}
}

func (param *ForeignTableParam) Format(ctx *FmtCtx) {
	ctx.WriteString("engine = ")
	ctx.WriteString(strings.ToLower(param.Kind))
	if len(param.Options) > 0 {
		ctx.WriteString(" with (")
		param.Options.Format(ctx)
		ctx.WriteByte(')')
	}
}
