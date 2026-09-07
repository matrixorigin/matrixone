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

// JsonValueResponseMode describes how JSON_VALUE responds when extraction or
// conversion cannot produce the requested value.
type JsonValueResponseMode uint8

const (
	JsonValueImplicitResponse JsonValueResponseMode = iota
	JsonValueNullResponse
	JsonValueErrorResponse
	JsonValueDefaultResponse
)

// JsonValueResponse is the response policy for one of the ON EMPTY/ON ERROR
// clauses. Default is populated only for JsonValueDefaultResponse.
type JsonValueResponse struct {
	Mode    JsonValueResponseMode
	Default Expr
}

// JsonValueSpec contains the syntax that is specific to JSON_VALUE. Keeping
// it on FuncExpr preserves the existing expression traversal and cloning
// machinery while retaining the structured RETURNING/response contract.
type JsonValueSpec struct {
	Returning *JsonValueReturning
	OnEmpty   JsonValueResponse
	OnError   JsonValueResponse
}

// SemanticNormalized returns the clause representation used for expression
// identity. SQL/JSON's implicit response is NULL for both ON EMPTY and ON
// ERROR, so spelling an explicit NULL response must not make an otherwise
// identical expression compare differently in GROUP BY, ORDER BY, or view
// dependency tracking.
func (s JsonValueSpec) SemanticNormalized() JsonValueSpec {
	if s.Returning != nil && s.Returning.Charset != "" {
		returning := *s.Returning
		returning.Charset = strings.ToLower(returning.Charset)
		s.Returning = &returning
	}
	if s.OnEmpty.Mode == JsonValueNullResponse {
		s.OnEmpty = JsonValueResponse{}
	}
	if s.OnError.Mode == JsonValueNullResponse {
		s.OnError = JsonValueResponse{}
	}
	return s
}

type JsonValueReturning struct {
	Type    *T
	Charset string
}

func formatJsonValueExpr(ctx *FmtCtx, node *FuncExpr) {
	ctx.WriteString("json_value(")
	if len(node.Exprs) > 0 && node.Exprs[0] != nil {
		node.Exprs[0].Format(ctx)
	}
	ctx.WriteString(", ")
	if len(node.Exprs) > 1 && node.Exprs[1] != nil {
		node.Exprs[1].Format(ctx)
	}
	// Omission is a VARCHAR(512) contract, not an explicit CHAR(512).
	// Preserve it when SQL is persisted or reparsed by CTAS/view planning.
	if node.JsonValue.Returning != nil && node.JsonValue.Returning.Type != nil {
		ctx.WriteString(" returning ")
		node.JsonValue.Returning.Type.InternalType.Format(ctx)
		if node.JsonValue.Returning.Charset != "" {
			ctx.WriteString(" character set ")
			ctx.WriteString(node.JsonValue.Returning.Charset)
		}
	}
	formatJsonValueResponse(ctx, node.JsonValue.OnEmpty, "empty")
	formatJsonValueResponse(ctx, node.JsonValue.OnError, "error")
	ctx.WriteByte(')')
}

func formatJsonValueResponse(ctx *FmtCtx, response JsonValueResponse, clause string) {
	if !response.IsExplicit() {
		return
	}
	ctx.WriteByte(' ')
	switch response.Mode {
	case JsonValueNullResponse:
		ctx.WriteString("null on ")
	case JsonValueErrorResponse:
		ctx.WriteString("error on ")
	case JsonValueDefaultResponse:
		ctx.WriteString("default ")
		if response.Default != nil {
			formatJsonValueDefault(ctx, response.Default)
		}
		ctx.WriteString(" on ")
	default:
		return
	}
	ctx.WriteString(strings.ToLower(clause))
}

// formatJsonValueDefault keeps string defaults as valid SQL literals even
// when the surrounding formatter is configured to print ordinary strings
// without quotes.
func formatJsonValueDefault(ctx *FmtCtx, expr Expr) {
	if nv, ok := expr.(*NumVal); ok && nv.ValType == P_char {
		ctx.WriteByte('\'')
		if ctx.NoBackslashEscape() {
			ctx.WriteString(strings.ReplaceAll(nv.String(), "'", "''"))
		} else {
			ctx.WriteString(strings.ReplaceAll(FormatString(nv.String()), "'", "''"))
		}
		ctx.WriteByte('\'')
		return
	}
	expr.Format(ctx)
}

func (r JsonValueResponse) IsExplicit() bool {
	return r.Mode != JsonValueImplicitResponse
}
