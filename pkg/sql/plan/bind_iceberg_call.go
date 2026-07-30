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

package plan

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/maintenance"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const icebergBuiltinCallPrefix = "iceberg_"

func isIcebergBuiltinCall(stmt *tree.CallStmt) bool {
	if stmt == nil || stmt.Name == nil || !stmt.Name.HasNoNameQualifier() {
		return false
	}
	name := strings.ToLower(strings.TrimSpace(tree.String(stmt.Name, dialect.MYSQL)))
	return strings.HasPrefix(name, icebergBuiltinCallPrefix)
}

func buildIcebergBuiltinCall(stmt *tree.CallStmt, ctx CompilerContext) (*Plan, error) {
	call, err := parseIcebergBuiltinCall(ctx.GetContext(), stmt)
	if err != nil {
		return nil, err
	}
	return nil, moerr.NewNotSupportedf(
		ctx.GetContext(),
		"Iceberg builtin procedure %s for %s is recognized but not implemented in this phase",
		call.Name,
		call.Parsed.Target,
	)
}

type icebergBuiltinCall struct {
	Name    string
	Target  string
	Options string
	Parsed  maintenance.ParsedCall
}

func parseIcebergBuiltinCall(ctx context.Context, stmt *tree.CallStmt) (icebergBuiltinCall, error) {
	if !isIcebergBuiltinCall(stmt) {
		return icebergBuiltinCall{}, moerr.NewInvalidInput(ctx, "not an Iceberg builtin procedure call")
	}
	name := strings.TrimSpace(tree.String(stmt.Name, dialect.MYSQL))
	if len(stmt.Args) < 1 || len(stmt.Args) > 2 {
		return icebergBuiltinCall{}, moerr.NewInvalidInputf(ctx, "Iceberg builtin procedure %s requires target and optional options string arguments", name)
	}
	target, err := icebergBuiltinStringArg(ctx, name, "target", stmt.Args[0])
	if err != nil {
		return icebergBuiltinCall{}, err
	}
	options := ""
	if len(stmt.Args) == 2 {
		options, err = icebergBuiltinStringArg(ctx, name, "options", stmt.Args[1])
		if err != nil {
			return icebergBuiltinCall{}, err
		}
	}
	parsed, err := maintenance.ParseProcedureCall(name, target, options)
	if err != nil {
		return icebergBuiltinCall{}, api.ToMOErr(ctx, err)
	}
	return icebergBuiltinCall{
		Name:    name,
		Target:  target,
		Options: options,
		Parsed:  parsed,
	}, nil
}

func icebergBuiltinStringArg(ctx context.Context, procedure, argName string, expr tree.Expr) (string, error) {
	var out string
	switch value := expr.(type) {
	case *tree.StrVal:
		out = strings.TrimSpace(value.String())
	case *tree.NumVal:
		if value.Kind() == tree.Str {
			out = strings.TrimSpace(value.String())
		}
	}
	if out == "" {
		return "", moerr.NewInvalidInputf(ctx, "Iceberg builtin procedure %s requires %s as a string literal", procedure, argName)
	}
	return out, nil
}
