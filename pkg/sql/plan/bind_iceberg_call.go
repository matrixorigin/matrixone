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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/maintenance"
	sqliceberg "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

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
	if !sqliceberg.IsBuiltinProcedure(stmt) {
		return icebergBuiltinCall{}, moerr.NewInvalidInput(ctx, "not an Iceberg builtin procedure call")
	}
	name, target, options, err := sqliceberg.BuiltinProcedureArgs(ctx, stmt)
	if err != nil {
		return icebergBuiltinCall{}, err
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
