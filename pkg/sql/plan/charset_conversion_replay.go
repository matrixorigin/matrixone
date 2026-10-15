// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/gogo/protobuf/proto"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// This wrapper is local to CONVERT. Ordinary MODIFY still authors the supplied
// declaration using the current session, even when its SQL text is unchanged.
type charsetConversionCompilerContext struct {
	CompilerContext
	replay *persistedDDLReplay
	source *planpb.TableDef
}

func (c *charsetConversionCompilerContext) getInternalSQLExecutor(proc *process.Process) (executor.SQLExecutor, bool) {
	if provider, ok := c.CompilerContext.(interface {
		getInternalSQLExecutor(*process.Process) (executor.SQLExecutor, bool)
	}); ok {
		return provider.getInternalSQLExecutor(proc)
	}
	return nil, false
}

// Retain the persisted computation, including any user-authored plain CAST.
// Only retarget the root string assignment owner (cast_strict), never an inner
// expression. Reusing that owner bounds metadata depth across repeated CONVERT.
func convertedPersistedAssignment(ctx context.Context, expr *planpb.Expr, target planpb.Type) (*planpb.Expr, error) {
	if expr == nil {
		return nil, nil
	}
	copy := DeepCopyExpr(expr)
	if isSameColumnType(copy.Typ, target) && copy.Typ.SameCollation(target) && copy.Typ.PadSpace == target.PadSpace {
		return copy, nil
	}
	if f := copy.GetF(); f != nil && f.Func != nil && f.Func.ObjName == "cast_strict" &&
		len(f.Args) == 2 && f.Args[1].GetT() != nil &&
		useSqlModeStringAssignmentCast(copy.Typ) && useSqlModeStringAssignmentCast(target) {
		// SQL CAST is represented by "cast", not this assignment-only owner.
		// In particular, keep an authored inner CAST even if its type happens
		// to match the previous column declaration.
		copy = f.Args[0]
		return forceAssignmentCastExprWithName(ctx, copy, target, "cast_strict")
	}
	return makePlan2AssignmentCastExpr(ctx, copy, target)
}

func charsetConversionExpressions(ctx CompilerContext, tableName, columnName string, target planpb.Type) (*replayedColumnExpressions, error) {
	conversion, ok := ctx.(*charsetConversionCompilerContext)
	if !ok || conversion.replay == nil || conversion.replay.tableName != strings.ToLower(tableName) {
		return nil, nil
	}
	source := conversion.replay.columns[strings.ToLower(columnName)]
	if source == nil {
		return nil, nil
	}
	// Replay entries use visible CREATE positions. MODIFY binds against the
	// complete catalog schema, so take the proven-safe expressions from the raw
	// source snapshot instead, without a second visible/raw remapping.
	raw := FindColumn(conversion.source.Cols, columnName)
	result := &replayedColumnExpressions{columnType: target}
	var err error
	if source.defaultExpr != nil {
		result.defaultExpr = proto.Clone(raw.Default).(*planpb.Default)
		result.defaultExpr.Expr, err = convertedPersistedAssignment(ctx.GetContext(), raw.Default.Expr, target)
		if err != nil {
			return nil, err
		}
	}
	if source.onUpdate != nil {
		result.onUpdate = proto.Clone(raw.OnUpdate).(*planpb.OnUpdate)
		result.onUpdate.Expr, err = convertedPersistedAssignment(ctx.GetContext(), raw.OnUpdate.Expr, target)
		if err != nil {
			return nil, err
		}
	}
	if source.generated != nil {
		result.generated = proto.Clone(raw.GeneratedCol).(*planpb.GeneratedCol)
		result.generated.Expr, err = convertedPersistedAssignment(ctx.GetContext(), raw.GeneratedCol.Expr, target)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
