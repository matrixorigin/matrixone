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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// This wrapper is local to CONVERT. Ordinary MODIFY still authors the supplied
// declaration using the current session, even when its SQL text is unchanged.
type charsetConversionCompilerContext struct {
	CompilerContext
	replay *persistedDDLReplay
}

func (c *charsetConversionCompilerContext) getInternalSQLExecutor(proc *process.Process) (executor.SQLExecutor, bool) {
	if provider, ok := c.CompilerContext.(interface {
		getInternalSQLExecutor(*process.Process) (executor.SQLExecutor, bool)
	}); ok {
		return provider.getInternalSQLExecutor(proc)
	}
	return nil, false
}

func isPersistedCharacterCoercion(expr *planpb.Expr) bool {
	f := expr.GetF()
	if f == nil || f.Func == nil || f.Func.ObjName != "cast" || f.SyntaxExplicitCast ||
		(expr.Typ.CollationCoercibilitySet && expr.Typ.CollationCoercibility == 0) ||
		len(f.Args) != 2 || f.Args[0] == nil || f.Args[1].GetT() == nil || !types.T(expr.Typ.Id).IsMySQLString() {
		return false
	}
	id, overload := function.DecodeOverloadID(f.Func.Obj)
	return id == function.CAST && overload == 0
}

// Refresh compiler-owned character coercions before rebinding their consumer.
// Keep the physical target (including a narrowing width), every authored
// CAST/EXPLICIT COLLATE or assignment boundary, and the typed computation underneath. Only collapse
// equal-physical-type coercion layers; repeated CONVERT must not grow metadata.
func refreshedPersistedCharacterCoercion(ctx context.Context, expr *planpb.Expr) (*planpb.Expr, error) {
	if list := expr.GetList(); list != nil {
		for i, item := range list.List {
			var err error
			list.List[i], err = refreshedPersistedCharacterCoercion(ctx, item)
			if err != nil {
				return nil, err
			}
		}
	}
	if !isPersistedCharacterCoercion(expr) {
		return expr, nil
	}
	target := expr.Typ
	source := expr.GetF().Args[0]
	for isPersistedCharacterCoercion(source) && source.Typ.Id == target.Id &&
		source.Typ.Width == target.Width && source.Typ.Scale == target.Scale && source.Typ.PadSpace == target.PadSpace {
		source = source.GetF().Args[0]
	}
	if domain, ok := collationForExpression(source); ok {
		domain.apply(&target)
	} else {
		// An implicit scalar-to-text conversion has NUMERIC coercibility. Treating
		// it as a column (IMPLICIT) makes a changed native peer spuriously conflict.
		// Annotate the coercion, not the unaffected numeric subtree.
		target.CollationCoercibility = 5
		target.CollationCoercibilitySet = true
		target.CollationMergeConflict = false
	}
	return appendCastBeforeExpr(ctx, source, target)
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
	// Replay retains unaffected typed children while adapting character
	// operations. Restore its visible CREATE positions to the catalog schema
	// used by MODIFY, rather than discarding the adapted tree for the raw one.
	assignment := func(expr *planpb.Expr) (*planpb.Expr, error) {
		converted, err := convertedPersistedAssignment(ctx.GetContext(), expr, target)
		if err != nil {
			return nil, err
		}
		remapGeneratedColExprPositions(converted, conversion.replay.rawPositions)
		return converted, nil
	}
	result := &replayedColumnExpressions{columnType: target}
	var err error
	if source.defaultExpr != nil {
		result.defaultExpr = proto.Clone(source.defaultExpr).(*planpb.Default)
		result.defaultExpr.Expr, err = assignment(source.defaultExpr.Expr)
		if err != nil {
			return nil, err
		}
	}
	if source.onUpdate != nil {
		result.onUpdate = proto.Clone(source.onUpdate).(*planpb.OnUpdate)
		result.onUpdate.Expr, err = assignment(source.onUpdate.Expr)
		if err != nil {
			return nil, err
		}
	}
	if source.generated != nil {
		result.generated = proto.Clone(source.generated).(*planpb.GeneratedCol)
		result.generated.Expr, err = assignment(source.generated.Expr)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
