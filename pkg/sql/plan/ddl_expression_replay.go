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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

type persistedDDLReplayKey struct{}

// persistedDDLReplay carries the already-bound expressions of an existing
// table through SQL reconstruction. New declarations and physical type changes
// still bind from SQL; character-only changes adapt affected typed nodes without
// re-authoring unaffected computation using the current session.
type persistedDDLReplay struct {
	tableName string
	columns   map[string]*replayedColumnExpressions
	checks    map[string]*planpb.CheckDef
	// Inverse of the visible CREATE coordinates, used by in-place MODIFY.
	rawPositions map[int32]int32
	err          error
}

type replayedColumnExpressions struct {
	columnType  planpb.Type
	defaultExpr *planpb.Default
	onUpdate    *planpb.OnUpdate
	generated   *planpb.GeneratedCol
}

// WithPersistedDDLReplay scopes expression preservation to one reconstructed
// CREATE, used by COPY ALTER and CREATE TABLE LIKE. It does not change the
// ordinary CREATE path.
func WithPersistedDDLReplay(ctx context.Context, original, target *planpb.TableDef) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = defines.WithoutAlterCopyColumnLimit(ctx)
	if original == nil || target == nil {
		return ctx
	}
	replay := &persistedDDLReplay{
		tableName:    strings.ToLower(target.Name),
		columns:      make(map[string]*replayedColumnExpressions),
		checks:       make(map[string]*planpb.CheckDef),
		rawPositions: make(map[int32]int32),
	}
	targetByID := make(map[uint64]*planpb.ColDef, len(target.Cols))
	targetRawPosByID := make(map[uint64]int32, len(target.Cols))
	targetVisiblePosByID := make(map[uint64]int32, len(target.Cols))
	targetByName := make(map[string]*planpb.ColDef, len(target.Cols))
	targetRawPosByName := make(map[string]int32, len(target.Cols))
	targetVisiblePosByName := make(map[string]int32, len(target.Cols))
	targetIsCluster := util.TableIsClusterTable(target.TableType)
	originalIsCluster := util.TableIsClusterTable(original.TableType)
	var visiblePos int32
	for i, col := range target.Cols {
		if col == nil || col.Hidden || col.Name == catalog.Row_ID ||
			(targetIsCluster && util.IsClusterTableAttribute(col.GetOriginCaseName())) {
			continue
		}
		replay.rawPositions[visiblePos] = int32(i)
		targetByID[col.ColId] = col
		targetRawPosByID[col.ColId] = int32(i)
		targetVisiblePosByID[col.ColId] = visiblePos
		key := strings.ToLower(col.Name)
		targetByName[key] = col
		targetRawPosByName[key] = int32(i)
		targetVisiblePosByName[key] = visiblePos
		visiblePos++
	}
	rawPositions := make(map[int32]int32, len(original.Cols))
	visiblePositions := make(map[int32]int32, len(original.Cols))
	compatible := make(map[int32]bool, len(original.Cols))
	characterTypes := make(map[int32]planpb.Type)
	for i, oldCol := range original.Cols {
		if oldCol == nil || oldCol.Hidden || oldCol.Name == catalog.Row_ID ||
			(originalIsCluster && util.IsClusterTableAttribute(oldCol.GetOriginCaseName())) {
			continue
		}
		newCol := targetByID[oldCol.ColId]
		rawPos, ok := targetRawPosByID[oldCol.ColId]
		visiblePos := targetVisiblePosByID[oldCol.ColId]
		if !ok || newCol == nil || oldCol.ColId == 0 {
			// Synthetic columns in LIKE can have zero IDs. Their names and
			// declarations are unchanged in the reconstructed skeleton.
			key := strings.ToLower(oldCol.Name)
			newCol = targetByName[key]
			rawPos, ok = targetRawPosByName[key]
			visiblePos = targetVisiblePosByName[key]
		}
		if !ok || newCol == nil {
			continue
		}
		rawPositions[int32(i)] = rawPos
		visiblePositions[int32(i)] = visiblePos
		// Nullability alone does not change expression values. Auto-increment
		// changes generated-column legality, while enum order, charset and
		// CHAR padding change value semantics. Rebind affected expressions.
		compatible[int32(i)] = oldCol.Typ.Id == newCol.Typ.Id &&
			oldCol.Typ.Width == newCol.Typ.Width && oldCol.Typ.Scale == newCol.Typ.Scale &&
			oldCol.Typ.AutoIncr == newCol.Typ.AutoIncr &&
			oldCol.Typ.Enumvalues == newCol.Typ.Enumvalues &&
			oldCol.Typ.SameCollation(newCol.Typ) &&
			oldCol.Typ.PadSpace == newCol.Typ.PadSpace
		// A character-domain change invalidates only the affected operation,
		// not its unrelated, already-bound children (for example decimal /).
		if oldCol.Typ.Id == newCol.Typ.Id && oldCol.Typ.Width == newCol.Typ.Width &&
			oldCol.Typ.Scale == newCol.Typ.Scale && oldCol.Typ.AutoIncr == newCol.Typ.AutoIncr &&
			oldCol.Typ.Enumvalues == newCol.Typ.Enumvalues {
			switch types.T(oldCol.Typ.Id) {
			case types.T_char, types.T_varchar, types.T_text:
				characterTypes[int32(i)] = newCol.Typ
			}
		}
	}
	copyExpr := func(expr *planpb.Expr, positions map[int32]int32) *planpb.Expr {
		if expr == nil {
			return nil
		}
		copy := DeepCopyExpr(expr)
		var valid func(*planpb.Expr) bool
		valid = func(e *planpb.Expr) bool {
			if e == nil {
				return true
			}
			if ref := e.GetCol(); ref != nil && ref.RelPos == 0 {
				if !compatible[ref.ColPos] {
					target, ok := characterTypes[ref.ColPos]
					if !ok {
						return false
					}
					e.Typ.Charset = target.Charset
					e.Typ.CollationVersion = target.CollationVersion
					e.Typ.PadSpace = target.PadSpace
				}
				ref.ColPos = positions[ref.ColPos]
			}
			if f := e.GetF(); f != nil {
				changed := false
				for _, arg := range f.Args {
					if arg == nil {
						continue
					}
					before := arg.Typ
					var beforeList *planpb.Expr
					if arg.GetList() != nil {
						beforeList = DeepCopyExpr(arg)
						// Coordinate-only remapping is not a new operand type contract.
						remapGeneratedColExprPositions(beforeList, positions)
					}
					if !valid(arg) {
						return false
					}
					changed = changed || !proto.Equal(&before, &arg.Typ) ||
						(beforeList != nil && !proto.Equal(beforeList, arg))
				}
				if changed && f.Func != nil {
					// Authored and assignment CAST targets are boundaries, not
					// inferred results. Only compiler-owned character coercions
					// follow their changed input; root assignment has its own owner.
					cast := len(f.Args) == 2 && f.Args[1].GetT() != nil &&
						(f.Func.ObjName == "cast" || f.Func.ObjName == "cast_strict" ||
							f.Func.ObjName == "cast_assign" || f.Func.ObjName == "cast_ignore")
					if cast && isPersistedCharacterCoercion(e) {
						bound, err := refreshedPersistedCharacterCoercion(ctx, e)
						if err != nil {
							replay.err = err
							return false
						}
						e.Expr, e.Typ = bound.Expr, bound.Typ
					} else if !cast {
						for i, arg := range f.Args {
							var err error
							f.Args[i], err = refreshedPersistedCharacterCoercion(ctx, arg)
							if err != nil {
								replay.err = err
								return false
							}
						}
						bound, err := BindFuncExprImplByPlanExpr(ctx, f.Func.ObjName, f.Args)
						if err != nil {
							replay.err = err
							return false
						}
						preserveReboundFunctionMetadata(f, bound.GetF())
						e.Expr, e.Typ = bound.Expr, bound.Typ
					}
				}
			}
			if list := e.GetList(); list != nil {
				for _, item := range list.List {
					if !valid(item) {
						return false
					}
				}
			}
			return true
		}
		if !valid(copy) {
			return nil
		}
		return copy
	}
	// CONVERT may add a destination assignment around an otherwise unchanged
	// typed expression. Prove that exact transformation before preserving it;
	// physical dependency changes or newly authored SQL still take the ordinary binder.
	copyAssignment := func(source, target *planpb.Expr, typ planpb.Type) *planpb.Expr {
		raw := copyExpr(source, rawPositions)
		if raw == nil {
			return nil
		}
		unchanged := DeepCopyExpr(source)
		remapGeneratedColExprPositions(unchanged, rawPositions)
		if proto.Equal(raw, target) || proto.Equal(unchanged, target) {
			return copyExpr(source, visiblePositions)
		}
		converted, err := convertedPersistedAssignment(ctx, raw, typ)
		if err != nil || !proto.Equal(converted, target) {
			return nil
		}
		visible, err := convertedPersistedAssignment(ctx, copyExpr(source, visiblePositions), typ)
		if err != nil {
			return nil
		}
		return visible
	}
	for _, oldCol := range original.Cols {
		if oldCol == nil {
			continue
		}
		newCol := targetByID[oldCol.ColId]
		if oldCol.ColId == 0 || newCol == nil {
			newCol = targetByName[strings.ToLower(oldCol.Name)]
		}
		if newCol == nil {
			continue
		}
		entry := &replayedColumnExpressions{columnType: oldCol.Typ}
		if oldCol.Default != nil && newCol.Default != nil {
			expr := copyAssignment(oldCol.Default.Expr, newCol.Default.Expr, newCol.Typ)
			unchangedAbsentDefault := oldCol.Default.Expr == nil && newCol.Default.Expr == nil &&
				proto.Equal(&oldCol.Typ, &newCol.Typ) && proto.Equal(oldCol.Default, newCol.Default)
			if unchangedAbsentDefault || expr != nil {
				entry.defaultExpr = proto.Clone(newCol.Default).(*planpb.Default)
				entry.defaultExpr.Expr = expr
			}
		}
		if oldCol.OnUpdate != nil && oldCol.OnUpdate.Expr != nil && newCol.OnUpdate != nil {
			if expr := copyAssignment(oldCol.OnUpdate.Expr, newCol.OnUpdate.Expr, newCol.Typ); expr != nil {
				entry.onUpdate = proto.Clone(newCol.OnUpdate).(*planpb.OnUpdate)
				entry.onUpdate.Expr = expr
			}
		}
		if oldCol.GeneratedCol != nil && oldCol.GeneratedCol.Expr != nil && newCol.GeneratedCol != nil {
			if expr := copyAssignment(oldCol.GeneratedCol.Expr, newCol.GeneratedCol.Expr, newCol.Typ); expr != nil {
				entry.generated = proto.Clone(newCol.GeneratedCol).(*planpb.GeneratedCol)
				entry.generated.Expr = expr
			}
		}
		replay.columns[strings.ToLower(newCol.Name)] = entry
	}
	targetChecks := make(map[string]*planpb.CheckDef, len(target.Checks))
	for _, check := range target.Checks {
		if check != nil {
			targetChecks[strings.ToLower(check.Name)] = check
		}
	}
	for _, oldCheck := range original.Checks {
		if oldCheck == nil || oldCheck.Check == nil {
			continue
		}
		newCheck := targetChecks[strings.ToLower(oldCheck.Name)]
		if newCheck == nil || !proto.Equal(oldCheck.Check, newCheck.Check) {
			continue
		}
		if expr := copyExpr(oldCheck.Check, visiblePositions); expr != nil {
			copy := proto.Clone(newCheck).(*planpb.CheckDef)
			copy.Check = expr
			replay.checks[strings.ToLower(copy.Name)] = copy
		}
	}
	return context.WithValue(ctx, persistedDDLReplayKey{}, replay)
}

// WithAlterTableDDLReplay preserves COPY expressions and admits exactly the
// validated replacement width. Ordinary LIKE expression replay has no allowance.
func WithAlterTableDDLReplay(ctx context.Context, database string, original, target *planpb.TableDef) (context.Context, error) {
	if err := validateAlterColumnCount(ctx, original, target); err != nil {
		return nil, err
	}
	ctx = WithPersistedDDLReplay(ctx, original, target)
	if count := userColumnCount(target.Cols, util.TableIsClusterTable(target.TableType)); count > TableColumnCountLimit {
		ctx = defines.WithAlterCopyColumnLimit(ctx, database, target.Name, count)
	}
	return ctx, nil
}

func ddlReplayForTable(ctx context.Context, tableName string) *persistedDDLReplay {
	if ctx == nil {
		return nil
	}
	replay, _ := ctx.Value(persistedDDLReplayKey{}).(*persistedDDLReplay)
	if replay == nil || replay.tableName != strings.ToLower(tableName) {
		return nil
	}
	return replay
}
