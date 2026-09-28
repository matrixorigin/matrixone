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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

type persistedDDLReplayKey struct{}

// persistedDDLReplay carries the already-bound expressions of an existing
// table through SQL reconstruction. New or type-dependent expressions are
// still bound from the reconstructed SQL in the current session.
type persistedDDLReplay struct {
	tableName string
	columns   map[string]*replayedColumnExpressions
	checks    map[string]*planpb.CheckDef
}

type replayedColumnExpressions struct {
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
	if original == nil || target == nil {
		return ctx
	}
	replay := &persistedDDLReplay{
		tableName: strings.ToLower(target.Name),
		columns:   make(map[string]*replayedColumnExpressions),
		checks:    make(map[string]*planpb.CheckDef),
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
			oldCol.Typ.Charset == newCol.Typ.Charset &&
			oldCol.Typ.PadSpace == newCol.Typ.PadSpace
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
					return false
				}
				ref.ColPos = positions[ref.ColPos]
			}
			if f := e.GetF(); f != nil {
				for _, arg := range f.Args {
					if !valid(arg) {
						return false
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
		entry := &replayedColumnExpressions{}
		if oldCol.Default != nil && oldCol.Default.Expr != nil && newCol.Default != nil {
			if expr := copyExpr(oldCol.Default.Expr, rawPositions); expr != nil && proto.Equal(expr, newCol.Default.Expr) {
				entry.defaultExpr = proto.Clone(newCol.Default).(*planpb.Default)
				entry.defaultExpr.Expr = copyExpr(oldCol.Default.Expr, visiblePositions)
			}
		}
		if oldCol.OnUpdate != nil && oldCol.OnUpdate.Expr != nil && newCol.OnUpdate != nil {
			if expr := copyExpr(oldCol.OnUpdate.Expr, rawPositions); expr != nil && proto.Equal(expr, newCol.OnUpdate.Expr) {
				entry.onUpdate = proto.Clone(newCol.OnUpdate).(*planpb.OnUpdate)
				entry.onUpdate.Expr = copyExpr(oldCol.OnUpdate.Expr, visiblePositions)
			}
		}
		if oldCol.GeneratedCol != nil && oldCol.GeneratedCol.Expr != nil && newCol.GeneratedCol != nil {
			if expr := copyExpr(oldCol.GeneratedCol.Expr, rawPositions); expr != nil && proto.Equal(expr, newCol.GeneratedCol.Expr) {
				entry.generated = proto.Clone(newCol.GeneratedCol).(*planpb.GeneratedCol)
				entry.generated.Expr = copyExpr(oldCol.GeneratedCol.Expr, visiblePositions)
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
