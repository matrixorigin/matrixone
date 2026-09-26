// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"google.golang.org/protobuf/encoding/protowire"
)

type tableDumpWireKind uint8

const (
	tableDumpWireTable tableDumpWireKind = iota
	tableDumpWireColumn
	tableDumpWireCheck
	tableDumpWireDeclaration
	tableDumpWireExpression
	tableDumpWireFunction
	tableDumpWireExprList
	tableDumpWireLiteral
	tableDumpWireNumeric
)

// The protobuf decoder allocates one object per repeated field and recurses
// through expressions. Bound both costs before handing it a fixture payload.
func preflightTableDumpBoundPayload(data []byte) error {
	type frame struct {
		data []byte
		kind tableDumpWireKind
	}
	stack := []frame{{data: data, kind: tableDumpWireTable}}
	var fields, columns, checks int
	for len(stack) != 0 {
		current := &stack[len(stack)-1]
		if len(current.data) == 0 {
			stack = stack[:len(stack)-1]
			continue
		}
		fields++
		if fields > 100_000 {
			return moerr.NewInvalidInputNoCtx("table dump expression metadata has too many fields")
		}
		number, wireType, n := protowire.ConsumeTag(current.data)
		if n < 0 {
			return moerr.NewInvalidInputNoCtx("invalid table dump expression wire data")
		}
		current.data = current.data[n:]
		var child tableDumpWireKind
		var nested bool
		switch current.kind {
		case tableDumpWireTable:
			switch number {
			case 4:
				columns++
				child, nested = tableDumpWireColumn, true
				if columns > 16_384 {
					return moerr.NewInvalidInputNoCtx("table dump has too many columns")
				}
			case 15:
				checks++
				child, nested = tableDumpWireCheck, true
				if checks > 16_384 {
					return moerr.NewInvalidInputNoCtx("table dump has too many checks")
				}
			default:
				return moerr.NewInvalidInputNoCtx("unexpected table dump expression metadata")
			}
		case tableDumpWireColumn:
			switch number {
			case 5, 7, 9, 20:
				// Type has no recursive expression fields.
				if number != 5 {
					child, nested = tableDumpWireDeclaration, true
				}
			}
		case tableDumpWireCheck:
			if number == 2 {
				child, nested = tableDumpWireExpression, true
			}
		case tableDumpWireDeclaration:
			if number == 1 {
				child, nested = tableDumpWireExpression, true
			}
		case tableDumpWireExpression:
			switch number {
			case 2:
				child, nested = tableDumpWireLiteral, true
			case 7:
				child, nested = tableDumpWireFunction, true
			case 12:
				child, nested = tableDumpWireExprList, true
			case 19:
				child, nested = tableDumpWireNumeric, true
			case 8, 9:
				return moerr.NewInvalidInputNoCtx("unsupported table dump expression kind")
			}
		case tableDumpWireFunction:
			if number == 2 {
				child, nested = tableDumpWireExpression, true
			}
		case tableDumpWireExprList:
			if number == 1 {
				child, nested = tableDumpWireExpression, true
			}
		case tableDumpWireLiteral:
			if number == 26 {
				child, nested = tableDumpWireExpression, true
			}
		case tableDumpWireNumeric:
			if number == 11 {
				child, nested = tableDumpWireExpression, true
			}
		}
		if nested && wireType != protowire.BytesType {
			return moerr.NewInvalidInputNoCtx("invalid table dump expression wire type")
		}
		if wireType == protowire.BytesType {
			value, consumed := protowire.ConsumeBytes(current.data)
			if consumed < 0 {
				return moerr.NewInvalidInputNoCtx("invalid table dump expression wire data")
			}
			current.data = current.data[consumed:]
			if nested {
				if len(stack) >= 64 {
					return moerr.NewInvalidInputNoCtx("table dump expression nesting exceeds limit")
				}
				stack = append(stack, frame{data: value, kind: child})
			}
			continue
		}
		consumed := protowire.ConsumeFieldValue(number, wireType, current.data)
		if consumed < 0 {
			return moerr.NewInvalidInputNoCtx("invalid table dump expression wire data")
		}
		current.data = current.data[consumed:]
	}
	return nil
}

// tableDumpBoundExpressions stores only declarations and already-bound
// expressions. Physical IDs and other catalog state belong to the target.
func tableDumpBoundExpressions(def *plan.TableDef) ([]byte, string, error) {
	if def == nil {
		return nil, "", moerr.NewInternalErrorNoCtx("table definition is unavailable")
	}
	bound := &plan.TableDef{Cols: make([]*plan.ColDef, len(def.Cols)), Checks: def.Checks}
	for i, col := range def.Cols {
		if col == nil {
			return nil, "", moerr.NewInvalidInputNoCtx("table dump contains a nil column")
		}
		bound.Cols[i] = &plan.ColDef{
			Name: col.Name, Typ: col.Typ, Hidden: col.Hidden,
			Default: col.Default, OnUpdate: col.OnUpdate, GeneratedCol: col.GeneratedCol,
		}
	}
	data, err := bound.Marshal()
	if err != nil {
		return nil, "", err
	}
	if len(data) > tableDumpMaxManifest/2 {
		return nil, "", moerr.NewInvalidInputNoCtx("table dump expression metadata exceeds manifest limit")
	}
	if err := preflightTableDumpBoundPayload(data); err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(data)
	return data, hex.EncodeToString(sum[:]), nil
}

func tableDumpHasBoundExpressions(def *plan.TableDef) bool {
	if def == nil {
		return false
	}
	for _, col := range def.Cols {
		if col != nil && ((col.Default != nil && col.Default.Expr != nil) ||
			(col.OnUpdate != nil && col.OnUpdate.Expr != nil) ||
			(col.GeneratedCol != nil && col.GeneratedCol.Expr != nil)) {
			return true
		}
	}
	for _, check := range def.Checks {
		if check != nil && check.Check != nil {
			return true
		}
	}
	return false
}

func tableDumpMayDependOnDivision(def *plan.TableDef) bool {
	if def == nil {
		return false
	}
	for _, col := range def.Cols {
		if col == nil {
			continue
		}
		if col.Default != nil && col.Default.Expr != nil && strings.Contains(col.Default.OriginString, "/") ||
			col.OnUpdate != nil && col.OnUpdate.Expr != nil && strings.Contains(col.OnUpdate.OriginString, "/") ||
			col.GeneratedCol != nil && col.GeneratedCol.Expr != nil && strings.Contains(col.GeneratedCol.OriginString, "/") {
			return true
		}
	}
	for _, check := range def.Checks {
		if check != nil && check.Check != nil && strings.Contains(check.OriginSql, "/") {
			return true
		}
	}
	return false
}

// tableDumpRestoredExpressions validates the declaration skeleton before
// transplanting bound expressions. A table created from the same SQL under a
// different session value has matching declarations but different bindings.
func tableDumpRestoredExpressions(target *plan.TableDef, payload []byte, digest string) (*plan.TableDef, bool, error) {
	if target == nil || len(payload) == 0 || len(payload) > tableDumpMaxManifest/2 {
		return nil, false, moerr.NewInvalidInputNoCtx("invalid table dump expression metadata")
	}
	sum := sha256.Sum256(payload)
	if !strings.EqualFold(hex.EncodeToString(sum[:]), digest) {
		return nil, false, moerr.NewInvalidInputNoCtx("table dump expression checksum does not match")
	}
	if err := preflightTableDumpBoundPayload(payload); err != nil {
		return nil, false, err
	}
	var bound plan.TableDef
	if err := bound.Unmarshal(payload); err != nil {
		return nil, false, moerr.NewInvalidInputNoCtxf("invalid table dump expression metadata: %v", err)
	}
	if len(bound.Cols) != len(target.Cols) || len(bound.Checks) != len(target.Checks) ||
		!tableDumpHasBoundExpressions(&bound) {
		return nil, false, moerr.NewInvalidInputNoCtxf("table dump expressions do not match target schema: cols %d/%d checks %d/%d", len(bound.Cols), len(target.Cols), len(bound.Checks), len(target.Checks))
	}
	replacement := sqlplan.DeepCopyTableDef(target, true)
	changed := false
	sourceByName := make(map[string]*plan.ColDef, len(bound.Cols))
	for _, source := range bound.Cols {
		if source == nil {
			return nil, false, moerr.NewInvalidInputNoCtx("table dump contains a nil column")
		}
		key := strings.ToLower(source.Name)
		if _, exists := sourceByName[key]; exists {
			return nil, false, moerr.NewInvalidInputNoCtx("table dump contains duplicate columns")
		}
		sourceByName[key] = source
	}
	for i, dest := range target.Cols {
		if dest == nil {
			return nil, false, moerr.NewInvalidInputNoCtx("target table contains a nil column")
		}
		source := sourceByName[strings.ToLower(dest.Name)]
		if source == nil || !strings.EqualFold(source.Name, dest.Name) ||
			source.Hidden != dest.Hidden || !sameTableDumpValueType(source.Typ, dest.Typ) {
			return nil, false, moerr.NewInvalidInputNoCtx("table dump column does not match target schema")
		}
		if (source.Default != nil && strings.Contains(source.Default.OriginString, "/") ||
			dest.Default != nil && strings.Contains(dest.Default.OriginString, "/")) &&
			!sameTableDumpDefaultDeclaration(source.Default, dest.Default) {
			return nil, false, moerr.NewInvalidInputNoCtx("table dump default does not match target schema")
		}
		if (source.OnUpdate != nil && strings.Contains(source.OnUpdate.OriginString, "/") ||
			dest.OnUpdate != nil && strings.Contains(dest.OnUpdate.OriginString, "/")) &&
			!sameTableDumpOnUpdateDeclaration(source.OnUpdate, dest.OnUpdate) {
			return nil, false, moerr.NewInvalidInputNoCtx("table dump on-update expression does not match target schema")
		}
		if (source.GeneratedCol != nil && strings.Contains(source.GeneratedCol.OriginString, "/") ||
			dest.GeneratedCol != nil && strings.Contains(dest.GeneratedCol.OriginString, "/")) &&
			!sameTableDumpGeneratedDeclaration(source.GeneratedCol, dest.GeneratedCol) {
			return nil, false, moerr.NewInvalidInputNoCtx("table dump generated expression does not match target schema")
		}
		if source.Default != nil && strings.Contains(source.Default.OriginString, "/") {
			changed = changed || !proto.Equal(source.Default, dest.Default)
			replacement.Cols[i].Default = source.Default
		}
		if source.OnUpdate != nil && strings.Contains(source.OnUpdate.OriginString, "/") {
			changed = changed || !proto.Equal(source.OnUpdate, dest.OnUpdate)
			replacement.Cols[i].OnUpdate = source.OnUpdate
		}
		if source.GeneratedCol != nil && strings.Contains(source.GeneratedCol.OriginString, "/") {
			changed = changed || !proto.Equal(source.GeneratedCol, dest.GeneratedCol)
			replacement.Cols[i].GeneratedCol = source.GeneratedCol
		}
	}
	sourceChecks := make(map[string]*plan.CheckDef, len(bound.Checks))
	for _, source := range bound.Checks {
		if source == nil {
			return nil, false, moerr.NewInvalidInputNoCtx("table dump contains a nil check")
		}
		key := strings.ToLower(source.Name)
		if _, exists := sourceChecks[key]; exists {
			return nil, false, moerr.NewInvalidInputNoCtx("table dump contains duplicate checks")
		}
		sourceChecks[key] = source
	}
	for i, dest := range target.Checks {
		if dest == nil {
			return nil, false, moerr.NewInvalidInputNoCtx("target table contains a nil check")
		}
		source := sourceChecks[strings.ToLower(dest.Name)]
		if source == nil || !strings.EqualFold(source.Name, dest.Name) ||
			source.OriginSql != dest.OriginSql {
			return nil, false, moerr.NewInvalidInputNoCtx("table dump checks do not match target schema")
		}
		if strings.Contains(source.OriginSql, "/") {
			changed = changed || !proto.Equal(source.Check, dest.Check)
			replacement.Checks[i].Check = source.Check
		}
	}
	return replacement, changed, nil
}

func sameTableDumpValueType(a, b plan.Type) bool {
	// Table and NotNullable describe catalog lineage and eligibility, not the
	// value representation that a saved expression consumes.
	return a.Id == b.Id && a.Width == b.Width && a.Scale == b.Scale &&
		a.AutoIncr == b.AutoIncr && a.Enumvalues == b.Enumvalues &&
		a.Charset == b.Charset && a.PadSpace == b.PadSpace
}

func sameTableDumpDefaultDeclaration(a, b *plan.Default) bool {
	return (a == nil && b == nil) || (a != nil && b != nil &&
		a.OriginString == b.OriginString && a.NullAbility == b.NullAbility &&
		(a.Expr == nil) == (b.Expr == nil))
}

func sameTableDumpOnUpdateDeclaration(a, b *plan.OnUpdate) bool {
	return (a == nil && b == nil) || (a != nil && b != nil &&
		a.OriginString == b.OriginString && (a.Expr == nil) == (b.Expr == nil))
}

func sameTableDumpGeneratedDeclaration(a, b *plan.GeneratedCol) bool {
	return (a == nil && b == nil) || (a != nil && b != nil &&
		a.OriginString == b.OriginString && a.IsStored == b.IsStored &&
		(a.Expr == nil) == (b.Expr == nil))
}

// ReplaceDef recreates the catalog row. Preserve its original SQL ownership
// rather than accidentally assigning it to the account running LOAD.
func tableDumpTargetOwnership(ctx context.Context, ses *Session, tableID uint64) (uint32, uint32, int64, error) {
	bh := ses.GetShareTxnBackgroundExec(ctx, false)
	defer bh.Close()
	bh.ClearExecResultSet()
	err := bh.Exec(ctx, fmt.Sprintf(
		"select creator, owner, created_time from %s.%s where account_id=%d and rel_id=%d limit 1",
		catalog.MO_CATALOG, catalog.MO_TABLES, ses.GetAccountId(), tableID))
	if err != nil {
		return 0, 0, 0, err
	}
	results, err := getResultSet(ctx, bh)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(results) != 1 || results[0].GetRowCount() != 1 {
		return 0, 0, 0, moerr.NewInvalidInputNoCtx("target table identity changed during LOAD")
	}
	creator, err := results[0].GetUint64(ctx, 0, 0)
	if err != nil {
		return 0, 0, 0, err
	}
	owner, err := results[0].GetUint64(ctx, 0, 1)
	if err != nil {
		return 0, 0, 0, err
	}
	if creator > uint64(^uint32(0)) || owner > uint64(^uint32(0)) {
		return 0, 0, 0, moerr.NewInvalidInputNoCtx("invalid target table ownership")
	}
	valueResult, ok := results[0].(interface {
		GetValue(context.Context, uint64, uint64) (interface{}, error)
	})
	if !ok {
		return 0, 0, 0, moerr.NewInternalErrorNoCtx("target table creation time is unavailable")
	}
	value, err := valueResult.GetValue(ctx, 0, 2)
	if err != nil {
		return 0, 0, 0, err
	}
	var created types.Timestamp
	switch typed := value.(type) {
	case types.Timestamp:
		created = typed
	case string:
		// BackgroundExec renders TIMESTAMP in the session time zone.
		// Parse with that same zone to recover the catalog's UTC value.
		created, err = types.ParseTimestamp(ses.GetTimeZone(), typed, 0)
		if err != nil {
			return 0, 0, 0, moerr.NewInvalidInputNoCtx("invalid target table creation time")
		}
	default:
		return 0, 0, 0, moerr.NewInvalidInputNoCtx("invalid target table creation time")
	}
	return uint32(creator), uint32(owner), int64(created), nil
}
