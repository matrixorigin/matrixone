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

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

var activeM0Enabled bool

func m0AppendBaseFromPlan(table *plan.TableDef) (uint32, bool) {
	if !activeM0Enabled || table == nil || table.IsTemporary || table.Pkey == nil || table.Pkey.CompPkeyCol == nil ||
		table.ClusterBy != nil || table.TableType == catalog.SystemPartitionRel ||
		features.IsPartitioned(table.FeatureFlag) || features.IsPartition(table.FeatureFlag) || table.Partition != nil ||
		len(table.Indexes) != 0 || len(table.Fkeys) != 0 || len(table.RefChildTbls) != 0 ||
		len(table.Checks) != 0 ||
		len(table.Cols) < 2 {
		return 0, false
	}
	pk := table.Pkey.CompPkeyCol
	if !pk.Hidden || !pk.Primary || pk.Name != catalog.CPrimaryKeyColName ||
		table.Pkey.PkeyColName != pk.Name ||
		!objectio.IsPhysicalAddr(table.Cols[len(table.Cols)-1].Name) {
		return 0, false
	}
	base := pk.Seqnum + 1
	next := uint32(len(table.Cols) - 1)
	if base > next || uint32(table.Version) != next-base {
		return 0, false
	}
	for i, col := range table.Cols {
		if col == nil || uint32(col.Seqnum) != uint32(i) {
			return 0, false
		}
	}
	anchor := table.Cols[pk.Seqnum]
	if anchor.Name != pk.Name || !anchor.Hidden || !anchor.Primary {
		return 0, false
	}
	if err := plan.ValidateAppendMissingTable(table); err != nil {
		return 0, false
	}
	for _, col := range table.Cols[base:next] {
		if m0PlanColumnSupportsAppend(col) {
			continue
		}
		state := table.AppendMissingState
		if state == nil || col.Seqnum < state.BaseWidth || col.Seqnum-state.BaseWidth >= uint32(len(state.Columns)) ||
			state.Columns[col.Seqnum-state.BaseWidth].Kind != plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1 ||
			!plan.IsLiteralFalseDefault(col) {
			return 0, false
		}
	}
	return base, true
}

func m0PlanColumnSupportsAppend(col *plan.ColDef) bool {
	if col == nil || col.Hidden || col.Primary || col.ClusterBy || col.Typ.AutoIncr ||
		col.GeneratedCol != nil || col.OnUpdate != nil {
		return false
	}
	if col.Default == nil || !col.Default.NullAbility || col.Typ.NotNullable {
		return false
	}
	if col.Default.Expr == nil {
		return true
	}
	lit := col.Default.Expr.GetLit()
	return lit != nil && lit.GetIsnull()
}

var (
	activeM0ReadShards = func(
		_ context.Context, _ *plan.TableDef, data engine.RelData, count int, _ bool,
	) []engine.RelData {
		return data.Split(count)
	}
	activeM0CNAddColumn = func(
		*txnTable,
		context.Context,
		*api.AlterTableReq,
	) ([]engine.TableDef, error) {
		return nil, moerr.NewInternalErrorNoCtxf("M0: metadata-only ADD COLUMN is not enabled")
	}
	activeM0ReadObject = func(
		context.Context,
		*plan.TableDef,
		objectio.ObjectEntry,
		fileservice.FileService,
	) (bool, objectio.M0ObjectProof, error) {
		return false, objectio.M0ObjectProof{}, nil
	}
)

// m0MissingColumnMetadata is only for a fully proved absent physical suffix.
// It shares the reader's immutable fill rule, never the current INSERT default.
func m0MissingColumnMetadata(ctx context.Context, name string, col *plan.ColDef, rows int64) (objectio.ZoneMap, int64, error) {
	kind, ok := objectio.MissingColumnFill(ctx, name, uint16(col.Seqnum))
	if !ok {
		return nil, 0, objectio.ErrMissingColumnWithoutProof
	}
	zm := index.NewZM(types.T(col.Typ.Id), col.Typ.Scale)
	switch kind {
	case plan.MissingFillKind_MISSING_FILL_NULL_V1:
		return zm, rows, nil
	case plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1:
		if types.T(col.Typ.Id) != types.T_bool || col.Typ.Width != 0 || col.Typ.Scale != 0 {
			return nil, 0, objectio.ErrMissingColumnWithoutProof
		}
		if rows > 0 {
			zm = index.SetBool(zm, false)
		}
		return zm, 0, nil
	default:
		return nil, 0, objectio.ErrMissingColumnWithoutProof
	}
}
