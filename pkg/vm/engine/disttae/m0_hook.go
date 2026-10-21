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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var activeM0Enabled bool

func m0AppendBaseFromPlan(table *plan.TableDef) (uint32, bool) {
	if !activeM0Enabled || table == nil || table.IsTemporary || table.Pkey == nil || table.Pkey.CompPkeyCol == nil ||
		table.ClusterBy != nil || table.TableType == catalog.SystemPartitionRel ||
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
	for _, col := range table.Cols[base:next] {
		if !m0PlanColumnSupportsAppend(col) {
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
	activeM0CNAddColumn = func(
		*txnTable,
		context.Context,
		*api.AlterTableReq,
	) ([]engine.TableDef, error) {
		return nil, fmt.Errorf("M0: metadata-only ADD COLUMN is not enabled")
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
