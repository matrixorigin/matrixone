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
	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

var activeM0Enabled bool

var activeM0CreateEvolution = func(context.Context, *txnTable, bool) error { return nil }
var activeM0CheckReadEvolution = func(context.Context, *txnTable) error { return nil }
var activeM0CheckEvolution = func(context.Context, *txnTable) error { return nil }
var activeM0AdvanceEvolution = func(context.Context, *txnTable) error { return nil }

var activeM0ValidateRemoteMembers = func(context.Context, *txnTable, []engine.Relation) error { return nil }

// The experimental reader must bind flushed tombstones to the selected
// relation and workspace boundary. The default reader retains its contract.
var activeM0ScopedFlushedTombstones func(*txnTable, int, bool) ([]objectio.ObjectStats, error)
var activeM0RetireFlushedTombstones func(*Transaction, func(int, Entry) bool)

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
	generation := table.Version
	if c, err := m0read.PlanEvolution(table); err != nil {
		return 0, false
	} else if c != nil {
		if err := m0read.ValidateEvolution(c, m0read.EvolutionBinding{Account: c.AccountId, Database: table.DbId, Relation: table.TblId}); err != nil {
			return 0, false
		}
		generation = c.LayoutGeneration
	}
	if base > next || generation != next-base {
		return 0, false
	}
	for i, col := range table.Cols {
		if col == nil || !objectio.IsPhysicalAddr(col.Name) && uint32(col.Seqnum) != uint32(i) {
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
	activeM0PrepareWorkspacePhysicalRewrite = func(
		_ context.Context, _ *txnTable, _ objectio.ObjectStats, _ fileservice.FileService,
	) (*workspacePhysicalRewrite, error) {
		return nil, nil
	}
	activeM0WorkspaceRewriteAfterSync = func(context.Context, []string) error { return nil }
	activeM0ReadShards                = func(
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
	activeM0ObjectReadLayout = func(
		context.Context,
		*plan.TableDef,
		objectio.ObjectEntry,
		fileservice.FileService,
	) (bool, m0read.ObjectReadLayout, error) {
		return false, m0read.ObjectReadLayout{}, nil
	}
	activeM0ValidateRangeFilters = func(
		context.Context, *txnTable, *plan.TableDef, timestamp.Timestamp, engine.RangesParam,
		*logtailreplay.PartitionState, []objectio.ObjectStats,
	) (bool, error) {
		return true, nil
	}
)

// A strict Engine metadata consumer requires real columns without authority.
// MustGetColumn returns empty metadata for absent columns; do not interpret
// those zero values as a real NULL count or zone map without authority.
func requirePhysicalMetadataColumns(meta objectio.ObjectDataMeta, cols []*plan.ColDef, table *plan.TableDef) error {
	for _, col := range cols {
		// Preserve the pre-existing system-catalog logical-ID exception only
		// for this bound relation and exact column identity.
		if table != nil && table.DbId == catalog.MO_CATALOG_ID && table.TblId == catalog.MO_TABLES_ID && table.Name == catalog.MO_TABLES && col.Name == catalog.SystemRelAttr_LogicalID && col.Seqnum == uint32(catalog.MO_TABLES_LOGICAL_ID_IDX) && col.Typ.Id == int32(types.T_uint64) {
			continue
		}
		if col.Seqnum > uint32(meta.BlockHeader().MaxSeqnum()) {
			return m0read.ErrMissingColumnWithoutProof
		}
		for i := uint32(0); i < meta.BlockCount(); i++ {
			block := meta.GetBlockMeta(i)
			if col.Seqnum > uint32(block.GetMaxSeqnum()) || block.MustGetColumn(uint16(col.Seqnum)).DataType() == 0 {
				return m0read.ErrMissingColumnWithoutProof
			}
		}
	}
	return nil
}
