//go:build mo_m0

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
	"slices"
	"strings"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"go.uber.org/zap"
)

func init() {
	activeM0Enabled = true
	activeM0ValidateRemoteMembers = validateM0RemoteMemberMappings
	activeM0CNAddColumn = buildM0CNAddColumn
	activeM0ObjectReadLayout = buildM0ObjectReadLayout
	activeM0ValidateRangeFilters = validateM0RangeFilters
	activeM0ReadShards = splitM0ReadShards
	activeM0ScopedFlushedTombstones = m0ScopedFlushedTombstones
	activeM0RetireFlushedTombstones = m0RetireFlushedTombstones
}

// Called with the workspace locked before entries are released. This only
// retires the transaction's in-memory index, never the shared object file.
func m0RetireFlushedTombstones(txn *Transaction, discard func(int, Entry) bool) {
	removed := make(map[objectio.ObjectStats]struct{})
	retained := make(map[objectio.ObjectStats]struct{})
	for i, entry := range txn.writes {
		if entry.typ != DELETE || entry.fileName == "" {
			continue
		}
		target := retained
		if discard(i, entry) {
			target = removed
		}
		for _, stats := range collectObjectStatsFromEntry(entry) {
			target[stats] = struct{}{}
		}
	}
	for stats := range removed {
		if _, stillReferenced := retained[stats]; !stillReferenced {
			txn.cn_flushed_s3_tombstone_object_stats_list.Delete(stats)
		}
	}
}

// m0ScopedFlushedTombstones copies the visible entry-owned stats while the
// workspace is stable. It rejects map-only stats rather than guessing their
// relation; callers may already hold the non-reentrant workspace lock.
func m0ScopedFlushedTombstones(tbl *txnTable, offset int, locked bool) ([]objectio.ObjectStats, error) {
	txn := tbl.getTxn()
	if tbl.db.op.IsSnapOp() {
		offset = txn.GetSnapshotWriteOffset()
	}
	if !locked {
		txn.Lock()
		defer txn.Unlock()
	}
	known := make(map[objectio.ObjectStats]struct{})
	selected := make(map[objectio.ObjectStats]struct{})
	var result []objectio.ObjectStats
	for i, entry := range txn.writes {
		if entry.typ != DELETE || entry.fileName == "" || entry.bat == nil || entry.bat.IsEmpty() {
			continue
		}
		idx := slices.Index(entry.bat.Attrs, pkgcatalog.ObjectMeta_ObjectStats)
		if idx < 0 || idx >= len(entry.bat.Vecs) || entry.bat.Vecs[idx] == nil {
			return nil, moerr.NewInternalErrorNoCtx("M0: flushed tombstone entry lacks object stats")
		}
		for j := 0; j < entry.bat.Vecs[idx].Length(); j++ {
			var stats objectio.ObjectStats
			data := entry.bat.Vecs[idx].GetBytesAt(j)
			if len(data) != len(stats) {
				return nil, moerr.NewInternalErrorNoCtx("M0: invalid flushed tombstone stats")
			}
			stats.UnMarshal(data)
			known[stats] = struct{}{}
			if i < offset && entry.accountId == tbl.accountId && entry.databaseId == tbl.db.databaseId && entry.tableId == tbl.tableId {
				if _, exists := selected[stats]; !exists {
					selected[stats] = struct{}{}
					result = append(result, stats)
				}
			}
		}
	}
	unknown := false
	txn.cn_flushed_s3_tombstone_object_stats_list.Range(func(k, _ any) bool {
		stats, ok := k.(objectio.ObjectStats)
		if !ok {
			unknown = true
			return false
		}
		_, ok = known[stats]
		unknown = !ok
		return ok
	})
	if unknown {
		return nil, moerr.NewInternalErrorNoCtx("M0: flushed tombstone has no workspace owner")
	}
	return result, nil
}

// validateM0RangeFilters establishes the missing-column interpretation before
// either ranges filter can discard a selected snapshot object. The scope is
// this relation's snapshot and this invocation; no proof survives the call.
func validateM0RangeFilters(
	ctx context.Context, tbl *txnTable, table *plan.TableDef, snapshot timestamp.Timestamp, param engine.RangesParam,
	state *logtailreplay.PartitionState, uncommitted []objectio.ObjectStats,
) (bool, error) {
	if len(param.BlockFilters) == 0 {
		return true, nil
	}
	if table == nil {
		return false, moerr.NewInternalErrorNoCtx("M0: ranges filter has no table definition")
	}
	filterColumns := make(map[uint16]struct{})
	for _, filter := range param.BlockFilters {
		if err := collectM0RangeFilterColumns(table, filter, filterColumns); err != nil {
			return false, err
		}
	}
	// Ranges does not own the subsequent reader's projection. Inspect the
	// whole relation schema rather than trusting a caller-supplied scan node.
	columns := make(map[uint16]struct{}, len(table.Cols))
	for _, col := range table.Cols {
		if col == nil {
			return false, moerr.NewInternalErrorNoCtx("M0: nil ranges schema column")
		}
		if objectio.IsPhysicalAddr(col.Name) || col.Seqnum >= uint32(objectio.SEQNUM_UPPER) {
			continue
		}
		seq := uint16(col.Seqnum)
		if _, duplicate := columns[seq]; duplicate {
			return false, moerr.NewInternalErrorNoCtxf("M0: duplicate ranges column sequence %d", seq)
		}
		columns[seq] = struct{}{}
	}
	if len(columns) == 0 {
		return true, nil
	}
	fs := tbl.getTxn().engine.fs
	_, appendDomain := m0AppendBaseFromPlan(table)
	canPrune := true
	err := ForeachSnapshotObjects(snapshot, func(obj objectio.ObjectEntry, committed bool) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if committed && plan2.ShouldSkipObjByShuffle(param.Rsp, &obj.ObjectStats) {
			return nil
		}
		if appendDomain {
			proven, _, err := activeM0ObjectReadLayout(ctx, table, obj, fs)
			if err != nil {
				return err
			}
			if proven {
				return nil
			}
		}
		location := obj.ObjectLocation()
		meta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
		if err != nil {
			return err
		}
		data := meta.MustDataMeta()
		if data.IsEmpty() || data.BlockCount() == 0 {
			return moerr.NewInternalErrorNoCtx("M0: ranges object has no data blocks")
		}
		for seq := range columns {
			for i := uint32(0); i < data.BlockCount(); i++ {
				block := data.GetBlockMeta(i)
				if seq > block.GetMaxSeqnum() || seq >= block.GetMetaColumnCount() || block.ColumnMeta(seq).DataType() == 0 {
					if _, required := filterColumns[seq]; required {
						return m0read.ErrMissingColumnWithoutProof
					}
					// The unknown column may be projected later. Keep the block
					// and let the ordinary reader apply its existing contract.
					canPrune = false
					break
				}
			}
		}
		return nil
	}, state, nil, uncommitted...)
	return canPrune, err
}

// collectM0RangeFilterColumns resolves names against the same table definition
// as ranges; a missing name must not silently become column zero.
func collectM0RangeFilterColumns(table *plan.TableDef, expr *plan.Expr, columns map[uint16]struct{}) error {
	if expr == nil {
		return nil
	}
	switch node := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range node.F.Args {
			if err := collectM0RangeFilterColumns(table, arg, columns); err != nil {
				return err
			}
		}
	case *plan.Expr_Col:
		name := node.Col.Name
		if dot := strings.Index(name, "."); dot >= 0 {
			name = name[dot+1:]
		}
		idx, ok := table.Name2ColIndex[name]
		if !ok || idx < 0 || int(idx) >= len(table.Cols) || table.Cols[idx] == nil {
			return moerr.NewInternalErrorNoCtxf("M0: ranges filter column %q is not in the target schema", name)
		}
		col := table.Cols[idx]
		if objectio.IsPhysicalAddr(col.Name) {
			return nil
		}
		if col.Seqnum >= uint32(objectio.SEQNUM_UPPER) {
			return moerr.NewInternalErrorNoCtxf("M0: invalid ranges column sequence %d", col.Seqnum)
		}
		columns[uint16(col.Seqnum)] = struct{}{}
	}
	return nil
}

func splitM0ReadShards(
	_ context.Context, table *plan.TableDef, data engine.RelData, count int, ordered bool,
) []engine.RelData {
	_, appendDomain := m0AppendBaseFromPlan(table)
	if ordered || !appendDomain || count <= 1 || data.DataCnt() <= count {
		return data.Split(count)
	}
	blocks, materialized := data.(*readutil.BlockListRelData)
	if objects, ok := data.(*readutil.ObjListRelData); ok {
		// Range shuffle owns reader assignment; preserve that partitioning.
		if rsp := objects.Rsp; rsp != nil && rsp.Node.GetStats().GetHashmapStats().GetShuffle() &&
			rsp.Node.GetStats().GetHashmapStats().GetShuffleType() == plan.ShuffleType_Range {
			return data.Split(count)
		}
		// This expands existing block descriptors, not object payloads.
		list := objects.GetBlockInfoSlice()
		blocks, materialized = objects.DataSlice(0, list.Len()).(*readutil.BlockListRelData)
	}
	if !materialized || blocks.DataCnt() <= count {
		return data.Split(count)
	}
	shards := make([]engine.RelData, count)
	for i := range shards {
		shards[i] = blocks.BuildEmptyRelData((blocks.DataCnt() + count - 1) / count)
		_ = shards[i].AttachTombstones(blocks.GetTombstones())
	}
	// Contiguous history can place all surviving rows in only the last readers.
	// Striping preserves every block and keeps a leading memory block in reader 0.
	for i := 0; i < blocks.DataCnt(); i++ {
		block := blocks.GetBlockInfo(i)
		shards[i%count].AppendBlockInfo(&block)
	}
	return shards
}

func buildM0ObjectReadLayout(
	ctx context.Context,
	table *plan.TableDef,
	obj objectio.ObjectEntry,
	fs fileservice.FileService,
) (bool, m0read.ObjectReadLayout, error) {
	if table == nil {
		return false, m0read.ObjectReadLayout{}, nil
	}
	raw, parseErr := m0read.PlanEvolution(table)
	if parseErr != nil {
		return true, m0read.ObjectReadLayout{}, parseErr
	}
	if raw == nil {
		return false, m0read.ObjectReadLayout{}, nil
	}
	account, err := defines.GetAccountId(ctx)
	if err != nil {
		return true, m0read.ObjectReadLayout{}, err
	}
	contract, err := m0read.ValidatePlanEvolution(table, account)
	if err != nil {
		return true, m0read.ObjectReadLayout{}, err
	}
	if contract == nil {
		return false, m0read.ObjectReadLayout{}, nil
	}
	next := uint32(len(table.Cols) - 1)
	layout, err := catalog.M0AppendLayoutFromPlan(table, next)
	if err != nil {
		return true, m0read.ObjectReadLayout{}, err
	}
	if err = m0read.ValidateM0AppendLayout(layout); err != nil {
		return true, m0read.ObjectReadLayout{}, err
	}
	location := obj.Location()
	reader, err := objectio.NewObjectReaderWithStr(location.Name().String(), fs)
	if err != nil {
		return true, m0read.ObjectReadLayout{}, err
	}
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	header, err := reader.ReadHeader(ctx, mp)
	if err != nil {
		return true, m0read.ObjectReadLayout{}, err
	}
	meta, err := reader.ReadAllMeta(ctx, mp)
	if err != nil {
		return true, m0read.ObjectReadLayout{}, err
	}
	width, err := m0read.ValidateM0Object(layout, header, meta.MustDataMeta())
	return true, m0read.ObjectReadLayout{
		Evolution:   true,
		ObjectName:  location.Name().String(),
		WrittenSize: width,
		SchemaSize:  layout.NextColSeqnum,
	}, err
}

func buildM0CNAddColumn(
	tbl *txnTable,
	ctx context.Context,
	req *api.AlterTableReq,
) ([]engine.TableDef, error) {
	if tbl == nil || req == nil || req.GetKind() != api.AlterKind_AddColumn {
		return nil, moerr.NewInternalErrorNoCtxf("M0: invalid CN ADD COLUMN request")
	}
	if tbl.db.op.IsSnapOp() {
		return nil, moerr.NewInternalErrorNoCtxf("M0: snapshot transaction cannot alter a table")
	}
	op := tbl.getTxn().op
	if op == nil {
		return nil, moerr.NewInternalErrorNoCtxf("M0: missing transaction operator")
	}
	options := op.TxnOptions()
	if options.GetByBegin() || !options.GetAutocommit() ||
		!op.Txn().IsPessimistic() || !op.Txn().IsRCIsolation() {
		return nil, moerr.NewInternalErrorNoCtxf("M0: ADD COLUMN requires independent pessimistic RC autocommit")
	}
	if created, err := tbl.isCreatedInTxn(ctx); err != nil {
		return nil, err
	} else if created {
		return nil, moerr.NewInternalErrorNoCtxf("M0: table created in the transaction is outside the persisted-root domain")
	}

	add := req.GetAddColumn()
	if add == nil || add.Column == nil || add.Column.Hidden || add.Column.Primary || add.Column.Unique ||
		add.Column.ClusterBy || add.Column.NotNull ||
		add.Column.Default == nil || !add.Column.Default.NullAbility || add.Column.Typ.NotNullable ||
		add.Column.GeneratedCol != nil || add.Column.OnUpdate != nil || add.Column.Typ.AutoIncr ||
		add.Column.Comment != "" {
		return nil, moerr.NewInternalErrorNoCtxf("M0: unsupported appended column")
	}
	if add.Column.Typ.Id == int32(types.T_enum) || len(add.Column.Typ.Enumvalues) > 0 {
		return nil, moerr.NewInternalErrorNoCtxf("M0: ENUM and SET appended columns are outside the append domain")
	}
	if add.Column.Default.Expr != nil {
		lit := add.Column.Default.Expr.GetLit()
		if lit == nil || !lit.GetIsnull() {
			return nil, moerr.NewInternalErrorNoCtxf("M0: non-NULL default requires backfill")
		}
	}

	table := tbl.GetTableDef(ctx)
	if !m0AppendPositionAllowed(add.InsertPosition, len(table.Cols)) {
		return nil, moerr.NewInternalErrorNoCtx("M0: ADD COLUMN must append before rowid")
	}
	if strings.TrimSpace(add.Column.Name) == "" {
		return nil, moerr.NewInternalErrorNoCtx("M0: appended column has no name")
	}
	for _, existing := range table.Cols {
		if existing == nil {
			return nil, moerr.NewInternalErrorNoCtx("M0: existing column definition is missing")
		}
		if strings.EqualFold(existing.Name, add.Column.Name) {
			return nil, moerr.NewInternalErrorNoCtx("M0: duplicate appended column name")
		}
	}
	if table.TableType == pkgcatalog.SystemPartitionRel ||
		features.IsPartitioned(table.FeatureFlag) || features.IsPartition(table.FeatureFlag) || table.Partition != nil ||
		tbl.partitioned > 0 || tbl.partition != "" {
		return nil, moerr.NewInternalErrorNoCtxf("M0: partitioned tables and backing partitions require COPY")
	}
	if len(table.Indexes) != 0 || len(table.Fkeys) != 0 ||
		len(table.RefChildTbls) != 0 || len(table.Checks) != 0 {
		return nil, moerr.NewInternalErrorNoCtxf("M0: secondary indexes, foreign keys and checks are outside the append domain")
	}
	if err := validateM0Evolution(tbl, table); err != nil {
		return nil, err
	}
	next := uint32(len(table.Cols) - 1)
	if tbl.extraInfo == nil || tbl.extraInfo.NextColSeqnum != next {
		return nil, moerr.NewInternalErrorNoCtxf("M0: stale NextColSeqnum")
	}
	before, err := catalog.M0AppendLayoutFromPlan(table, next)
	if err != nil {
		return nil, err
	}
	if err = m0read.ValidateM0AppendLayout(before); err != nil {
		return nil, err
	}

	afterDef := plan2.DeepCopyTableDef(table, true)
	newCol := &plan.ColDef{
		Name:       strings.ToLower(add.Column.Name),
		OriginName: add.Column.Name,
		Typ:        add.Column.Typ,
		ColId:      uint64(next + 1),
		Seqnum:     next,
		Default: &plan.Default{
			NullAbility:  true,
			Expr:         nil,
			OriginString: "",
		},
	}
	afterDef.Cols = append(afterDef.Cols[:len(afterDef.Cols)-1],
		append([]*plan.ColDef{newCol}, afterDef.Cols[len(afterDef.Cols)-1:]...)...)
	afterDef.Name2ColIndex[newCol.Name] = int32(len(afterDef.Cols) - 2)
	afterDef.Version++
	if err = validateM0CNColumnTransition(table, afterDef); err != nil {
		return nil, err
	}
	after, err := catalog.M0AppendLayoutFromPlan(afterDef, next+1)
	if err != nil {
		return nil, err
	}
	// This is a private candidate, still carrying the old persisted authority.
	// Only the checked ADD transition may propose the next layout generation;
	// publication below advances schema and contract together.
	after.Version = before.Version + 1
	if err = m0read.ValidateM0AppendTransition(before, after); err != nil {
		return nil, err
	}

	if err = m0ValidateNoUncommittedData(tbl); err != nil {
		return nil, err
	}
	part, err := tbl.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}
	fs := tbl.getTxn().engine.fs
	if err = ForeachVisibleObjects(
		ctx,
		part,
		types.TimestampToTS(tbl.db.op.SnapshotTS()),
		func(objCtx context.Context, obj objectio.ObjectEntry) error {
			ok, _, proofErr := buildM0ObjectReadLayout(objCtx, table, obj, fs)
			if !ok {
				return moerr.NewInternalErrorNoCtxf("M0: current relation has no admissible object root")
			}
			return proofErr
		},
		nil,
		false,
	); err != nil {
		return nil, err
	}
	logutil.Info("M0_ADMIT_ADD_COLUMN",
		zap.String("table", table.Name),
		zap.String("column", newCol.Name),
		zap.Uint32("source_version", tbl.version),
		zap.Uint32("new_seqnum", next),
	)
	return m0AppendCNDefs(tbl.defs, newCol)
}

func m0ValidateNoUncommittedData(tbl *txnTable) error {
	txn := tbl.getTxn()
	txn.Lock()
	defer txn.Unlock()
	for _, entry := range txn.writes {
		if entry.typ != INSERT || entry.databaseId != tbl.db.databaseId || entry.tableId != tbl.tableId ||
			entry.bat == nil || entry.bat.IsEmpty() {
			continue
		}
		idx := slices.Index(entry.bat.Attrs, pkgcatalog.ObjectMeta_ObjectStats)
		if idx < 0 {
			return moerr.NewInternalErrorNoCtx("M0: uncommitted data rows are outside the persisted-root domain")
		}
		if idx >= len(entry.bat.Vecs) || entry.bat.Vecs[idx] == nil {
			return moerr.NewInternalErrorNoCtx("M0: invalid uncommitted object metadata")
		}
		if entry.bat.Vecs[idx].Length() != 0 {
			return moerr.NewInternalErrorNoCtx("M0: uncommitted data objects must be flushed before ADD COLUMN")
		}
	}
	return nil
}

func m0AppendCNDefs(oldDefs []engine.TableDef, newCol *plan.ColDef) ([]engine.TableDef, error) {
	defs := make([]engine.TableDef, 0, len(oldDefs)+1)
	inserted := false
	for _, def := range oldDefs {
		if attr, ok := def.(*engine.AttributeDef); ok && objectio.IsPhysicalAddr(attr.Attr.Name) {
			defs = append(defs, &engine.AttributeDef{Attr: engine.Attribute{
				Name:          newCol.Name,
				ID:            newCol.ColId,
				Type:          types.MustTypeFromPlan(newCol.Typ),
				Default:       newCol.Default,
				IsHidden:      false,
				Seqnum:        uint16(newCol.Seqnum),
				AutoIncrement: false,
			}})
			inserted = true
			// The old definition remains the rollback owner. Catalog regeneration
			// assigns this same trailing identity to its newly generated rowid.
			rowid := *attr
			rowid.Attr.ID = newCol.ColId + 1
			rowid.Attr.Seqnum = uint16(newCol.Seqnum + 1)
			defs = append(defs, &rowid)
			continue
		}
		defs = append(defs, def)
	}
	if !inserted {
		return nil, moerr.NewInternalErrorNoCtxf("M0: rowid boundary is missing")
	}
	return defs, nil
}
