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
	"strings"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"go.uber.org/zap"
)

func init() {
	activeM0Enabled = true
	activeM0CNAddColumn = buildM0CNAddColumn
	activeM0ReadObject = proveM0ReadObject
	activeM0ReadShards = splitM0ReadShards
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

func proveM0ReadObject(
	ctx context.Context,
	table *plan.TableDef,
	obj objectio.ObjectEntry,
	fs fileservice.FileService,
) (bool, objectio.M0ObjectProof, error) {
	if table == nil {
		return false, objectio.M0ObjectProof{}, nil
	}
	candidate := catalog.M0PlanLooksLikeAppendDomain(table)
	next := uint32(len(table.Cols) - 1)
	layout, err := catalog.M0AppendLayoutFromPlan(table, next)
	if err != nil {
		if candidate {
			return true, objectio.M0ObjectProof{}, err
		}
		return false, objectio.M0ObjectProof{}, nil
	}
	if err = objectio.ValidateM0AppendLayout(layout); err != nil {
		if candidate {
			return true, objectio.M0ObjectProof{}, err
		}
		return false, objectio.M0ObjectProof{}, nil
	}
	location := obj.Location()
	reader, err := objectio.NewObjectReaderWithStr(location.Name().String(), fs)
	if err != nil {
		return true, objectio.M0ObjectProof{}, err
	}
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	header, err := reader.ReadHeader(ctx, mp)
	if err != nil {
		return true, objectio.M0ObjectProof{}, err
	}
	meta, err := reader.ReadAllMeta(ctx, mp)
	if err != nil {
		return true, objectio.M0ObjectProof{}, err
	}
	width, err := objectio.ValidateM0Object(layout, header, meta.MustDataMeta())
	return true, objectio.M0ObjectProof{
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
	if add == nil || add.Column == nil || add.Column.Hidden || add.Column.Primary ||
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
	if table.TableType == pkgcatalog.SystemPartitionRel ||
		features.IsPartitioned(table.FeatureFlag) || features.IsPartition(table.FeatureFlag) || table.Partition != nil ||
		tbl.partitioned > 0 || tbl.partition != "" {
		return nil, moerr.NewInternalErrorNoCtxf("M0: partitioned tables and backing partitions require COPY")
	}
	if len(table.Indexes) != 0 || len(table.Fkeys) != 0 ||
		len(table.RefChildTbls) != 0 || len(table.Checks) != 0 {
		return nil, moerr.NewInternalErrorNoCtxf("M0: secondary indexes, foreign keys and checks are outside the append domain")
	}
	next := uint32(len(table.Cols) - 1)
	if tbl.extraInfo == nil || tbl.extraInfo.NextColSeqnum != next {
		return nil, moerr.NewInternalErrorNoCtxf("M0: stale NextColSeqnum")
	}
	before, err := catalog.M0AppendLayoutFromPlan(table, next)
	if err != nil {
		return nil, err
	}
	if err = objectio.ValidateM0AppendLayout(before); err != nil {
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
	after, err := catalog.M0AppendLayoutFromPlan(afterDef, next+1)
	if err != nil {
		return nil, err
	}
	if err = objectio.ValidateM0AppendTransition(before, after); err != nil {
		return nil, err
	}

	part, err := tbl.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}
	fs := tbl.getTxn().engine.fs
	if !tbl.getTxn().Readonly() {
		if uncommitted, _ := tbl.collectUnCommittedDataObjs(0); len(uncommitted) != 0 {
			return nil, moerr.NewInternalErrorNoCtxf("M0: uncommitted data objects must be flushed before ADD COLUMN")
		}
	}
	if err = ForeachVisibleObjects(
		ctx,
		part,
		types.TimestampToTS(tbl.db.op.SnapshotTS()),
		func(objCtx context.Context, obj objectio.ObjectEntry) error {
			ok, _, proofErr := proveM0ReadObject(objCtx, table, obj, fs)
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
