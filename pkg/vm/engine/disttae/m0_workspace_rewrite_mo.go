//go:build mo_m0

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

package disttae

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	taecatalog "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

func init() {
	activeM0PrepareWorkspacePhysicalRewrite = prepareM0WorkspacePhysicalRewrite
	activeM0WorkspaceRewriteAfterSync = m0WorkspaceRewriteAfterSync
}

func m0WorkspaceRewriteAfterSync(ctx context.Context, names []string) error {
	_, message, injected := fault.TriggerFaultWithContext(ctx, "fj/m0/rewrite/after-sync")
	if !injected {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(err, moerr.NewInternalErrorNoCtxf("M0 rewrite fault objects=%s", strings.Join(names, ",")))
	}
	return moerr.NewInternalErrorNoCtxf("M0 rewrite fault: %s; objects=%s", message, strings.Join(names, ","))
}

// prepareM0WorkspacePhysicalRewrite only preserves a previously selected
// object's physical columns and version. It does not classify missing columns
// or authorize NULL synthesis. Unsupported shapes retain the fail-closed
// ordinary rewrite path.
func prepareM0WorkspacePhysicalRewrite(
	ctx context.Context, tbl *txnTable, obj objectio.ObjectStats, fs fileservice.FileService,
) (*workspacePhysicalRewrite, error) {
	if tbl == nil || tbl.getTxn() == nil || !tbl.getTxn().isCloneTxn || fs == nil {
		return nil, nil
	}
	def := tbl.tableDef
	if err := activeM0CheckReadEvolution(ctx, tbl); err != nil {
		return nil, err
	}
	contract, err := m0read.PlanEvolution(def)
	if err != nil {
		return nil, err
	}
	if _, ok := m0AppendBaseFromPlan(def); !ok {
		if contract != nil {
			return nil, moerr.NewInternalErrorNoCtx("M0: contracted rewrite schema is outside its domain")
		}
		return nil, nil
	}
	if obj.GetAppendable() {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reader, err := objectio.NewObjectReaderWithStr(obj.ObjectName().String(), fs)
	if err != nil {
		return nil, err
	}
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	header, err := reader.ReadHeader(ctx, mp)
	if err != nil {
		return nil, err
	}
	version, err := objectio.ObjectSchemaVersion(header)
	if err != nil {
		return nil, err
	}
	meta, err := reader.ReadAllMeta(ctx, mp)
	if err != nil {
		return nil, err
	}
	data, ok := meta.DataMeta()
	if !ok || data.IsEmpty() || data.BlockCount() == 0 || data.BlockHeader().TableID() != 0 {
		return nil, nil
	}
	if obj.BlkCnt() != data.BlockCount() || obj.BlkCnt() > math.MaxUint16 ||
		!bytes.Equal(obj.Extent(), header.Extent()) {
		return nil, moerr.NewInternalErrorNoCtx("M0: workspace object stats do not match physical metadata")
	}
	maxVersion := def.Version
	if contract != nil {
		if _, err = m0read.ValidatePlanEvolution(def, tbl.accountId); err != nil {
			return nil, err
		}
		owner := m0read.EvolutionBinding{Account: tbl.accountId, Database: tbl.db.databaseId, Relation: tbl.tableId}
		if err = m0read.ValidateEvolution(contract, owner); err != nil {
			return nil, err
		}
		layout, err := taecatalog.M0AppendLayoutFromPlan(def, uint32(len(def.Cols)-1))
		if err != nil {
			return nil, err
		}
		if _, err = m0read.ValidateM0Object(layout, header, data); err != nil {
			return nil, err
		}
		maxVersion = contract.LayoutGeneration
	}
	width := uint32(data.BlockHeader().MaxSeqnum()) + 1
	logicalWidth := uint32(len(def.Cols) - 1)
	pk := def.Pkey.CompPkeyCol.Seqnum
	if width >= logicalWidth || pk >= width || version > maxVersion ||
		data.BlockHeader().SortKey() != uint16(pk) {
		return nil, nil
	}
	rows := make([]uint32, 0, data.BlockCount())
	var totalRows uint64
	for blockIdx := uint32(0); blockIdx < data.BlockCount(); blockIdx++ {
		block := data.GetBlockMeta(blockIdx)
		rows = append(rows, block.GetRows())
		totalRows += uint64(block.GetRows())
		if uint32(block.GetMaxSeqnum())+1 != width || uint32(block.GetMetaColumnCount()) < width {
			return nil, nil
		}
		for seq := uint32(0); seq < width; seq++ {
			col := def.Cols[seq]
			if col == nil || col.Seqnum != seq || block.ColumnMeta(uint16(seq)).DataType() != uint8(col.Typ.Id) {
				return nil, nil
			}
		}
	}
	if totalRows != uint64(obj.Rows()) {
		return nil, moerr.NewInternalErrorNoCtx("M0: workspace object row count does not match physical metadata")
	}
	physical := proto.Clone(def).(*plan.TableDef)
	physical.Cols = append(physical.Cols[:width:width], physical.Cols[len(physical.Cols)-1])
	physical.Version = version
	if contract != nil {
		// Keep the metadata snapshot version distinct from the pinned output
		// generation. The contract, never this field, selects the M0 header.
		physical.Version = def.Version
		// A private writer binding for a recorded older generation. This is
		// derived only after validating the source against current authority;
		// it is never published as the relation's catalog contract.
		pinned := api.CloneExtra(&api.SchemaExtra{M0Evolution: contract}).M0Evolution
		widthForVersion, err := m0read.EvolutionWidth(pinned, m0read.EvolutionBinding{Account: tbl.accountId, Database: tbl.db.databaseId, Relation: tbl.tableId}, version)
		if err != nil {
			return nil, err
		}
		if widthForVersion != width {
			return nil, moerr.NewInternalErrorNoCtx("M0: physical rewrite is not a recorded layout")
		}
		pinned.LayoutGeneration = version
		pinned.Columns = pinned.Columns[:width:width]
		pinned.SchemaDigest = m0read.EvolutionDigest(pinned.Columns)
		for _, d := range physical.Defs {
			for _, p := range d.GetProperties().GetProperties() {
				if p.Key == "schema_extra" {
					var extra api.SchemaExtra
					if err = extra.Unmarshal([]byte(p.Value)); err != nil {
						return nil, err
					}
					extra.M0Evolution = pinned
					p.Value = string(api.MustMarshalTblExtra(&extra))
				}
			}
		}
		if _, err = m0read.ValidatePlanEvolution(physical, tbl.accountId); err != nil {
			return nil, err
		}
	}
	result := &workspacePhysicalRewrite{tableDef: physical, rows: rows}
	for _, col := range physical.Cols[:width] {
		result.seqnums = append(result.seqnums, uint16(col.Seqnum))
		result.typs = append(result.typs, vector.ProtoTypeToType(col.Typ))
	}
	// The retained rowid identity remains outside the pinned physical prefix.
	// The writer uses its explicit pinned contract; no width-derived authority.
	if physical.Cols[width].Seqnum <= width || len(result.typs) != int(width) {
		return nil, nil
	}
	return result, nil
}
