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

package frontend

import (
	"context"
	"errors"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

func init() {
	validateM0TableDumpObjects = checkM0TableDumpObjects
}

// A dump's logical schema hash does not describe the order of hidden physical
// columns. Check the target's existing M0 object contract before installing
// any relation objects. This check does not prove an object's DDL provenance.
func checkM0TableDumpObjects(ctx context.Context, rel engine.Relation, objects []tableDumpObject, fs fileservice.FileService, mp *mpool.MPool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if rel == nil || fs == nil || mp == nil {
		return moerr.NewInternalErrorNoCtx("M0: table dump validation lacks target resources")
	}
	// A serialized physical dump has no trusted relation-chain membership.
	// Matching geometry cannot authorize installing its objects in a contract.
	if extra := rel.GetExtraInfo(); extra != nil && extra.M0Evolution != nil {
		return moerr.NewInvalidInputNoCtx("M0: physical table LOAD cannot join an evolution contract; use logical rewrite")
	}
	def := rel.GetTableDef(ctx)
	appendDomain := catalog.M0PlanLooksLikeAppendDomain(def)
	if !appendDomain && (def == nil || def.Pkey == nil || def.Pkey.CompPkeyCol == nil ||
		!def.Pkey.CompPkeyCol.Hidden || !def.Pkey.CompPkeyCol.Primary ||
		def.Pkey.PkeyColName != def.Pkey.CompPkeyCol.Name) {
		return nil
	}
	layout, err := catalog.M0AppendLayoutFromPlan(def, uint32(len(def.Cols)-1))
	if err != nil && !appendDomain {
		// Outside the append domain, leave unrecognized ordinary definitions to
		// their existing compatibility path. Do not turn shape into authority.
		return nil
	}
	if err == nil && appendDomain {
		err = m0read.ValidateM0AppendLayout(layout)
	}
	if err != nil {
		return errors.Join(moerr.NewInvalidInputNoCtx("M0: table dump target has an invalid append layout"), err)
	}
	for _, item := range objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if item.Tombstone {
			continue
		}
		reader, err := objectio.NewObjectReaderWithStr(item.Name, fs)
		if err != nil {
			return err
		}
		header, err := reader.ReadHeader(ctx, mp)
		if err != nil {
			return err
		}
		if _, err = objectio.ObjectSchemaVersion(header); err != nil {
			return errors.Join(moerr.NewInvalidInputNoCtx("M0: table dump object has an invalid header"), err)
		}
		extent := header.Extent()
		reader.CacheMetaExtent(&extent)
		meta, err := reader.ReadAllMeta(ctx, mp)
		if err != nil {
			return err
		}
		dataMeta, ok := meta.DataMeta()
		if !ok {
			return moerr.NewInvalidInputNoCtx("M0: table dump data object has no data metadata")
		}
		if dataMeta.IsEmpty() || dataMeta.BlockCount() == 0 {
			return moerr.NewInvalidInputNoCtx("M0: table dump object is incompatible with target layout")
		}
		key := def.Pkey.CompPkeyCol
		if key.Seqnum >= objectio.SEQNUM_UPPER {
			return moerr.NewInvalidInputNoCtx("M0: table dump target has an invalid composite key")
		}
		if appendDomain {
			if _, err = m0read.ValidateM0Object(layout, header, dataMeta); err != nil {
				return errors.Join(moerr.NewInvalidInputNoCtx("M0: table dump object is incompatible with target layout"), err)
			}
			continue
		}
		// A recorded sort-key seqnum is physical evidence of which column was
		// used as the source key. An absent marker cannot prove compatibility.
		physicalKey := dataMeta.BlockHeader().SortKey()
		if physicalKey != math.MaxUint16 && physicalKey != uint16(key.Seqnum) {
			return moerr.NewInvalidInputNoCtx("M0: table dump object is incompatible with target layout")
		}
		// A non-append DDL can advance the target version without changing its
		// physical columns. Do not infer the meaning of absent columns from that
		// version. A physically present column with a different OID, however,
		// cannot be read as the target column at the same seqnum.
		for blockIdx := uint32(0); blockIdx < dataMeta.BlockCount(); blockIdx++ {
			block := dataMeta.GetBlockMeta(blockIdx)
			keySeq := uint16(key.Seqnum)
			if keySeq > block.GetMaxSeqnum() || keySeq >= block.GetMetaColumnCount() ||
				block.ColumnMeta(keySeq).DataType() != uint8(key.Typ.Id) {
				return moerr.NewInvalidInputNoCtx("M0: table dump object is incompatible with target layout")
			}
			for _, col := range layout.Columns {
				if col.Seqnum > block.GetMaxSeqnum() || col.Seqnum >= block.GetMetaColumnCount() {
					continue
				}
				physical := block.ColumnMeta(col.Seqnum).DataType()
				if physical != 0 && physical != uint8(col.Type) {
					return moerr.NewInvalidInputNoCtx("M0: table dump object is incompatible with target layout")
				}
			}
		}
	}
	return ctx.Err()
}
