// Copyright 2022 Matrix Origin
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

package rpc

import (
	"context"
	"strings"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

func DefsToSchema(name string, defs []engine.TableDef) (schema *catalog.Schema, err error) {
	schema = catalog.NewEmptySchema(name)
	schema.CatalogVersion = pkgcatalog.CatalogVersion_Curr
	var pkeyColName string
	for _, def := range defs {
		switch defVal := def.(type) {
		case *engine.ConstraintDef:
			primaryKeyDef := defVal.GetPrimaryKeyDef()
			if primaryKeyDef != nil {
				pkeyColName = primaryKeyDef.Pkey.PkeyColName
				break
			}
		}
	}
	for _, def := range defs {
		switch defVal := def.(type) {
		case *engine.AttributeDef:
			if pkeyColName == defVal.Attr.Name {
				if err = schema.AppendSortColWithAttribute(defVal.Attr, 0, true); err != nil {
					return
				}
			} else if defVal.Attr.ClusterBy {
				if err = schema.AppendSortColWithAttribute(defVal.Attr, 0, false); err != nil {
					return
				}
			} else {
				if err = schema.AppendColWithAttribute(defVal.Attr); err != nil {
					return
				}
			}

		case *engine.PropertiesDef:
			for _, property := range defVal.Properties {
				switch strings.ToLower(property.Key) {
				case pkgcatalog.SystemRelAttr_Comment:
					schema.Comment = property.Value
				case pkgcatalog.SystemRelAttr_Kind:
					schema.Relkind = property.Value
				case pkgcatalog.SystemRelAttr_CreateSQL:
					schema.Createsql = property.Value
				default:
				}
			}

		case *engine.PartitionDef:
			schema.Partitioned = defVal.Partitioned
			schema.Partition = defVal.Partition
		case *engine.ViewDef:
			schema.View = defVal.View
		case *engine.CommentDef:
			schema.Comment = defVal.Comment
		case *engine.ConstraintDef:
			schema.Constraint, err = defVal.MarshalBinary()
			if err != nil {
				return nil, err
			}
		default:
			// We will not deal with other cases for the time being
		}
	}
	if err = schema.Finalize(false); err != nil {
		return
	}
	return
}

func SchemaToDefs(schema *catalog.Schema) (defs []engine.TableDef, err error) {
	if schema.Comment != "" {
		commentDef := new(engine.CommentDef)
		commentDef.Comment = schema.Comment
		defs = append(defs, commentDef)
	}

	if schema.Partitioned > 0 || schema.Partition != "" {
		partitionDef := new(engine.PartitionDef)
		partitionDef.Partitioned = schema.Partitioned
		partitionDef.Partition = schema.Partition
		defs = append(defs, partitionDef)
	}

	if schema.View != "" {
		viewDef := new(engine.ViewDef)
		viewDef.View = schema.View
		defs = append(defs, viewDef)
	}

	if len(schema.Constraint) > 0 {
		c := new(engine.ConstraintDef)
		if err := c.UnmarshalBinary(schema.Constraint); err != nil {
			return nil, err
		}
		defs = append(defs, c)
	}

	for _, col := range schema.ColDefs {
		if col.IsPhyAddr() {
			continue
		}
		attr, err := AttrFromColDef(col)
		if err != nil {
			return nil, err
		}
		defs = append(defs, &engine.AttributeDef{Attr: *attr})
	}
	pro := new(engine.PropertiesDef)
	pro.Properties = append(pro.Properties, engine.Property{
		Key:   pkgcatalog.SystemRelAttr_Kind,
		Value: string(schema.Relkind),
	})
	if schema.Createsql != "" {
		pro.Properties = append(pro.Properties, engine.Property{
			Key:   pkgcatalog.SystemRelAttr_CreateSQL,
			Value: schema.Createsql,
		})
	}
	defs = append(defs, pro)

	return
}

func AttrFromColDef(col *catalog.ColDef) (attrs *engine.Attribute, err error) {
	var defaultVal *plan.Default
	if len(col.Default) > 0 {
		defaultVal = &plan.Default{}
		if err := types.Decode(col.Default, defaultVal); err != nil {
			return nil, err
		}
	}

	var onUpdate *plan.OnUpdate
	if len(col.OnUpdate) > 0 {
		onUpdate = new(plan.OnUpdate)
		if err := types.Decode(col.OnUpdate, onUpdate); err != nil {
			return nil, err
		}
	}

	attr := &engine.Attribute{
		Name:          col.Name,
		Type:          col.Type,
		Primary:       col.IsPrimary(),
		IsHidden:      col.IsHidden(),
		IsRowId:       col.IsPhyAddr(),
		Comment:       col.Comment,
		Default:       defaultVal,
		OnUpdate:      onUpdate,
		AutoIncrement: col.IsAutoIncrement(),
		ClusterBy:     col.IsClusterBy(),
	}
	return attr, nil
}

type mItem struct {
	objcnt   int
	did, tid uint64
}

type itemSet []mItem

func (is itemSet) Len() int { return len(is) }

func (is itemSet) Less(i, j int) bool {
	return is[i].objcnt < is[j].objcnt
}

func (is itemSet) Swap(i, j int) {
	is[i], is[j] = is[j], is[i]
}

func (is *itemSet) Push(x any) {
	item := x.(mItem)
	*is = append(*is, item)
}

func (is *itemSet) Pop() any {
	old := *is
	n := len(old)
	item := old[n-1]
	// old[n-1] = nil // avoid memory leak
	*is = old[0 : n-1]
	return item
}

func (is *itemSet) Clear() {
	old := *is
	*is = old[:0]
}

func getBlkIDsFromRowids(vec *vector.Vector) map[types.Blockid]struct{} {
	rowids := vector.MustFixedCol[types.Rowid](vec)
	blkids := make(map[types.Blockid]struct{})
	for _, rowid := range rowids {
		blkID := *rowid.BorrowBlockID()
		blkids[blkID] = struct{}{}
	}
	return blkids
}

func (h *Handle) prefetchDeleteRowID(_ context.Context, req *db.WriteReq) error {
	if len(req.DeltaLocs) == 0 {
		return nil
	}
	//for loading deleted rowid.
	columnIdx := 0
	pkIdx := 1
	//start loading jobs asynchronously,should create a new root context.
	loc, err := blockio.EncodeLocationFromString(req.DeltaLocs[0])
	if err != nil {
		return err
	}
	pref, err := blockio.BuildPrefetchParams(h.db.Runtime.Fs.Service, loc)
	if err != nil {
		return err
	}
	for _, key := range req.DeltaLocs {
		var location objectio.Location
		location, err = blockio.EncodeLocationFromString(key)
		if err != nil {
			return err
		}
		pref.AddBlockWithType([]uint16{uint16(columnIdx), uint16(pkIdx)}, []uint16{location.ID()}, uint16(objectio.SchemaTombstone))
	}
	return blockio.PrefetchWithMerged(pref)
}

func (h *Handle) prefetchMetadata(_ context.Context, req *db.WriteReq) (int, error) {
	if len(req.MetaLocs) == 0 {
		return 0, nil
	}
	//start loading jobs asynchronously,should create a new root context.
	objCnt := 0
	var objectName objectio.ObjectNameShort
	for _, meta := range req.MetaLocs {
		loc, err := blockio.EncodeLocationFromString(meta)
		if err != nil {
			return 0, err
		}
		if !objectio.IsSameObjectLocVsShort(loc, &objectName) {
			err := blockio.PrefetchMeta(h.db.Runtime.Fs.Service, loc)
			if err != nil {
				return 0, err
			}
			objCnt++
			objectName = *loc.Name().Short()
		}
	}
	logutil.Info(
		"CN-COMMIT-S3",
		zap.Int("table-id", int(req.TableID)),
		zap.String("table-name", req.TableName),
		zap.Int("obj-cnt", objCnt),
	)
	return objCnt, nil
}

// TryPrefechTxn only prefecth data written by CN, do not change the state machine of TxnEngine.
func (h *Handle) TryPrefechTxn(ctx context.Context, meta txn.TxnMeta) error {
	txnCtx, _ := h.txnCtxs.Load(util.UnsafeBytesToString(meta.GetID()))

	metaLocCnt := 0
	deltaLocCnt := 0

	defer func() {
		if metaLocCnt != 0 {
			v2.TxnCNCommittedMetaLocationQuantityGauge.Set(float64(metaLocCnt))
		}

		if deltaLocCnt != 0 {
			v2.TxnCNCommittedDeltaLocationQuantityGauge.Set(float64(deltaLocCnt))
		}
	}()

	for _, e := range txnCtx.reqs {
		if r, ok := e.(*db.WriteReq); ok && r.FileName != "" {
			if r.Type == db.EntryDelete {
				// start to load deleted row ids
				deltaLocCnt += len(r.DeltaLocs)
				if err := h.prefetchDeleteRowID(ctx, r); err != nil {
					return err
				}
			} else if r.Type == db.EntryInsert {
				objCnt, err := h.prefetchMetadata(ctx, r)
				if err != nil {
					return err
				}
				metaLocCnt += objCnt
			}
		}
	}
	return nil
}

func traverseCatalogForNewAccounts(c *catalog.Catalog, memo *logtail.TNUsageMemo, ids []uint64) {
	if len(ids) == 0 {
		return
	}
	processor := new(catalog.LoopProcessor)
	processor.DatabaseFn = func(entry *catalog.DBEntry) error {
		if entry.HasDropCommitted() {
			return nil
		}

		accId := entry.GetTenantID()
		if !slices.Contains(ids, uint64(accId)) {
			return nil
		}

		tblIt := entry.MakeTableIt(true)
		for tblIt.Valid() {
			insUsage := logtail.UsageData{
				AccId: uint64(accId), DbId: entry.ID, TblId: tblIt.Get().GetPayload().ID}

			tblEntry := tblIt.Get().GetPayload()
			if tblEntry.HasDropCommitted() {
				tblIt.Next()
				continue
			}

			objIt := tblEntry.MakeObjectIt(true)
			for objIt.Valid() {
				objEntry := objIt.Get().GetPayload()
				// PXU TODO
				if !objEntry.IsAppendable() && !objEntry.HasDropCommitted() && objEntry.IsCommitted() {
					insUsage.Size += uint64(objEntry.GetCompSize())
				}
				objIt.Next()
			}

			if insUsage.Size > 0 {
				memo.UpdateNewAccCache(insUsage, false)
			}

			tblIt.Next()
		}
		return nil
	}

	c.RecurLoop(processor)
}
