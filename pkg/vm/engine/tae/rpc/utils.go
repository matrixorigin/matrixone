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

	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

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
	return blockio.PrefetchWithMerged(h.db.Opts.SID, pref)
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
			err := blockio.PrefetchMeta(h.db.Opts.SID, h.db.Runtime.Fs.Service, loc)
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

// TryPrefetchTxn only prefetch data written by CN, do not change the state machine of TxnEngine.
func (h *Handle) TryPrefetchTxn(ctx context.Context, meta txn.TxnMeta) error {
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

			objIt := tblEntry.MakeTombstoneObjectIt()
			for objIt.Next() {
				objEntry := objIt.Item()
				// PXU TODO
				if !objEntry.IsAppendable() && !objEntry.HasDropCommitted() && objEntry.IsCommitted() {
					insUsage.Size += uint64(objEntry.GetCompSize())
				}
			}
			objIt.Release()

			if insUsage.Size > 0 {
				memo.UpdateNewAccCache(insUsage, false)
			}

			tblIt.Next()
		}
		return nil
	}

	c.RecurLoop(processor)
}
