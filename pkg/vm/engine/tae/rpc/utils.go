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

	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"

	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
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

func (h *Handle) prefetchDeleteRowID(
	_ context.Context,
	req *cmd_util.WriteReq,
	txnMeta *txn.TxnMeta,
) error {
	if len(req.TombstoneStats) == 0 {
		return nil
	}

	//start loading jobs asynchronously,should create a new root context.
	for _, stats := range req.TombstoneStats {
		location := stats.BlockLocation(uint16(0), objectio.BlockMaxRows)
		err := ioutil.Prefetch(h.db.Opts.SID, h.db.Runtime.Fs.Service, location)
		if err != nil {
			return err
		}
	}

	logCNCommittedObjects(txnMeta, true, req.TableID, req.TableName, req.TombstoneStats)

	return nil
}

func (h *Handle) prefetchMetadata(
	_ context.Context,
	req *cmd_util.WriteReq,
	txnMeta *txn.TxnMeta,
) error {
	if len(req.DataObjectStats) == 0 {
		return nil
	}

	//start loading jobs asynchronously,should create a new root context.
	for _, stats := range req.DataObjectStats {
		location := stats.BlockLocation(uint16(0), objectio.BlockMaxRows)
		err := ioutil.PrefetchMeta(h.db.Opts.SID, h.db.Runtime.Fs.Service, location)
		if err != nil {
			return err
		}
	}

	logCNCommittedObjects(txnMeta, false, req.TableID, req.TableName, req.DataObjectStats)

	return nil
}

func logCNCommittedObjects(
	txnMeta *txn.TxnMeta,
	isTombstone bool,
	tableId uint64,
	tableName string,
	statsList []objectio.ObjectStats,
) {

	totalBlkCnt := 0
	totalRowCnt := 0
	totalOSize := 0
	totalCSize := 0
	maxPrintObjCnt := 100
	if len(statsList) < maxPrintObjCnt {
		maxPrintObjCnt = len(statsList)
	}
	var objNames = make([]string, 0, maxPrintObjCnt)
	for _, stats := range statsList {
		totalBlkCnt += int(stats.BlkCnt())
		totalRowCnt += int(stats.Rows())
		totalCSize += int(stats.Size())
		totalOSize += int(stats.OriginSize())
		if len(objNames) >= maxPrintObjCnt {
			continue
		}
		objNames = append(objNames, stats.ObjectName().ObjectId().ShortStringEx())
	}

	hint := "CN-COMMIT-S3-Data"
	if isTombstone {
		hint = "CN-COMMIT-S3-Tombstone"
	}

	logutil.Info(
		hint,
		zap.String("txn-id", txnMeta.DebugString()),
		zap.Int("table-id", int(tableId)),
		zap.String("table-name", tableName),
		zap.Int("obj-cnt", len(statsList)),
		zap.String("obj-osize", common.HumanReadableBytes(totalOSize)),
		zap.String("obj-size", common.HumanReadableBytes(totalCSize)),
		zap.Int("blk-cnt", totalBlkCnt),
		zap.Int("row-cnt", totalRowCnt),
		zap.Strings("names", objNames),
	)
}

// TryPrefetchTxn only prefetch data written by CN, do not change the state machine of TxnEngine.
func (h *Handle) TryPrefetchTxn(ctx context.Context, meta *txn.TxnMeta) error {
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
		if r, ok := e.(*cmd_util.WriteReq); ok && r.FileName != "" {
			if r.Type == cmd_util.EntryDelete {
				// start to load deleted row ids
				deltaLocCnt += len(r.TombstoneStats)
				if err := h.prefetchDeleteRowID(ctx, r, meta); err != nil {
					return err
				}
			} else if r.Type == cmd_util.EntryInsert {
				metaLocCnt += len(r.DataObjectStats)
				if err := h.prefetchMetadata(ctx, r, meta); err != nil {
					return err
				}
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
				AccId: uint64(accId), DbId: entry.ID, TblId: tblIt.Get().GetPayload().ID,
			}

			tblEntry := tblIt.Get().GetPayload()
			if tblEntry.HasDropCommitted() {
				tblIt.Next()
				continue
			}

			objIt := tblEntry.MakeTombstoneVisibleObjectIt(txnbase.MockTxnReaderWithNow())
			for objIt.Next() {
				objEntry := objIt.Item()
				// PXU TODO
				if !objEntry.IsAppendable() && !objEntry.HasDropCommitted() && objEntry.IsCommitted() {
					insUsage.Size += uint64(objEntry.Size())
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
