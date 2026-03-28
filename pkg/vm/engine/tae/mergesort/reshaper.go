// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mergesort

import (
	"context"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

func reshape(ctx context.Context, host MergeTaskHost) error {
	var table *TransferTable
	var slab []api.TransferDestPos
	var blockActive []bool
	var stride int
	if host.DoTransfer() {
		objBlkCnts := host.GetBlkCnts()
		totalBlkCnt := 0
		for _, cnt := range objBlkCnts {
			totalBlkCnt += cnt
		}
		stride = int(host.GetBlockMaxRows())
		slab = getTransferSlab(totalBlkCnt * stride)
		blockActive = make([]bool, totalBlkCnt)
		table = &TransferTable{Slab: slab, Stride: stride, BlockActive: blockActive}
	}
	stats := mergeStats{
		targetObjSize: host.GetTargetObjSize(),
		blkPerObj:     host.GetObjectMaxBlocks(),
		totalSize:     host.GetTotalSize(),
	}
	originalObjCnt := host.GetObjectCnt()
	maxRowCnt := host.GetBlockMaxRows()
	accObjBlkCnts := host.GetAccBlkCnts()
	mp := host.GetMPool()

	var writer *ioutil.BlockWriter
	var buffer *batch.Batch
	var releaseF func()
	defer func() {
		if releaseF != nil {
			releaseF()
		}
	}()

	var nextBatch *batch.Batch
	for i := 0; i < originalObjCnt; i++ {
		loadedBlkCnt := 0
		nextBatch, del, nextReleaseF, err := host.LoadNextBatch(ctx, uint32(i), nextBatch)
		for err == nil {
			if buffer == nil {
				buffer, releaseF = getSimilarBatch(nextBatch, int(maxRowCnt), host)
			}
			loadedBlkCnt++
			for j := 0; j < nextBatch.RowCount(); j++ {
				if del.Contains(uint64(j)) {
					continue
				}

				for i := range buffer.Vecs {
					err := buffer.Vecs[i].UnionOne(nextBatch.Vecs[i], int64(j), mp)
					if err != nil {
						return err
					}
				}

				if host.DoTransfer() {
					idx := accObjBlkCnts[i] + loadedBlkCnt - 1
					blockActive[idx] = true
					slab[idx*stride+int(j)] = api.TransferDestPos{
						ObjIdx: uint8(stats.objCnt),
						BlkIdx: uint16(stats.objBlkCnt),
						RowIdx: uint32(stats.blkRowCnt),
					}
				}

				stats.blkRowCnt++
				stats.objRowCnt++

				if stats.blkRowCnt == int(maxRowCnt) {
					if writer == nil {
						writer = host.PrepareNewWriter()
					}

					if _, err := writer.WriteBatch(buffer); err != nil {
						return err
					}
					stats.writtenBytes = writer.GetWrittenOriginalSize()
					stats.mergedSize += uint64(stats.writtenBytes)
					stats.blkRowCnt = 0
					stats.objBlkCnt++

					buffer.CleanOnlyData()

					if stats.needNewObject() {
						if err := syncObject(ctx, writer, host); err != nil {
							return err
						}
						writer = nil

						stats.objRowCnt = 0
						stats.objBlkCnt = 0
						stats.objCnt++
					}
				}
			}

			nextReleaseF()
			nextBatch, del, nextReleaseF, err = host.LoadNextBatch(ctx, uint32(i), nextBatch)
		}
		if !errors.Is(err, ErrNoMoreBlocks) {
			return err
		}
	}
	// write remain data
	if stats.blkRowCnt > 0 {
		stats.objBlkCnt++

		if writer == nil {
			writer = host.PrepareNewWriter()
		}
		if _, err := writer.WriteBatch(buffer); err != nil {
			return err
		}
		buffer.CleanOnlyData()
	}
	if stats.objBlkCnt > 0 {
		if err := syncObject(ctx, writer, host); err != nil {
			return err
		}
		writer = nil
	}

	if table != nil {
		host.SetTransferTable(table)
	}

	return nil
}

func syncObject(ctx context.Context, writer *ioutil.BlockWriter, host MergeTaskHost) error {
	if host.HasBigDelEvent() {
		return moerr.NewInternalErrorNoCtxf("LockMerge give up in syncObject %v", host.Name())
	}
	commitEntry := host.GetCommitEntry()
	if _, _, err := writer.Sync(ctx); err != nil {
		return err
	}
	cobjstats := writer.GetObjectStats()
	commitEntry.CreatedObjs = append(commitEntry.CreatedObjs, cobjstats.Clone().Marshal())
	return nil
}
