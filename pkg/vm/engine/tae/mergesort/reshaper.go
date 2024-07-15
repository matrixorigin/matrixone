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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
)

func reshape(ctx context.Context, host MergeTaskHost) error {
	if host.DoTransfer() {
		objBlkCnts := host.GetBlkCnts()
		totalBlkCnt := 0
		for _, cnt := range objBlkCnts {
			totalBlkCnt += cnt
		}
		initTransferMapping(host.GetCommitEntry(), totalBlkCnt)
	}
	rowSizeU64 := host.GetTotalSize() / uint64(host.GetTotalRowCnt())
	stats := mergeStats{
		totalRowCnt:   host.GetTotalRowCnt(),
		rowSize:       uint32(rowSizeU64),
		targetObjSize: host.GetTargetObjSize(),
		blkPerObj:     host.GetObjectMaxBlocks(),
	}
	originalObjCnt := host.GetObjectCnt()
	maxRowCnt := host.GetBlockMaxRows()
	accObjBlkCnts := host.GetAccBlkCnts()
	commitEntry := host.GetCommitEntry()

	var writer *blockio.BlockWriter
	var buffer *batch.Batch
	var releaseF func()
	defer func() {
		if releaseF != nil {
			releaseF()
		}
	}()

	for i := 0; i < originalObjCnt; i++ {
		loadedBlkCnt := 0
		nextBatch, del, nextReleaseF, err := host.LoadNextBatch(ctx, uint32(i))
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
					err := buffer.Vecs[i].UnionOne(nextBatch.Vecs[i], int64(j), host.GetMPool())
					if err != nil {
						return err
					}
				}

				if host.DoTransfer() {
					commitEntry.Booking.Mappings[accObjBlkCnts[i]+loadedBlkCnt-1].M[int32(j)] = api.TransDestPos{
						ObjIdx: int32(stats.objCnt),
						BlkIdx: int32(uint32(stats.objBlkCnt)),
						RowIdx: int32(stats.blkRowCnt),
					}
				}

				stats.blkRowCnt++
				stats.objRowCnt++
				stats.mergedRowCnt++

				if stats.blkRowCnt == int(maxRowCnt) {
					if writer == nil {
						writer = host.PrepareNewWriter()
					}

					if _, err := writer.WriteBatch(buffer); err != nil {
						return err
					}

					stats.blkRowCnt = 0
					stats.objBlkCnt++

					buffer.CleanOnlyData()

					if stats.needNewObject() {
						if err := syncObject(ctx, writer, commitEntry); err != nil {
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
			nextBatch, del, nextReleaseF, err = host.LoadNextBatch(ctx, uint32(i))
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
		if err := syncObject(ctx, writer, commitEntry); err != nil {
			return err
		}
		writer = nil
	}

	return nil
}

func syncObject(ctx context.Context, writer *blockio.BlockWriter, commitEntry *api.MergeCommitEntry) error {
	if _, _, err := writer.Sync(ctx); err != nil {
		return err
	}
	cobjstats := writer.GetObjectStats()[:objectio.SchemaTombstone]
	for _, cobj := range cobjstats {
		commitEntry.CreatedObjs = append(commitEntry.CreatedObjs, cobj.Clone().Marshal())
	}
	return nil
}
