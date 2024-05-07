package mergesort

import (
	"context"
	"errors"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
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

	originalObjCnt := host.GetObjectCnt()
	totalRowCnt := host.GetTotalRowCnt()
	rowSize := host.GetTotalSize() / totalRowCnt
	maxRowCnt := host.GetBlockMaxRows()

	var writer *blockio.BlockWriter
	var buffer *batch.Batch
	var releaseF func()
	defer func() {
		if releaseF != nil {
			releaseF()
		}
	}()

	blkRowCnt := 0
	objRowCnt := 0
	objBlkCnt := 0
	mergedRowCnt := 0
	objCnt := 0

	for i := 0; i < originalObjCnt; i++ {
		loadedBlkCnt := 0
		nextBatch, del, nextReleaseF, err := host.LoadNextBatch(ctx, uint32(i))
		if buffer == nil {
			buffer, releaseF = getSimilarBatch(nextBatch, int(maxRowCnt), host)
		}
		for err == nil {
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
					host.GetCommitEntry().Booking.Mappings[host.GetAccBlkCnts()[i]+loadedBlkCnt-1].M[int32(j)] = api.TransDestPos{
						ObjIdx: int32(objCnt),
						BlkIdx: int32(uint32(objBlkCnt)),
						RowIdx: int32(blkRowCnt),
					}
				}

				blkRowCnt++
				objRowCnt++
				mergedRowCnt++

				if blkRowCnt == int(maxRowCnt) {
					if writer == nil {
						writer = host.PrepareNewWriter()
					}
					if _, err := writer.WriteBatch(buffer); err != nil {
						return err
					}
					buffer.CleanOnlyData()
					blkRowCnt = 0
					objBlkCnt++
					if needNewObject(objBlkCnt, objRowCnt, totalRowCnt, uint32(mergedRowCnt), rowSize, host.GetTargetObjSize()) {
						objCnt++
						objBlkCnt = 0
						objRowCnt = 0
						if err := syncObject(ctx, writer, host.GetCommitEntry()); err != nil {
							return err
						}
						writer = nil
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
	if blkRowCnt > 0 {
		objBlkCnt++

		if writer == nil {
			writer = host.PrepareNewWriter()
		}
		if _, err := writer.WriteBatch(buffer); err != nil {
			return err
		}
		buffer.CleanOnlyData()
	}
	if objBlkCnt > 0 {
		if err := syncObject(ctx, writer, host.GetCommitEntry()); err != nil {
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

func needNewObject(objBlkCnt, objRowCnt int, totalRowCnt, mergedRowCnt, rowSize, targetObjSize uint32) bool {
	if targetObjSize == 0 {
		return objBlkCnt == int(options.DefaultBlocksPerObject)
	}

	if uint32(objRowCnt)*rowSize > targetObjSize {
		return (totalRowCnt-mergedRowCnt)*rowSize > targetObjSize
	}
	return false
}
