// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	commonUtil "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

// dumpLifecycleTransferInfo writes only immutable, Root-owned external
// booking pages. The durable Root remains their cleanup owner on both success
// and failure; the retired ordinary CN merge booking path is not restored.
func dumpLifecycleTransferInfo(
	ctx context.Context,
	taskHost *lifecycleRewriteTask,
	pathAllocator func(pageOrdinal uint32) (string, error),
) (_ *api.MergeCommitEntry, err error) {
	tt := taskHost.transferTable

	nblks := tt.Len()
	blkCnt := int32(nblks)
	totalRows := 0

	// BookingLoc layout:
	// | blockCnt | Blk1RowCnt | Blk2RowCnt | ... | filepath1 | filepath2 | ... |
	taskHost.commitEntry.BookingLoc = append(taskHost.commitEntry.BookingLoc,
		commonUtil.UnsafeBytesToString(types.EncodeInt32(&blkCnt)))
	for i := 0; i < nblks; i++ {
		m := tt.GetBlockMap(i)
		rowCnt := int32(len(m))
		taskHost.commitEntry.BookingLoc = append(taskHost.commitEntry.BookingLoc,
			commonUtil.UnsafeBytesToString(types.EncodeInt32(&rowCnt)))
		totalRows += len(m)
	}

	columns := []string{"src_blk", "src_row", "dest_obj", "dest_blk", "dest_row"}
	colTypes := []types.T{types.T_int32, types.T_uint32, types.T_uint8, types.T_uint16, types.T_uint32}
	batchSize := min(200*mpool.MB/len(columns)/int(unsafe.Sizeof(int32(0))), totalRows)
	buffer := batch.New(columns)
	releases := make([]func(), len(columns))
	for i := range columns {
		t := colTypes[i].ToType()
		vec, release := taskHost.GetVector(&t)
		err := vec.PreExtend(batchSize, taskHost.GetMPool())
		if err != nil {
			return nil, err
		}
		buffer.Vecs[i] = vec
		releases[i] = release
	}
	defer func() {
		for _, rel := range releases {
			if rel != nil {
				rel()
			}
		}
	}()
	objRowCnt := 0
	var bookingOrdinal uint32
	allocatePath := func() (string, error) {
		path, err := pathAllocator(bookingOrdinal)
		if err != nil {
			return "", err
		}
		bookingOrdinal++
		return path, nil
	}
	for blkIdx := 0; blkIdx < nblks; blkIdx++ {
		transMap := tt.GetBlockMap(blkIdx)
		for rowIdx, destPos := range transMap {
			if destPos.ObjIdx == api.NoTransfer {
				continue
			}
			if err = vector.AppendFixed(buffer.Vecs[0], int32(blkIdx), false, taskHost.GetMPool()); err != nil {
				return nil, err
			}
			if err = vector.AppendFixed(buffer.Vecs[1], uint32(rowIdx), false, taskHost.GetMPool()); err != nil {
				return nil, err
			}
			if err = vector.AppendFixed(buffer.Vecs[2], destPos.ObjIdx, false, taskHost.GetMPool()); err != nil {
				return nil, err
			}
			if err = vector.AppendFixed(buffer.Vecs[3], destPos.BlkIdx, false, taskHost.GetMPool()); err != nil {
				return nil, err
			}
			if err = vector.AppendFixed(buffer.Vecs[4], destPos.RowIdx, false, taskHost.GetMPool()); err != nil {
				return nil, err
			}

			buffer.SetRowCount(buffer.RowCount() + 1)
			objRowCnt++

			if objRowCnt*len(columns)*int(unsafe.Sizeof(int32(0))) > 200*mpool.MB {
				filename, err := allocatePath()
				if err != nil {
					return nil, err
				}
				writer, err := objectio.NewObjectWriterSpecial(objectio.WriterTmp, filename, taskHost.fs)
				if err != nil {
					return nil, err
				}

				_, err = writer.Write(buffer)
				if err != nil {
					return nil, err
				}
				buffer.CleanOnlyData()

				_, err = writer.WriteEnd(ctx)
				if err != nil {
					return nil, err
				}
				taskHost.commitEntry.BookingLoc = append(taskHost.commitEntry.BookingLoc, filename)
				objRowCnt = 0
			}
		}
	}

	// write remaining data
	if buffer.RowCount() != 0 {
		filename, err := allocatePath()
		if err != nil {
			return nil, err
		}
		writer, err := objectio.NewObjectWriterSpecial(objectio.WriterTmp, filename, taskHost.fs)
		if err != nil {
			return nil, err
		}

		_, err = writer.Write(buffer)
		if err != nil {
			return nil, err
		}
		buffer.CleanOnlyData()

		_, err = writer.WriteEnd(ctx)
		if err != nil {
			return nil, err
		}
		taskHost.commitEntry.BookingLoc = append(taskHost.commitEntry.BookingLoc, filename)
	}

	taskHost.commitEntry.Booking = nil
	return taskHost.commitEntry, nil
}
