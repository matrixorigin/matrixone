// Copyright 2021 - 2022 Matrix Origin
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

package rpc

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

// loadLifecycleTransferMaps decodes a validated external Lifecycle booking.
// Physical files remain Root-owned and must survive both commit and abort.
func loadLifecycleTransferMaps(
	ctx context.Context,
	req *api.MergeCommitEntry,
	fs fileservice.FileService,
) (api.TransferMaps, error) {
	if len(req.BookingLoc) > 0 {
		// load transfer info from s3
		if req.Booking != nil {
			logutil.Error("mergeblocks err booking loc is not empty, but booking is not nil")
		}

		blkCnt := types.DecodeInt32(util.UnsafeStringToBytes(req.BookingLoc[0]))
		booking := make(api.TransferMaps, blkCnt)
		for i := range blkCnt {
			rowCnt := types.DecodeInt32(util.UnsafeStringToBytes(req.BookingLoc[i+1]))
			if rowCnt == 0 {
				// fully-deleted block: leave booking[i] == nil so downstream
				// mapping == nil checks correctly identify it as all-deleted.
				continue
			}
			tm := make(api.TransferMap, rowCnt)
			for j := range tm {
				tm[j].ObjIdx = api.NoTransfer
			}
			booking[i] = tm
		}
		req.BookingLoc = req.BookingLoc[blkCnt+1:]
		locations := req.BookingLoc
		for _, filepath := range locations {
			reader, err := ioutil.NewFileReader(fs, filepath)
			if err != nil {
				return nil, err
			}
			bats, releases, err := reader.LoadAllColumns(ctx, nil, nil)
			if err != nil {
				return nil, err
			}

			for _, bat := range bats {
				if err := validateTransferMapSourceBounds(
					ctx,
					bat,
					booking,
				); err != nil {
					releases()
					return nil, err
				}
				for i := range bat.RowCount() {
					srcBlk := vector.GetFixedAtNoTypeCheck[int32](bat.Vecs[0], i)
					srcRow := vector.GetFixedAtNoTypeCheck[uint32](bat.Vecs[1], i)
					destObj := vector.GetFixedAtNoTypeCheck[uint8](bat.Vecs[2], i)
					destBlk := vector.GetFixedAtNoTypeCheck[uint16](bat.Vecs[3], i)
					destRow := vector.GetFixedAtNoTypeCheck[uint32](bat.Vecs[4], i)

					booking[srcBlk][srcRow] = api.TransferDestPos{
						ObjIdx: destObj,
						BlkIdx: destBlk,
						RowIdx: destRow,
					}
				}
			}
			releases()
		}
		return booking, nil
	}
	return nil, nil
}

func validateTransferMapSourceBounds(
	ctx context.Context,
	bat *batch.Batch,
	booking api.TransferMaps,
) error {
	for i := range bat.RowCount() {
		srcBlk := vector.GetFixedAtNoTypeCheck[int32](bat.Vecs[0], i)
		srcRow := vector.GetFixedAtNoTypeCheck[uint32](bat.Vecs[1], i)
		if srcBlk < 0 ||
			int(srcBlk) >= len(booking) ||
			int(srcRow) >= len(booking[srcBlk]) {
			return moerr.NewInvalidInput(
				ctx,
				"transfer booking contains an out-of-range source position",
			)
		}
	}
	return nil
}
