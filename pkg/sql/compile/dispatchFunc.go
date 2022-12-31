// Copyright 2021 Matrix Origin
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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// deleteDispatch will parse the rowId of the bacth rows to get segmentId and BlockId,
// and then we can know which CN we need to send,we need to make sure one block will be
// deleted by only one CN
// We won't separate batch into CN Blocks and DN Blocks here,
// but in deletion operator, we will write s3, but for CN blocks we need to do Compaction
// and DM blocks we won't do Compaction

// streams[IBucket] is nil, because it's a local chan
func deleteDispatch2(streams []morpc.Stream, IBucket uint64, NBucket uint64, bat *batch.Batch) error {
	// GetRowId Vec,we use this to acquire block
	// rowIdVec := bat.GetVector(int32(len(bat.Vecs) - 1))
	// rowIds := vector.MustTCols[types.Rowid](rowIdVec)
	// sels := make([][]int64, 0, NBucket)
	// for i, rowId := range rowIds {
	// 	segmentId, blockId := DecodeRowId(rowId)
	// }
	// message := cnclient.AcquireMessage()
	// {
	// 	message.Id = streamSender.ID()
	// 	message.Data = sData
	// 	message.ProcInfoData = pData
	// }
	return nil
}

// deleteDispatch will do a full Dispatch
// streams[IBucket] is nil, because it's a local chan
func deleteDispatch(streams []*dispatch.WrapperStream, IBucket uint64, NBucket uint64, bat *batch.Batch, localChan *process.WaitRegister, proc *process.Process) error {
	var err error
	for i := range streams {
		if i == int(IBucket) {
			localChan.Ch <- bat
		} else {
			message := cnclient.AcquireMessage()
			message.Id = streams[i].Stream.ID()
			// 1 means is a WrapperBatch data
			message.Cmd = 1
			wrapBat := batch.NewWrpaBat(bat, streams[i].Uuid)
			// Todo: One batch maybe too large, we need to split it
			if message.Data, err = wrapBat.MarshalBinary(); err != nil {
				return err
			}
			if err = streams[i].Stream.Send(proc.Ctx, message); err != nil {
				return err
			}
		}
	}
	return nil
}
