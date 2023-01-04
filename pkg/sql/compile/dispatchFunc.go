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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// deleteDispatch will do a full Dispatch (it means it will send batch to all Cns)
// streams[localIndex] is nil, because we need to send batch to a local chan
func deleteDispatch(streams []*dispatch.WrapperStream, localIndex uint64, bat *batch.Batch, localChan *process.WaitRegister, proc *process.Process) error {
	var err error
	if bat == nil {
		for i := range streams {
			if i == int(localIndex) {
				localChan.Ch <- bat
			} else {
				message := cnclient.AcquireMessage()
				message.Id = streams[i].Stream.ID()
				message.Cmd = 1
				message.Sid = pipeline.MessageEnd
				if err = streams[i].Stream.Send(proc.Ctx, message); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for i := range streams {
		if i == int(localIndex) {
			localChan.Ch <- bat
		} else {
			encodeData, errEncode := types.Encode(bat)
			if errEncode != nil {
				return errEncode
			}
			if len(encodeData) <= maxMessageSizeToMoRpc {
				message := cnclient.AcquireMessage()
				message.Id = streams[i].Stream.ID()
				message.Data = encodeData
				message.Cmd = 1
				message.Uuid = streams[i].Uuid[:]
				if err = streams[i].Stream.Send(proc.Ctx, message); err != nil {
					return err
				}
			} else {
				// if data is too large, it should be split into small blocks.
				start := 0
				for start < len(encodeData) {
					end := start + maxMessageSizeToMoRpc
					sid := pipeline.WaitingNext
					if end > len(encodeData) {
						end = len(encodeData)
						sid = pipeline.BatchEnd
					}
					message := cnclient.AcquireMessage()
					message.Id = streams[i].Stream.ID()
					message.Data = encodeData[start:end]
					message.Sid = uint64(sid)
					message.Cmd = 1
					message.Uuid = streams[i].Uuid[:]
					if err = streams[i].Stream.Send(proc.Ctx, message); err != nil {
						return err
					}
					start = end
				}
			}
		}
	}
	return nil
}
