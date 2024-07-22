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

package process

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
)

var _ Message = new(JoinMapMsg)

type JoinMapMsg struct {
	JoinMapPtr *hashmap.JoinMap
	IsShuffle  bool
	ShuffleIdx int32
	Tag        int32
}

func (t JoinMapMsg) Serialize() []byte {
	panic("top value message only broadcasts on current CN, don't need to serialize")
}

func (t JoinMapMsg) Deserialize([]byte) Message {
	panic("top value message only broadcasts on current CN, don't need to deserialize")
}

func (t JoinMapMsg) NeedBlock() bool {
	return true
}

func (t JoinMapMsg) GetMsgTag() int32 {
	return t.Tag
}

func (t JoinMapMsg) GetReceiverAddr() MessageAddress {
	return AddrBroadCastOnCurrentCN()
}

func (proc *Process) ReceiveJoinMap(anal Analyze, tag int32, isShuffle bool, shuffleIdx int32) *hashmap.JoinMap {
	start := time.Now()
	defer anal.WaitStop(start)
	msgReceiver := proc.NewMessageReceiver([]int32{tag}, AddrBroadCastOnCurrentCN())
	for {
		msgs, ctxDone := msgReceiver.ReceiveMessage(true, proc.Ctx)
		if ctxDone {
			return nil
		}
		for i := range msgs {
			msg, ok := msgs[i].(JoinMapMsg)
			if !ok {
				panic("expect join map message, receive unknown message!")
			}
			if isShuffle || msg.IsShuffle {
				if shuffleIdx != msg.ShuffleIdx {
					continue
				}
			}
			jm := msg.JoinMapPtr
			if jm == nil {
				return nil
			}
			if !jm.IsValid() {
				panic("join receive a joinmap which has been freed!")
			}
			return jm
		}
	}
}

func (proc *Process) FinalizeJoinMapMessage(tag int32, isShuffle bool, shuffleIdx int32, pipelineFailed bool, err error) {
	if pipelineFailed || err != nil {
		proc.SendMessage(JoinMapMsg{JoinMapPtr: nil, IsShuffle: isShuffle, ShuffleIdx: shuffleIdx, Tag: tag})
	}
}
