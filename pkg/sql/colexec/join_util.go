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

package colexec

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func ReceiveHashMap(anal process.Analyze, tag int32, isShuffle bool, shuffleIdx int32, proc *process.Process) *hashmap.JoinMap {
	start := time.Now()
	defer anal.WaitStop(start)
	msgReceiver := proc.NewMessageReceiver([]int32{tag}, process.AddrBroadCastOnCurrentCN())
	for {
		msgs, ctxDone := msgReceiver.ReceiveMessage(true, proc.Ctx)
		if ctxDone {
			return nil
		}
		for i := range msgs {
			msg, ok := msgs[i].(process.JoinMapMsg)
			if !ok {
				panic("expect join map message, receive unknown message!")
			}
			if isShuffle || msg.IsShuffle {
				if shuffleIdx != msg.ShuffleIdx {
					continue
				}
			}
			ret := msg.JoinMapPtr
			if !ret.IsValid() {
				panic("join receive a joinmap which has been freed!")
			}
			ret.IncRef()
			return ret
		}
	}
}
