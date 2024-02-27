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

import "github.com/matrixorigin/matrixone/pkg/objectio"

var _ Message = new(TopValueMessage)

type TopValueMessage struct {
	TopValueZM objectio.ZoneMap
	Tag        int32
}

func (t TopValueMessage) Serialize() []byte {
	panic("top value message only broadcasts on current CN, don't need to serialize")
}

func (t TopValueMessage) Deserialize([]byte) Message {
	panic("top value message only broadcasts on current CN, don't need to deserialize")
}

func (t TopValueMessage) NeedBlock() bool {
	return false
}

func (t TopValueMessage) GetMsgTag() int32 {
	return t.Tag
}

func (t TopValueMessage) GetReceiverAddr() MessageAddress {
	return AddrBroadCastOnCurrentCN()
}
