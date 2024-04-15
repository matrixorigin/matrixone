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

const (
	RuntimeFilter_IN          = 0
	RuntimeFilter_BITMAP      = 1
	RuntimeFilter_MIN_MAX     = 2
	RuntimeFilter_BINARY_FUSE = 3
	RuntimeFilter_PASS        = 100
	RuntimeFilter_DROP        = 101
)

var _ Message = new(RuntimeFilterMessage)

type RuntimeFilterMessage struct {
	Tag  int32
	Typ  int32
	Card int32
	Data []byte
}

func (rt RuntimeFilterMessage) Serialize() []byte {
	panic("runtime filter message only broadcasts on current CN, don't need to serialize")
}

func (rt RuntimeFilterMessage) Deserialize([]byte) Message {
	panic("runtime filter message only broadcasts on current CN, don't need to deserialize")
}

func (rt RuntimeFilterMessage) NeedBlock() bool {
	return true
}

func (rt RuntimeFilterMessage) GetMsgTag() int32 {
	return rt.Tag
}

func (rt RuntimeFilterMessage) GetReceiverAddr() MessageAddress {
	return AddrBroadCastOnCurrentCN()
}
