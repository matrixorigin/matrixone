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

package plan

import "sync"

type MsgType int32

const (
	MsgMinValueSigned   MsgType = 1
	MsgMinValueUnsigned MsgType = 2
	MsgMaxValueSigned   MsgType = 3
	MsgMaxValueUnsigned MsgType = 4
	MsgPipelineStart    MsgType = 5
	MsgPipelineStop     MsgType = 6
	MsgRuntimeFilter    MsgType = 7
	MsgHashMap          MsgType = 8
)

type Message struct {
	senderTag int32
	msgType   MsgType
	data      any
}

type MessageBoard struct {
	Messages []*Message
	Mutex    sync.RWMutex
}

func NewMessageBoard() *MessageBoard {
	return &MessageBoard{}
}
