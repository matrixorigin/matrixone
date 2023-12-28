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

func (m MsgType) MessageName() string {
	switch m {
	case MsgMinValueSigned:
		return "MsgMinValueSigned"
	case MsgMinValueUnsigned:
		return "MsgMinValueUnsigned"
	case MsgMaxValueSigned:
		return "MsgMaxValueSigned"
	case MsgMaxValueUnsigned:
		return "MsgMaxValueUnsigned"
	}
	return "unknown message type"
}

type Message struct {
	SenderTag int32
	MsgType   MsgType
	Data      any
}

type MessageBoard struct {
	Messages []*Message
	Mutex    *sync.RWMutex
}

func NewMessageBoard() *MessageBoard {
	return &MessageBoard{
		Messages: make([]*Message, 0, 8),
		Mutex:    &sync.RWMutex{},
	}
}
