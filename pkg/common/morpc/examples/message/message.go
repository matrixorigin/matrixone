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

package message

import (
	"github.com/fagongzi/goetty/v2/buf"
)

// ExampleMessage example message
type ExampleMessage struct {
	MsgID   uint64
	Content string
}

func (tm *ExampleMessage) GetID() uint64 {
	return tm.MsgID
}

func (tm *ExampleMessage) SetID(id uint64) {
	tm.MsgID = id
}

func (tm *ExampleMessage) DebugString() string {
	return ""
}

func (tm *ExampleMessage) ProtoSize() int {
	return 4 + 8 + len(tm.Content)
}

func (tm *ExampleMessage) MarshalTo(data []byte) (int, error) {
	offset := 0

	buf.Uint64ToBytesTo(tm.MsgID, data[offset:])
	offset += 8

	buf.Int2BytesTo(len(tm.Content), data[offset:])
	offset += 4

	if len(tm.Content) > 0 {
		copy(data[offset:], []byte(tm.Content))
	}

	return len(data), nil
}

func (tm *ExampleMessage) Unmarshal(data []byte) error {
	offset := 0

	tm.MsgID = buf.Byte2Uint64(data)
	offset += 8

	n := buf.Byte2Int(data[offset:])
	offset += 4

	content := make([]byte, n)
	if n > 0 {
		copy(content, data[offset:offset+n])
	}
	tm.Content = string(content)

	return nil
}
