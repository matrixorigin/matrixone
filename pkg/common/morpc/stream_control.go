// Copyright 2021 - 2024 Matrix Origin
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

package morpc

import (
	"github.com/fagongzi/goetty/v2/buf"
)

const (
	featureExclusive     = 1 << 0
	featureEnableControl = 1 << 1
)

var (
	registerType = 0
	resumeType   = 1
)

func newStreamRegister(id uint64, opts StreamOptions) *streamControl {
	return &streamControl{
		id:      id,
		ctlType: registerType,
		opts:    opts,
	}
}

func newResumeStream(id uint64) *streamControl {
	return &streamControl{
		id:      id,
		ctlType: resumeType,
	}
}

type streamControl struct {
	id      uint64
	ctlType int
	opts    StreamOptions
}

func (m *streamControl) SetID(id uint64) {
	m.id = id
}

func (m *streamControl) GetID() uint64 {
	return m.id
}

func (m *streamControl) DebugString() string {
	return "stream control message"
}

func (m *streamControl) Size() int {
	total := 8 + 4

	switch m.ctlType {
	case registerType:
		total += 4
	case resumeType:
	}

	return total
}

func (m *streamControl) MarshalTo(data []byte) (int, error) {
	buf.Uint64ToBytesTo(m.id, data)
	buf.Int2BytesTo(m.ctlType, data[8:])

	total := 12
	switch m.ctlType {
	case registerType:
		buf.Int2BytesTo(m.opts.feature, data[total:])
		total += 4
	case resumeType:
	}

	return total, nil
}

func (m *streamControl) Unmarshal(data []byte) error {
	m.id = buf.Byte2Uint64(data)
	m.ctlType = buf.Byte2Int(data[8:])

	switch m.ctlType {
	case registerType:
		m.opts.feature = buf.Byte2Int(data[12:])
	case resumeType:
	}
	return nil
}
