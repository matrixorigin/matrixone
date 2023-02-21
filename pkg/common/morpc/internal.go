// Copyright 2023 Matrix Origin
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
	"fmt"

	"github.com/fagongzi/goetty/v2/buf"
)

type flagOnlyMessage struct {
	id   uint64
	flag byte
}

func (m *flagOnlyMessage) SetID(id uint64) {
	m.id = id
}

func (m *flagOnlyMessage) GetID() uint64 {
	return m.id
}

func (m *flagOnlyMessage) DebugString() string {
	return fmt.Sprintf("internal flag only message, flag %d", m.flag)
}

func (m *flagOnlyMessage) Size() int {
	return 8
}

func (m *flagOnlyMessage) MarshalTo(data []byte) (int, error) {
	buf.Uint64ToBytesTo(m.id, data)
	return 8, nil
}

func (m *flagOnlyMessage) Unmarshal(data []byte) error {
	m.id = buf.Byte2Uint64(data)
	return nil
}
