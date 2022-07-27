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

package pipeline

import fmt "fmt"

func (m *Message) GetID() uint64 {
	return m.Sid
}

func (m *Message) SetID(id uint64) {
	m.Sid = id
}

func (m *Message) DebugString() string {
	return fmt.Sprintf("sid: %v, cmd: %v, code: %s", m.Sid, m.Cmd, m.Code)
}
