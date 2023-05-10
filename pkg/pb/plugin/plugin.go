// Copyright 2021 - 2023 Matrix Origin
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

package plugin

import (
	"bytes"
	"fmt"
)

// SetID implement morpc Messgae
func (m *Request) SetID(id uint64) {
	m.RequestID = id
}

// GetID implement morpc Messgae
func (m *Request) GetID() uint64 {
	return m.RequestID
}

// DebugString returns the debug string
func (m *Request) DebugString() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%d: ", m.RequestID))
	buffer.WriteString(m.ClientInfo.String())
	buffer.WriteString("/")
	return buffer.String()
}

func (m *Response) SetID(id uint64) {
	m.RequestID = id
}

// GetID implement morpc Messgae
func (m *Response) GetID() uint64 {
	return m.RequestID
}

func (m *Response) DebugString() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%d: ", m.RequestID))
	buffer.WriteString(m.Recommendation.String())
	buffer.WriteString("/")
	return buffer.String()
}
