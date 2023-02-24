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

package lock

import (
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

func (m *Request) DebugString() string {
	return ""
}

// SetID implement morpc Messgae
func (m *Response) SetID(id uint64) {
	m.RequestID = id
}

// GetID implement morpc Messgae
func (m *Response) GetID() uint64 {
	return m.RequestID
}

func (m *Response) DebugString() string {
	return ""
}

// Changed returns true if LockTable bind changed
func (m LockTable) Changed(v LockTable) bool {
	return m.Version != v.Version ||
		m.ServiceID != v.ServiceID
}

// DebugString returns the debug string
func (m LockTable) DebugString() string {
	return fmt.Sprintf("%d/%s/%d", m.Table, m.ServiceID, m.Version)
}

// WithGranularity set rows granularity, the default granularity is Row.
func (m LockOptions) WithGranularity(granularity Granularity) LockOptions {
	m.Granularity = granularity
	return m
}

// WithMode set lock mode, the default mode is Exclusive.
func (m LockOptions) WithMode(mode LockMode) LockOptions {
	m.Mode = mode
	return m
}

// WithWaitPolicy set wait policy, the default policy is Wait.
func (m LockOptions) WithWaitPolicy(policy WaitPolicy) LockOptions {
	m.Policy = policy
	return m
}
