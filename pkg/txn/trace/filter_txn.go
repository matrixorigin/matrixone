// Copyright 2024 Matrix Origin
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

package trace

import (
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

var (
	sessionMethod    = "session"
	connectionMethod = "connection"
	tenantMethod     = "tenant"
	userMethod       = "user"
)

type txnFilters struct {
	filters []TxnFilter
}

func (f *txnFilters) isEmpty() bool {
	return f == nil || len(f.filters) == 0
}

// filter return true means the txn need to be skipped.
// Return true cases:
// 1. any filter skipped
// 2. filters is empty
func (f *txnFilters) filter(op client.TxnOperator) bool {
	if f.isEmpty() {
		return true
	}

	for _, v := range f.filters {
		if v.Filter(op) {
			return true
		}
	}
	return false
}

type sessionIDFilter struct {
	sessionID string
}

func (f *sessionIDFilter) Filter(op client.TxnOperator) bool {
	match := f.sessionID == op.TxnOptions().SessionID
	return !match
}

type connectionIDFilter struct {
	sessionID    string
	connectionID uint32
}

func (f *connectionIDFilter) Filter(op client.TxnOperator) bool {
	match := f.sessionID == op.TxnOptions().SessionID &&
		f.connectionID == op.TxnOptions().ConnectionID
	return !match
}

type tenantFilter struct {
	accountID uint32
	userName  string
}

func (f *tenantFilter) Filter(op client.TxnOperator) bool {
	match := f.accountID == op.TxnOptions().AccountID &&
		f.userName == op.TxnOptions().UserName
	return !match
}

type userFilter struct {
	userName string
}

func (f *userFilter) Filter(op client.TxnOperator) bool {
	match := f.userName == op.TxnOptions().UserName
	return !match
}

type disableFilter struct {
}

func (f *disableFilter) Filter(op client.TxnOperator) bool {
	return op == nil ||
		op.TxnOptions().TraceDisabled() ||
		op.TxnOptions().UserName == ""
}
