// Copyright 2026 Matrix Origin
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

package mock_frontend

import (
	"context"
	"reflect"

	"github.com/golang/mock/gomock"
)

// TryEnterRunSqlWithTokenAndSQL extends MockTxnOperator with the optional
// RunSQLAdmissionOperator capability. It lives outside txn_mock.go so the
// generated TxnOperator mock remains reproducible from its source interface.
func (m *MockTxnOperator) TryEnterRunSqlWithTokenAndSQL(
	cancel context.CancelFunc,
	sql string,
) (uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TryEnterRunSqlWithTokenAndSQL", cancel, sql)
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// TryEnterRunSqlWithTokenAndSQL records an expected optional admission call.
func (mr *MockTxnOperatorMockRecorder) TryEnterRunSqlWithTokenAndSQL(
	cancel, sql any,
) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"TryEnterRunSqlWithTokenAndSQL",
		reflect.TypeOf((*MockTxnOperator)(nil).TryEnterRunSqlWithTokenAndSQL),
		cancel,
		sql,
	)
}
