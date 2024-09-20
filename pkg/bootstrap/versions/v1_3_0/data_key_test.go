// Copyright 2021 Matrix Origin
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

package v1_3_0

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/assert"
)

type MockTxnExecutor struct{}

func (MockTxnExecutor) Use(db string) {
	//TODO implement me
	panic("implement me")
}

func (MockTxnExecutor) LockTable(table string) error {
	//TODO implement me
	panic("implement me")
}

func (MockTxnExecutor) Exec(sql string, options executor.StatementOption) (executor.Result, error) {
	return executor.Result{}, nil
}

func (MockTxnExecutor) Txn() client.TxnOperator {
	//TODO implement me
	panic("implement me")
}

func TestInsertInitDataKey(t *testing.T) {
	txn := &MockTxnExecutor{}
	err := InsertInitDataKey(txn, "01234567890123456789012345678901")
	assert.NoError(t, err)
}
