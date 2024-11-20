// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v2_0_1

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/prashantv/gostub"
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
	bat := batch.New([]string{"a"})
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil)
	bat.SetRowCount(1)
	return executor.Result{
		Batches: []*batch.Batch{bat},
		Mp:      testutil.TestUtilMp,
	}, nil
}

func (MockTxnExecutor) Txn() client.TxnOperator {
	//TODO implement me
	panic("implement me")
}

func Test_getSubbedAccNames(t *testing.T) {
	txn := &MockTxnExecutor{}
	accIdInfoMap := map[int32]*pubsub.AccountInfo{
		1: {Id: 1, Name: "acc1"},
	}
	accNames, err := getSubbedAccNames(txn, "pubAccountName", "pubName", accIdInfoMap)
	assert.NoError(t, err)
	assert.Equal(t, []string{"acc1"}, accNames)
}

func Test_migrateMoPubs(t *testing.T) {
	getAccountsStub := gostub.Stub(
		&pubsub.GetAccounts,
		func(_ executor.TxnExecutor) (map[string]*pubsub.AccountInfo, map[int32]*pubsub.AccountInfo, error) {
			return map[string]*pubsub.AccountInfo{
				"acc1": {Id: 1, Name: "acc1"},
			}, nil, nil
		},
	)
	defer getAccountsStub.Reset()

	getAllPubInfosStub := gostub.Stub(
		&versions.GetAllPubInfos,
		func(_ executor.TxnExecutor, _ map[string]*pubsub.AccountInfo) (map[string]*pubsub.PubInfo, error) {
			return map[string]*pubsub.PubInfo{
				"sys#pubName": {
					PubAccountName: "sys",
					PubName:        "pubName",
					SubAccountsStr: pubsub.AccountAll,
				},
				"acc1#pubName": {
					PubAccountName: "acc1",
					PubName:        "pubName",
					SubAccountsStr: pubsub.AccountAll,
				},
			}, nil
		},
	)
	defer getAllPubInfosStub.Reset()

	getSubbedAccNamesStub := gostub.Stub(
		&getSubbedAccNames,
		func(_ executor.TxnExecutor, _, _ string, _ map[int32]*pubsub.AccountInfo) ([]string, error) {
			return []string{"acc2"}, nil
		},
	)
	defer getSubbedAccNamesStub.Reset()

	txn := &MockTxnExecutor{}
	err := migrateMoPubs(txn)
	assert.NoError(t, err)
}
