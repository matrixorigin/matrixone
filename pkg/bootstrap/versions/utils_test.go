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

package versions

import (
	"testing"

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
	bat := batch.New([]string{"a", "b", "c", "d", "e", "f", "g", "h"})
	bat.Vecs[0] = testutil.MakeVarcharVector([]string{"PubName"}, nil)
	bat.Vecs[1] = testutil.MakeVarcharVector([]string{"DbName"}, nil)
	bat.Vecs[2] = testutil.MakeUint64Vector([]uint64{1}, nil)
	bat.Vecs[3] = testutil.MakeVarcharVector([]string{"TablesStr"}, nil)
	bat.Vecs[4] = testutil.MakeVarcharVector([]string{"SubAccountsStr"}, nil)
	bat.Vecs[5] = testutil.MakeTimestampVector([]string{"2023-02-03 01:23:45"}, nil)
	bat.Vecs[6] = testutil.MakeTimestampVector([]string{"2023-02-03 01:23:45"}, nil)
	bat.Vecs[7] = testutil.MakeVarcharVector([]string{"Comment"}, nil)
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

func TestGetAllPubInfos(t *testing.T) {
	stub := gostub.Stub(&CheckTableDefinition, func(_ executor.TxnExecutor, _ uint32, _ string, _ string) (bool, error) {
		return true, nil
	})
	defer stub.Reset()

	accNameInfoMap := map[string]*pubsub.AccountInfo{
		"sys": {Id: 0, Name: "sys"},
	}
	infos, err := GetAllPubInfos(&MockTxnExecutor{}, accNameInfoMap)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(infos))
}
