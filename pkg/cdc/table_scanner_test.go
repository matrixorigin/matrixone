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

package cdc

import (
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	mock_executor "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
)

func TestGetTableScanner(t *testing.T) {
	gostub.Stub(&getSqlExecutor, func(cnUUID string) executor.SQLExecutor {
		return &mock_executor.MockSQLExecutor{}
	})
	assert.NotNil(t, GetTableScanner("cnUUID"))
}

func TestTableScanner(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bat := batch.New([]string{"tblId", "tblName", "dbId", "dbName", "createSql", "accountId"})
	bat.Vecs[0] = testutil.MakeUint64Vector([]uint64{1}, nil)
	bat.Vecs[1] = testutil.MakeVarcharVector([]string{"tblName"}, nil)
	bat.Vecs[2] = testutil.MakeUint64Vector([]uint64{1}, nil)
	bat.Vecs[3] = testutil.MakeVarcharVector([]string{"dbName"}, nil)
	bat.Vecs[4] = testutil.MakeVarcharVector([]string{"createSql"}, nil)
	bat.Vecs[5] = testutil.MakeUint32Vector([]uint32{1}, nil)
	bat.SetRowCount(1)
	res := executor.Result{
		Mp:      testutil.TestUtilMp,
		Batches: []*batch.Batch{bat},
	}

	mockSqlExecutor := mock_executor.NewMockSQLExecutor(ctrl)
	mockSqlExecutor.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(res, nil).AnyTimes()

	scanner = &TableScanner{
		Mutex:     sync.Mutex{},
		Mp:        make(map[uint32]TblMap),
		Callbacks: make(map[string]func(map[uint32]TblMap)),
		exec:      mockSqlExecutor,
	}

	scanner.Register("id", func(mp map[uint32]TblMap) {})
	assert.Equal(t, 1, len(scanner.Callbacks))

	// one round of scanTable
	time.Sleep(11 * time.Second)

	scanner.UnRegister("id")
	assert.Equal(t, 0, len(scanner.Callbacks))
}
