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

package cdc

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

type wmMockSQLExecutor struct {
	mp       map[string]string
	insertRe *regexp.Regexp
	updateRe *regexp.Regexp
	selectRe *regexp.Regexp
}

func newWmMockSQLExecutor() *wmMockSQLExecutor {
	return &wmMockSQLExecutor{
		mp:       make(map[string]string),
		insertRe: regexp.MustCompile(`^insert .* values \(.*\, .*\, \'(.*)\'\, .*\, .*\, \'(.*)\'\, \'\'\)$`),
		updateRe: regexp.MustCompile(`^update .* set watermark\=\'(.*)\' where .* and table_id \= '(.*)'$`),
		selectRe: regexp.MustCompile(`^select .* and table_id \= '(.*)'$`),
	}
}

func (m *wmMockSQLExecutor) Exec(_ context.Context, sql string, _ ie.SessionOverrideOptions) error {
	if strings.HasPrefix(sql, "insert") {
		matches := m.insertRe.FindStringSubmatch(sql)
		m.mp[matches[1]] = matches[2]
	} else if strings.HasPrefix(sql, "update mo_catalog.mo_cdc_watermark set err_msg") {
		// do nothing
	} else if strings.HasPrefix(sql, "update") {
		matches := m.updateRe.FindStringSubmatch(sql)
		m.mp[matches[2]] = matches[1]
	} else if strings.HasPrefix(sql, "delete") {
		if strings.Contains(sql, "table_id") {
			delete(m.mp, "1_0")
		} else {
			m.mp = make(map[string]string)
		}
	}
	return nil
}

type MysqlResultSet struct {
	//column information
	Columns []string
	//column name --> column index
	Name2Index map[string]uint64
	//data
	Data [][]interface{}
}

type internalExecResult struct {
	affectedRows uint64
	resultSet    *MysqlResultSet
	err          error
}

func (res *internalExecResult) GetUint64(ctx context.Context, i uint64, j uint64) (uint64, error) {
	return res.resultSet.Data[i][j].(uint64), nil
}

func (res *internalExecResult) Error() error {
	return res.err
}

func (res *internalExecResult) ColumnCount() uint64 {
	return 1
}

func (res *internalExecResult) Column(ctx context.Context, i uint64) (name string, typ uint8, signed bool, err error) {
	return "test", 1, true, nil
}

func (res *internalExecResult) RowCount() uint64 {
	return 1
}

func (res *internalExecResult) Row(ctx context.Context, i uint64) ([]interface{}, error) {
	return nil, nil
}

func (res *internalExecResult) Value(ctx context.Context, ridx uint64, cidx uint64) (interface{}, error) {
	return nil, nil
}

func (res *internalExecResult) GetFloat64(ctx context.Context, ridx uint64, cid uint64) (float64, error) {
	return 0.0, nil
}

func (res *internalExecResult) GetString(ctx context.Context, i uint64, j uint64) (string, error) {
	return res.resultSet.Data[i][j].(string), nil
}

func (m *wmMockSQLExecutor) Query(ctx context.Context, sql string, pts ie.SessionOverrideOptions) ie.InternalExecResult {
	if strings.HasPrefix(sql, "select count") {
		return &internalExecResult{
			affectedRows: 1,
			resultSet: &MysqlResultSet{
				Columns:    nil,
				Name2Index: nil,
				Data: [][]interface{}{
					{uint64(len(m.mp))},
				},
			},
			err: nil,
		}
	} else if strings.HasPrefix(sql, "select") {
		matches := m.selectRe.FindStringSubmatch(sql)
		return &internalExecResult{
			affectedRows: 1,
			resultSet: &MysqlResultSet{
				Columns:    nil,
				Name2Index: nil,
				Data: [][]interface{}{
					{m.mp[matches[1]]},
				},
			},
			err: nil,
		}
	}

	return nil
}

func (m *wmMockSQLExecutor) ApplySessionOverride(opts ie.SessionOverrideOptions) {}

func TestNewWatermarkUpdater(t *testing.T) {
	taskId, err := uuid.NewV7()
	require.NoError(t, err)

	type args struct {
		accountId uint64
		taskId    string
		ie        ie.InternalExecutor
	}
	tests := []struct {
		name string
		args args
		want *WatermarkUpdater
	}{
		{
			name: "TestNewWatermarkUpdater",
			args: args{
				accountId: 1,
				taskId:    taskId.String(),
				ie:        nil,
			},
			want: &WatermarkUpdater{
				accountId:    1,
				taskId:       taskId,
				ie:           nil,
				watermarkMap: &sync.Map{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, NewWatermarkUpdater(tt.args.accountId, tt.args.taskId, tt.args.ie), "NewWatermarkUpdater(%v, %v, %v)", tt.args.accountId, tt.args.taskId, tt.args.ie)
		})
	}
}

func TestWatermarkUpdater_MemOps(t *testing.T) {
	u := &WatermarkUpdater{
		accountId:    1,
		taskId:       uuid.New(),
		ie:           nil,
		watermarkMap: &sync.Map{},
	}

	t1 := types.BuildTS(1, 1)
	u.UpdateMem("1_0", t1)
	actual := u.GetFromMem("1_0")
	assert.Equal(t, t1, actual)

	u.DeleteFromMem("1_0")
	actual = u.GetFromMem("1_0")
	assert.Equal(t, types.TS{}, actual)
}

func TestWatermarkUpdater_DbOps(t *testing.T) {
	u := &WatermarkUpdater{
		accountId:    1,
		taskId:       uuid.New(),
		ie:           newWmMockSQLExecutor(),
		watermarkMap: &sync.Map{},
	}

	// ---------- init count is 0
	count, err := u.GetCountFromDb()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), count)

	// ---------- insert into a record
	t1 := types.BuildTS(1, 1)
	info1 := &DbTableInfo{
		SourceTblIdStr: "1_0",
		SourceDbName:   "dbName",
		SourceTblName:  "tblName",
	}
	err = u.InsertIntoDb(info1, t1)
	assert.NoError(t, err)
	// count is 1
	count, err = u.GetCountFromDb()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), count)
	// get value of tableId 1
	actual, err := u.GetFromDb("1_0")
	assert.NoError(t, err)
	assert.Equal(t, t1, actual)

	// ---------- update t1 -> t2
	t2 := types.BuildTS(2, 1)
	err = u.flush("1_0", t2)
	assert.NoError(t, err)
	// value is t2
	actual, err = u.GetFromDb("1_0")
	assert.NoError(t, err)
	assert.Equal(t, t2, actual)

	// ---------- insert more records
	info2 := &DbTableInfo{
		SourceTblIdStr: "2_0",
		SourceDbName:   "dbName",
		SourceTblName:  "tblName",
	}
	err = u.InsertIntoDb(info2, t1)
	assert.NoError(t, err)
	info3 := &DbTableInfo{
		SourceTblIdStr: "3_0",
		SourceDbName:   "dbName",
		SourceTblName:  "tblName",
	}
	err = u.InsertIntoDb(info3, t1)
	assert.NoError(t, err)

	// ---------- delete tableId 1
	err = u.DeleteFromDb("1_0")
	assert.NoError(t, err)
	// count is 2
	count, err = u.GetCountFromDb()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), count)

	// ---------- delete all
	err = u.DeleteAllFromDb()
	assert.NoError(t, err)
	// count is 0
	count, err = u.GetCountFromDb()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), count)
}

func TestWatermarkUpdater_Run(t *testing.T) {
	u := &WatermarkUpdater{
		accountId:    1,
		taskId:       uuid.New(),
		ie:           newWmMockSQLExecutor(),
		watermarkMap: &sync.Map{},
	}
	ar := NewCdcActiveRoutine()
	go u.Run(context.Background(), ar)

	time.Sleep(2 * watermarkUpdateInterval)
	ar.Cancel <- struct{}{}
}

func TestWatermarkUpdater_flushAll(t *testing.T) {
	u := &WatermarkUpdater{
		accountId:    1,
		taskId:       uuid.New(),
		ie:           newWmMockSQLExecutor(),
		watermarkMap: &sync.Map{},
	}

	t1 := types.BuildTS(1, 1)
	info1 := &DbTableInfo{
		SourceTblIdStr: "1_0",
		SourceDbName:   "dbName",
		SourceTblName:  "tblName",
	}
	err := u.InsertIntoDb(info1, t1)
	assert.NoError(t, err)
	info2 := &DbTableInfo{
		SourceTblIdStr: "1_0",
		SourceDbName:   "dbName",
		SourceTblName:  "tblName",
	}
	err = u.InsertIntoDb(info2, t1)
	assert.NoError(t, err)
	info3 := &DbTableInfo{
		SourceTblIdStr: "1_0",
		SourceDbName:   "dbName",
		SourceTblName:  "tblName",
	}
	err = u.InsertIntoDb(info3, t1)
	assert.NoError(t, err)

	t2 := types.BuildTS(2, 1)
	u.UpdateMem("1_0", t2)
	u.UpdateMem("2_0", t2)
	u.UpdateMem("3_0", t2)
	u.flushAll()

	actual, err := u.GetFromDb("1_0")
	assert.NoError(t, err)
	assert.Equal(t, t2, actual)
	actual, err = u.GetFromDb("2_0")
	assert.NoError(t, err)
	assert.Equal(t, t2, actual)
	actual, err = u.GetFromDb("3_0")
	assert.NoError(t, err)
	assert.Equal(t, t2, actual)
}
