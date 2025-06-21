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
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockSQLExecutor struct {
	sync.RWMutex
	columnNames  map[string][]string
	columnIds    map[string]map[string]int
	tables       map[string][][]string
	pkColumnsMap map[string][]string
	pkIndexMap   map[string]map[string]int
	// support nulls in the future
}

func newMockSQLExecutor() *mockSQLExecutor {
	return &mockSQLExecutor{
		columnNames:  make(map[string][]string),
		columnIds:    make(map[string]map[string]int),
		tables:       make(map[string][][]string),
		pkColumnsMap: make(map[string][]string),
		pkIndexMap:   make(map[string]map[string]int),
	}
}

func (m *mockSQLExecutor) CreateTable(
	tableName string,
	columns []string,
	pkColumns []string,
) error {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.tables[tableName]; ok {
		return moerr.NewInternalErrorNoCtxf("table %s already exists", tableName)
	}
	m.columnNames[tableName] = columns
	m.columnIds[tableName] = make(map[string]int)
	for i, column := range columns {
		m.columnIds[tableName][column] = i
	}
	m.tables[tableName] = make([][]string, 0, 100)
	m.pkColumnsMap[tableName] = append(m.pkColumnsMap[tableName], pkColumns...)
	if len(pkColumns) > 0 {
		m.pkIndexMap[tableName] = make(map[string]int)
	}
	return nil
}

func (m *mockSQLExecutor) RemoveTable(
	tableName string,
) error {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.tables[tableName]; !ok {
		return moerr.NewInternalErrorNoCtxf("table %s not found", tableName)
	}
	delete(m.tables, tableName)
	delete(m.columnNames, tableName)
	delete(m.columnIds, tableName)
	delete(m.pkColumnsMap, tableName)
	delete(m.pkIndexMap, tableName)
	return nil
}

// when onDuplicateUpdate is true, currently this mock executor
// only supports:
//  1. when there is no primary key, the columns should be the same as the full columns
//  2. when there is primary key, the columns should include all the primary key columns
//     and all the pks in the tuples should be found in the table
//  3. remove this constraint when nulls are supported in the future: TODO
func (m *mockSQLExecutor) Insert(
	tableName string,
	columns []string,
	tuples [][]string,
	onDuplicateUpdate bool,
) error {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.tables[tableName]; !ok {
		return moerr.NewInternalErrorNoCtxf("table %s not found", tableName)
	}
	// the table full columns are: ['a', 'b', 'c'] and the
	// columns here may be ['a', 'b'] or ['a', 'c'] or ['b', 'c']
	// no check the columns here
	fullColumns := m.columnNames[tableName]
	columnIds := m.columnIds[tableName]
	pkColumns, hasPK := m.pkColumnsMap[tableName]
	pkIndex := m.pkIndexMap[tableName]
	tableData := m.tables[tableName]

	// 0: invalid
	// 1: insert without dedup
	// 2: insert with dedup and error on duplicate
	// 3: update only
	var insertMode int

	// onDuplicateUpdate constraint check:
	if onDuplicateUpdate {
		if hasPK {
			for _, pk := range pkColumns {
				if !slices.Contains(columns, pk) {
					return moerr.NewInternalErrorNoCtxf("primary key %s not found in columns %v", pk, columns)
				}
			}
			insertMode = 3
		} else {
			// no primary key, the columns should be the same as the full columns
			if len(columns) != len(fullColumns) {
				return moerr.NewInternalErrorNoCtxf("columns length mismatch: %d != %d", len(columns), len(fullColumns))
			}
			insertMode = 1
		}
	} else {
		if len(columns) != len(fullColumns) {
			return moerr.NewInternalErrorNoCtxf("columns length mismatch: %d != %d", len(columns), len(fullColumns))
		}
		if hasPK {
			insertMode = 2
		} else {
			insertMode = 1
		}
	}

	columnsIdMap := make(map[int]int)
	for i, column := range columns {
		// input columns: ['b', 'a', 'c'], full columns: ['a', 'b', 'c']
		// columnsIdMap: {1: 0, 0: 1, 2: 2}
		columnsIdMap[columnIds[column]] = i
	}
	// if pkColumns is ['a', 'b'], the input columns is ['b', 'a', 'c']
	// pkColumnIds: [1, 0]
	pkColumnIds := make([]int, len(pkColumns))
	for i, pk := range pkColumns {
		pkColumnIds[i] = columnsIdMap[columnIds[pk]]
	}

	// insert mode check:
	switch insertMode {
	case 1:
		// insert without dedup
		for _, tuple := range tuples {
			row := make([]string, len(fullColumns))
			for i, cell := range tuple {
				row[columnsIdMap[i]] = cell
			}
		}
	case 2:
		// insert with dedup and error on duplicate
		newPKs := make([]string, 0, len(tuples))
		newRows := make([][]string, 0, len(tuples))
		for _, tuple := range tuples {
			pkValues := make([]string, len(pkColumns))
			for idx, id := range pkColumnIds {
				pkValues[idx] = tuple[id]
			}
			pkValue := strings.Join(pkValues, ",")
			if _, ok := pkIndex[pkValue]; ok {
				return moerr.NewInternalErrorNoCtxf("primary key %s already exists", pkValue)
			}
			newPKs = append(newPKs, pkValue)
			row := make([]string, len(fullColumns))
			for i, cell := range tuple {
				row[columnsIdMap[i]] = cell
			}
			newRows = append(newRows, row)
		}
		// insert the new rows
		for i, row := range newRows {
			tableData = append(tableData, row)
			pk := newPKs[i]
			pkIndex[pk] = len(pkIndex)
		}
		m.tables[tableName] = tableData
	case 3:
		// update only
		// find the old row by the pk and update the row
		pkValues := make([]string, len(pkColumns))
		for _, tuple := range tuples {
			pkValues = pkValues[:0]
			for _, id := range pkColumnIds {
				pkValues = append(pkValues, tuple[id])
			}
			pkValue := strings.Join(pkValues, ",")
			// if the pkValue is not found, skip update
			offset, ok := pkIndex[pkValue]
			if !ok {
				continue
			}
			oldRow := tableData[offset]
			for i, cell := range tuple {
				oldRow[columnsIdMap[i]] = cell
			}
			tableData[offset] = oldRow
		}
		m.tables[tableName] = tableData
	}
	return nil
}

func (m *mockSQLExecutor) GetTableDataByPK(
	tableName string,
	pkValues []string,
) ([]string, error) {
	m.RLock()
	defer m.RUnlock()
	if _, ok := m.tables[tableName]; !ok {
		return nil, moerr.NewInternalErrorNoCtxf("table %s not found", tableName)
	}
	pkColumns, hasPK := m.pkColumnsMap[tableName]
	if !hasPK {
		return nil, moerr.NewInternalErrorNoCtxf("table %s has no primary key", tableName)
	}
	if len(pkValues) != len(pkColumns) {
		return nil, moerr.NewInternalErrorNoCtxf("pk values length mismatch: %d != %d", len(pkValues), len(pkColumns))
	}
	columns := m.columnNames[tableName]
	pkIndex := m.pkIndexMap[tableName]
	pkValue := strings.Join(pkValues, ",")
	offset, ok := pkIndex[pkValue]
	if !ok {
		return nil, nil
	}
	tableData := m.tables[tableName]
	row := tableData[offset]
	ret := make([]string, 0, len(columns))
	for i := range columns {
		ret = append(ret, row[i])
	}

	return ret, nil
}

type wmMockSQLExecutor struct {
	mp                        map[string]string
	insertRe                  *regexp.Regexp
	updateRe                  *regexp.Regexp
	selectRe                  *regexp.Regexp
	insertOnDuplicateUpdateRe *regexp.Regexp
}

func newWmMockSQLExecutor() *wmMockSQLExecutor {
	return &wmMockSQLExecutor{
		mp: make(map[string]string),
		// matches[1] = db_name, matches[2] = table_name, matches[3] = watermark
		insertRe:                  regexp.MustCompile(`^INSERT .* VALUES \(.*\, .*\, \'(.*)\'\, \'(.*)\'\, \'(.*)\'\, \'\'\)$`),
		updateRe:                  regexp.MustCompile(`^UPDATE .* SET watermark\=\'(.*)\' WHERE .* AND db_name \= '(.*)' AND table_name \= '(.*)'$`),
		selectRe:                  regexp.MustCompile(`^SELECT .* AND db_name \= '(.*)' AND table_name \= '(.*)'$`),
		insertOnDuplicateUpdateRe: regexp.MustCompile(`^INSERT .* VALUES \(.*\, .*\, \'(.*)\'\, \'(.*)\'\, \'(.*)\'\, \'\'\) ON DUPLICATE KEY UPDATE watermark \= VALUES\(watermark\)$`),
	}
}

func (m *wmMockSQLExecutor) Exec(_ context.Context, sql string, _ ie.SessionOverrideOptions) error {
	if strings.HasPrefix(sql, "INSERT") {
		matches := m.insertRe.FindStringSubmatch(sql)
		m.mp[GenDbTblKey(matches[1], matches[2])] = matches[3]
	} else if strings.HasPrefix(sql, "UPDATE `mo_catalog`.`mo_cdc_watermark` SET err_msg") {
		// do nothing
	} else if strings.HasPrefix(sql, "UPDATE") {
		matches := m.updateRe.FindStringSubmatch(sql)
		m.mp[GenDbTblKey(matches[2], matches[3])] = matches[1]
	} else if strings.HasPrefix(sql, "DELETE") {
		if strings.Contains(sql, "table_id") {
			delete(m.mp, "db1.t1")
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
	return uint64(len(res.resultSet.Data))
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
	if strings.HasPrefix(sql, "SELECT") {
		matches := m.selectRe.FindStringSubmatch(sql)
		return &internalExecResult{
			affectedRows: 1,
			resultSet: &MysqlResultSet{
				Columns:    nil,
				Name2Index: nil,
				Data: [][]interface{}{
					{m.mp[GenDbTblKey(matches[1], matches[2])]},
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
				taskId:       taskId.String(),
				ie:           nil,
				watermarkMap: &sync.Map{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, NewWatermarkUpdater(
				tt.args.accountId,
				tt.args.taskId,
				tt.args.ie,
			), "NewWatermarkUpdater(%v, %v, %v)", tt.args.accountId, tt.args.taskId, tt.args.ie)
		})
	}
}

func TestWatermarkUpdater_MemOps(t *testing.T) {
	taskId := NewTaskId()
	u := &WatermarkUpdater{
		accountId:    1,
		taskId:       taskId.String(),
		ie:           nil,
		watermarkMap: &sync.Map{},
	}

	t1 := types.BuildTS(1, 1)
	u.UpdateMem("db1", "t1", t1)
	actual := u.GetFromMem("db1", "t1")
	assert.Equal(t, t1, actual)

	u.DeleteFromMem("db1", "t1")
	actual = u.GetFromMem("db1", "t1")
	assert.Equal(t, types.TS{}, actual)
}

func TestWatermarkUpdater_DbOps(t *testing.T) {
	taskId := NewTaskId()
	u := &WatermarkUpdater{
		accountId:    1,
		taskId:       taskId.String(),
		ie:           newWmMockSQLExecutor(),
		watermarkMap: &sync.Map{},
	}

	// ---------- insert into a record
	t1 := types.BuildTS(1, 1)
	info1 := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "t1",
	}
	err := u.InsertIntoDb(info1, t1)
	assert.NoError(t, err)
	// get value of tableId 1
	actual, err := u.GetFromDb("db1", "t1")
	assert.NoError(t, err)
	assert.Equal(t, t1, actual)

	// ---------- update t1 -> t2
	t2 := types.BuildTS(2, 1)
	err = u.flush("db1.t1", t2)
	assert.NoError(t, err)
	// value is t2
	actual, err = u.GetFromDb("db1", "t1")
	assert.NoError(t, err)
	assert.Equal(t, t2, actual)

	// ---------- delete tableId 1
	err = u.DeleteFromDb("db1", "t1")
	assert.NoError(t, err)

	// ---------- insert more records
	info2 := &DbTableInfo{
		SourceDbName:  "db2",
		SourceTblName: "t2",
	}
	err = u.InsertIntoDb(info2, t1)
	assert.NoError(t, err)
	info3 := &DbTableInfo{
		SourceDbName:  "db3",
		SourceTblName: "t3",
	}
	err = u.InsertIntoDb(info3, t1)
	assert.NoError(t, err)

	err = u.SaveErrMsg("db1", "t1", "test")
	assert.NoError(t, err)
}

func TestWatermarkUpdater_Run(t *testing.T) {
	taskId := NewTaskId()
	u := &WatermarkUpdater{
		accountId:    1,
		taskId:       taskId.String(),
		ie:           newWmMockSQLExecutor(),
		watermarkMap: &sync.Map{},
	}
	ar := NewCdcActiveRoutine()
	go u.Run(context.Background(), ar)

	time.Sleep(2 * WatermarkUpdateInterval)
	ar.Cancel <- struct{}{}
}

func TestWatermarkUpdater_flushAll(t *testing.T) {
	taskId := NewTaskId()
	u := &WatermarkUpdater{
		accountId:    1,
		taskId:       taskId.String(),
		ie:           newWmMockSQLExecutor(),
		watermarkMap: &sync.Map{},
	}

	t1 := types.BuildTS(1, 1)
	info1 := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "t1",
	}
	err := u.InsertIntoDb(info1, t1)
	assert.NoError(t, err)
	info2 := &DbTableInfo{
		SourceDbName:  "db2",
		SourceTblName: "t2",
	}
	err = u.InsertIntoDb(info2, t1)
	assert.NoError(t, err)
	info3 := &DbTableInfo{
		SourceDbName:  "db3",
		SourceTblName: "t3",
	}
	err = u.InsertIntoDb(info3, t1)
	assert.NoError(t, err)

	t2 := types.BuildTS(2, 1)
	u.UpdateMem("db1", "t1", t2)
	u.UpdateMem("db2", "t2", t2)
	u.UpdateMem("db3", "t3", t2)
	u.flushAll()

	actual, err := u.GetFromDb("db1", "t1")
	assert.NoError(t, err)
	assert.Equal(t, t2, actual)
	actual, err = u.GetFromDb("db2", "t2")
	assert.NoError(t, err)
	assert.Equal(t, t2, actual)
	actual, err = u.GetFromDb("db3", "t3")
	assert.NoError(t, err)
	assert.Equal(t, t2, actual)
}

func TestWatermarkUpdater_MockSQLExecutor(t *testing.T) {
	executor := newMockSQLExecutor()
	err := executor.CreateTable("t1", []string{"a", "b", "c"}, []string{"a", "b"})
	assert.NoError(t, err)
	err = executor.CreateTable("t1", []string{"a", "b", "c"}, []string{"a", "b"})
	assert.Error(t, err)
	err = executor.Insert("t1", []string{"a", "b", "c"}, [][]string{{"1", "2", "3"}, {"4", "5", "6"}}, false)
	assert.NoError(t, err)
	err = executor.Insert("t1", []string{"a", "b", "c"}, [][]string{{"1", "2", "3"}, {"4", "5", "6"}}, false)
	t.Logf("err: %v", err)
	assert.Error(t, err)
	_, err = executor.GetTableDataByPK("t2", []string{"1", "2"})
	assert.Error(t, err)
	rows, err := executor.GetTableDataByPK("t1", []string{"1", "2"})
	assert.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3"}, rows)
	_, err = executor.GetTableDataByPK("t1", []string{"1", "2", "3", "4"})
	assert.Error(t, err)

	err = executor.Insert("t1", []string{"a", "b", "c"}, [][]string{{"1", "2", "33"}, {"4", "5", "66"}}, true)
	assert.NoError(t, err)
	rows, err = executor.GetTableDataByPK("t1", []string{"1", "2"})
	assert.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "33"}, rows)
	rows, err = executor.GetTableDataByPK("t1", []string{"4", "5"})
	assert.NoError(t, err)
	assert.Equal(t, []string{"4", "5", "66"}, rows)
}

// Scenario:
// 1. create a CDCWatermarkUpdater with user-defined cron job
// 2. wait for the cron job to execute 3 times
// 3. check the execution times: should be >= 3
// 4. stop the CDCWatermarkUpdater
// 5. get the execution times
// 5. wait for 5ms
// 6. check the execution times: should be the same as the previous value
// 7. start the CDCWatermarkUpdater
func TestCDCWatermarkUpdater_Basic1(t *testing.T) {
	ie := newWmMockSQLExecutor()
	cronJobExecNum := 0
	var wg1 sync.WaitGroup
	wg1.Add(1)
	cronJob := func(ctx context.Context) {
		cronJobExecNum++
		t.Logf("cronJobExecNum: %d", cronJobExecNum)
		if cronJobExecNum == 3 {
			wg1.Done()
		}
	}

	u := NewCDCWatermarkUpdater(
		"test",
		ie,
		WithCronJobInterval(time.Millisecond),
		WithCustomizedCronJob(cronJob),
		WithExportStatsInterval(time.Millisecond*5),
	)
	u.Start()
	wg1.Wait()
	assert.GreaterOrEqual(t, cronJobExecNum, 3)
	u.Stop()
	prevNum := cronJobExecNum
	time.Sleep(time.Millisecond * 5)
	assert.Equal(t, prevNum, cronJobExecNum)
}

func TestCDCWatermarkUpdater_cronRun(t *testing.T) {
	ie := newWmMockSQLExecutor()

	executeError := moerr.NewInternalErrorNoCtx(fmt.Sprintf("%s-execute-error", t.Name()))
	scheduleErr := moerr.NewInternalErrorNoCtx(fmt.Sprintf("%s-schedule-error", t.Name()))

	var passTimes atomic.Uint64
	passScheduler := func(job *UpdaterJob) (err error) {
		job.DoneWithResult(nil)
		passTimes.Add(1)
		return
	}
	var executeErrTimes atomic.Uint64
	executeErrScheduler := func(job *UpdaterJob) (err error) {
		job.DoneWithErr(executeError)
		executeErrTimes.Add(1)
		return
	}
	var scheduleErrTimes atomic.Uint64
	scheduleErrScheduler := func(job *UpdaterJob) (err error) {
		job.DoneWithErr(scheduleErr)
		scheduleErrTimes.Add(1)
		err = scheduleErr
		return
	}
	_ = executeErrScheduler
	_ = scheduleErrScheduler

	implScheduler := passScheduler

	scheduleJob := func(job *UpdaterJob) (err error) {
		return implScheduler(job)
	}
	u := NewCDCWatermarkUpdater(
		t.Name(),
		ie,
		WithCronJobInterval(time.Millisecond),
		WithCronJobErrorSupressTimes(1),
		WithCustomizedScheduleJob(scheduleJob),
	)
	u.Start()
	defer u.Stop()

	// check u.cacheUncommitted is empty logic
	var wg1 sync.WaitGroup
	wg1.Add(1)
	go func() {
		for {
			if u.stats.skipTimes.Load() > 0 {
				wg1.Done()
				break
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg1.Wait()

	ctx := context.Background()

	// add 1 uncommitted watermark and check the execution logic
	err := u.Add(ctx, new(WatermarkKey), new(types.TS))
	assert.NoError(t, err)

	// wait uncommitted watermark to be commtting
	wg1.Add(2)
	go func() {
		for {
			u.RLock()
			l1 := len(u.cacheCommitting)
			l2 := len(u.cacheUncommitted)
			u.RUnlock()
			if l1 == 1 && l2 == 0 {
				wg1.Done()
				break
			}
			time.Sleep(time.Millisecond)
		}
		for {
			if passTimes.Load() > 0 {
				wg1.Done()
				break
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg1.Wait()
	assert.Equal(t, uint64(1), passTimes.Load())

	// clear cacheCommitting manually
	u.Lock()
	u.cacheCommitting = make(map[WatermarkKey]types.TS)
	u.Unlock()

	implScheduler = executeErrScheduler
	err = u.Add(ctx, new(WatermarkKey), new(types.TS))
	assert.NoError(t, err)

	wg1.Add(2)
	go func() {
		for {
			if executeErrTimes.Load() > 0 {
				wg1.Done()
				break
			}
			time.Sleep(time.Millisecond)
		}
		for {
			if u.stats.errorTimes.Load() > 0 {
				wg1.Done()
				break
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg1.Wait()
	assert.Equal(t, uint64(1), executeErrTimes.Load())
	assert.Equal(t, uint64(1), u.stats.errorTimes.Load())
}

func TestCDCWatermarkUpdater_GetFromCache(t *testing.T) {
	ctx := context.Background()
	ie := newWmMockSQLExecutor()
	u := NewCDCWatermarkUpdater(
		t.Name(),
		ie,
	)
	key1 := new(WatermarkKey)
	key1.accountId = 1
	wm1 := types.BuildTS(1, 1)
	wm2 := types.BuildTS(2, 1)
	err := u.Add(ctx, key1, &wm1)
	assert.NoError(t, err)

	key2 := new(WatermarkKey)
	key2.accountId = 2

	// 1. only cacheUncommitted
	_, err = u.GetFromCache(ctx, key2)
	assert.ErrorIs(t, err, ErrNoWatermarkFound)

	rWM, err := u.GetFromCache(ctx, key1)
	assert.NoError(t, err)
	assert.True(t, wm1.EQ(&rWM))

	// 2. only cacheCommitting
	u.cacheUncommitted = make(map[WatermarkKey]types.TS)
	u.cacheCommitting = make(map[WatermarkKey]types.TS)
	u.cacheCommitting[*key1] = wm1
	rWM, err = u.GetFromCache(ctx, key1)
	assert.NoError(t, err)
	assert.True(t, wm1.EQ(&rWM))
	_, err = u.GetFromCache(ctx, key2)
	assert.ErrorIs(t, err, ErrNoWatermarkFound)

	// 3. only cacheCommitted
	u.cacheUncommitted = make(map[WatermarkKey]types.TS)
	u.cacheCommitting = make(map[WatermarkKey]types.TS)
	u.cacheCommitted = make(map[WatermarkKey]types.TS)
	u.cacheCommitted[*key1] = wm1
	rWM, err = u.GetFromCache(ctx, key1)
	assert.NoError(t, err)
	assert.True(t, wm1.EQ(&rWM))
	_, err = u.GetFromCache(ctx, key2)
	assert.ErrorIs(t, err, ErrNoWatermarkFound)

	// 4. cacheUncommitted and cacheCommitting same key with different watermark
	u.cacheUncommitted = make(map[WatermarkKey]types.TS)
	u.cacheCommitting = make(map[WatermarkKey]types.TS)
	u.cacheCommitting[*key1] = wm1
	u.cacheUncommitted[*key1] = wm2
	rWM, err = u.GetFromCache(ctx, key1)
	assert.NoError(t, err)
	assert.Truef(t, wm2.EQ(&rWM), "wm2: %s, rWM: %s", wm2.ToString(), rWM.ToString())
}

// test constructReadWMSQL
func TestCDCWatermarkUpdater_constructReadWMSQL(t *testing.T) {
	ie := newWmMockSQLExecutor()
	u := NewCDCWatermarkUpdater(
		t.Name(),
		ie,
	)
	keys := make(map[WatermarkKey]WatermarkResult)
	key1 := new(WatermarkKey)
	key1.accountId = 1
	key1.taskId = "test"
	key1.dbName = "db1"
	key1.tblName = "t1"
	key2 := new(WatermarkKey)
	key2.accountId = 2
	key2.taskId = "test"
	key2.dbName = "db2"
	key2.tblName = "t2"
	ts1 := types.BuildTS(1, 1)
	ts2 := types.BuildTS(2, 1)
	keys[*key1] = WatermarkResult{
		Watermark: ts1,
		Ok:        true,
	}
	keys[*key2] = WatermarkResult{
		Watermark: ts2,
		Ok:        true,
	}
	realSql := u.constructReadWMSQL(keys)
	expectedSql := "SELECT account_id, task_id, db_name, tbl_name, watermark FROM " +
		"`mo_catalog`.`mo_cdc_watermark` WHERE " +
		"(account_id = 1 AND task_id = 'test' AND db_name = 'db1' AND tbl_name = 't1') OR " +
		"(account_id = 2 AND task_id = 'test' AND db_name = 'db2' AND tbl_name = 't2')"
	assert.Equal(t, expectedSql, realSql)
}

func TestCDCWatermarkUpdater_constructAddWMSQL(t *testing.T) {
	ie := newWmMockSQLExecutor()
	u := NewCDCWatermarkUpdater(
		t.Name(),
		ie,
	)
	keys := make([]*UpdaterJob, 0, 1)
	key1 := new(WatermarkKey)
	key1.accountId = 1
	key1.taskId = "test"
	key1.dbName = "db1"
	key1.tblName = "t1"
	ts1 := types.BuildTS(1, 1)
	keys = append(keys, &UpdaterJob{
		Key:       key1,
		Watermark: ts1,
	})
	key2 := new(WatermarkKey)
	key2.accountId = 2
	key2.taskId = "test"
	key2.dbName = "db2"
	key2.tblName = "t2"
	ts2 := types.BuildTS(2, 1)
	keys = append(keys, &UpdaterJob{
		Key:       key2,
		Watermark: ts2,
	})
	key3 := new(WatermarkKey)
	key3.accountId = 3
	key3.taskId = "test"
	key3.dbName = "db3"
	key3.tblName = "t3"
	ts3 := types.BuildTS(3, 1)
	keys = append(keys, &UpdaterJob{
		Key:       key3,
		Watermark: ts3,
	})
	realSql := u.constructAddWMSQL(keys)
	expectedSql := "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"VALUES " +
		"(1, 'test', 'db1', 't1', '1-1', '')," +
		"(2, 'test', 'db2', 't2', '2-1', '')," +
		"(3, 'test', 'db3', 't3', '3-1', '')"

	assert.Equal(t, expectedSql, realSql)
}

func TestCDCWatermarkUpdater_constructBatchUpdateWMSQL(t *testing.T) {
	ie := newWmMockSQLExecutor()
	u := NewCDCWatermarkUpdater(
		t.Name(),
		ie,
	)
	keys := make([]*UpdaterJob, 0, 1)
	key1 := new(WatermarkKey)
	key1.accountId = 1
	key1.taskId = "test"
	key1.dbName = "db1"
	key1.tblName = "t1"
	ts1 := types.BuildTS(1, 1)
	keys = append(keys, &UpdaterJob{
		Key:       key1,
		Watermark: ts1,
	})
	key2 := new(WatermarkKey)
	key2.accountId = 2
	key2.taskId = "test"
	key2.dbName = "db2"
	key2.tblName = "t2"
	ts2 := types.BuildTS(2, 1)
	keys = append(keys, &UpdaterJob{
		Key:       key2,
		Watermark: ts2,
	})
	key3 := new(WatermarkKey)
	key3.accountId = 3
	key3.taskId = "test"
	key3.dbName = "db3"
	key3.tblName = "t3"
	ts3 := types.BuildTS(3, 1)
	keys = append(keys, &UpdaterJob{
		Key:       key3,
		Watermark: ts3,
	})
	expectedSql := "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, watermark) VALUES " +
		"(1, 'test', 'db1', 't1', '1-1')," +
		"(2, 'test', 'db2', 't2', '2-1')," +
		"(3, 'test', 'db3', 't3', '3-1') " +
		"ON DUPLICATE KEY UPDATE watermark = VALUES(watermark)"
	realSql := u.constructBatchUpdateWMSQL(keys)
	assert.Equal(t, expectedSql, realSql)

	re := regexp.MustCompile(`\(([^,]+),\s*'([^']+)',\s*'([^']+)',\s*'([^']+)',\s*'([^']+)'\)`)

	// Find all matches in the SQL string.
	matches := re.FindAllStringSubmatch(realSql, -1)
	t.Log(matches)
	for _, match := range matches {
		t.Log(match)
	}
}
