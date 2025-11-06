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
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func (m *wmMockSQLExecutor) Query(ctx context.Context, sql string, pts ie.SessionOverrideOptions) ie.InternalExecResult {
	if strings.HasPrefix(sql, "SELECT") {
		matches := m.selectRe.FindStringSubmatch(sql)
		return &InternalExecResultForTest{
			affectedRows: 1,
			resultSet: &MysqlResultSetForTest{
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

func TestWatermarkUpdater_MockSQLExecutor(t *testing.T) {
	executor := NewMockSQLExecutor()
	err := executor.CreateTable("db1", "t1", []string{"a", "b", "c"}, []string{"a", "b"})
	assert.NoError(t, err)
	err = executor.CreateTable("db1", "t1", []string{"a", "b", "c"}, []string{"a", "b"})
	assert.Error(t, err)
	err = executor.Insert("db1", "t1", []string{"a", "b", "c"}, [][]string{{"1", "2", "3"}, {"4", "5", "6"}}, false)
	assert.NoError(t, err)
	err = executor.Insert("db1", "t1", []string{"a", "b", "c"}, [][]string{{"1", "2", "3"}, {"4", "5", "6"}}, false)
	t.Logf("err: %v", err)
	assert.Error(t, err)
	_, err = executor.GetTableDataByPK("db1", "t2", []string{"1", "2"})
	assert.Error(t, err)
	rows, err := executor.GetTableDataByPK("db1", "t1", []string{"1", "2"})
	assert.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3"}, rows)
	_, err = executor.GetTableDataByPK("db1", "t1", []string{"1", "2", "3", "4"})
	assert.Error(t, err)

	err = executor.Insert("db1", "t1", []string{"a", "b", "c"}, [][]string{{"1", "2", "33"}, {"4", "5", "66"}}, true)
	assert.NoError(t, err)
	rows, err = executor.GetTableDataByPK("db1", "t1", []string{"1", "2"})
	assert.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "33"}, rows)
	rows, err = executor.GetTableDataByPK("db1", "t1", []string{"4", "5"})
	assert.NoError(t, err)
	assert.Equal(t, []string{"4", "5", "66"}, rows)

	err = executor.Delete("db1", "t1", []string{"1", "3"})
	assert.NoError(t, err)
	err = executor.Delete("db1", "t2", []string{"1", "2"})
	assert.Error(t, err)
	err = executor.Delete("db1", "t1", []string{"1", "2"})
	assert.NoError(t, err)
	rows, err = executor.GetTableDataByPK("db1", "t1", []string{"1", "2"})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(rows))

	err = executor.Delete("db1", "t1", []string{"4", "5"})
	assert.NoError(t, err)
	rows, err = executor.GetTableDataByPK("db1", "t1", []string{"4", "5"})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(rows))

	assert.Equal(t, 0, len(executor.tables[GenDbTblKey("db1", "t1")]))
	assert.Equal(t, 0, len(executor.pkIndexMap[GenDbTblKey("db1", "t1")]))

	err = executor.CreateTable(
		"mo_catalog",
		"mo_cdc_watermark",
		[]string{"account_id", "task_id", "db_name", "table_name", "watermark", "err_msg"},
		[]string{"account_id", "task_id", "db_name", "table_name"},
	)
	assert.NoError(t, err)
	u := NewCDCWatermarkUpdater("test", nil)
	jobs := make([]*UpdaterJob, 0, 2)
	jobs = append(jobs, &UpdaterJob{
		Key: &WatermarkKey{
			AccountId: 1,
			TaskId:    "test",
			DBName:    "db1",
			TableName: "t1",
		},
		Watermark: types.BuildTS(1, 1),
	})
	jobs = append(jobs, &UpdaterJob{
		Key: &WatermarkKey{
			AccountId: 2,
			TaskId:    "test",
			DBName:    "db1",
			TableName: "t2",
		},
		Watermark: types.BuildTS(2, 1),
	})
	insertSql := u.constructAddWMSQL(jobs)
	t.Logf("insertSql: %s", insertSql)

	err = executor.Exec(context.Background(), insertSql, ie.SessionOverrideOptions{})
	assert.NoError(t, err)
	assert.Equal(t, 2, executor.RowCount("mo_catalog", "mo_cdc_watermark"))
	keys := make(map[WatermarkKey]WatermarkResult)
	keys[*jobs[0].Key] = WatermarkResult{}
	keys[*jobs[1].Key] = WatermarkResult{}
	selectSql := u.constructReadWMSQL(keys)
	t.Logf("selectSql: %s", selectSql)
	tuples := executor.Query(context.Background(), selectSql, ie.SessionOverrideOptions{})
	assert.NoError(t, tuples.Error())
	assert.Equal(t, uint64(2), tuples.RowCount())
	row0, err := tuples.Row(context.Background(), 0)
	assert.NoError(t, err)
	row1, err := tuples.Row(context.Background(), 1)
	assert.NoError(t, err)
	// row0 and row1 disorder
	if row0[0] == "1" {
		assert.Equal(t, []any{"1", "test", "db1", "t1", "1-1"}, row0)
		assert.Equal(t, []any{"2", "test", "db1", "t2", "2-1"}, row1)
		accountId, err := tuples.GetUint64(context.Background(), 0, 0)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), accountId)
		accountId, err = tuples.GetUint64(context.Background(), 1, 0)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), accountId)
		taskId, err := tuples.GetString(context.Background(), 0, 1)
		assert.NoError(t, err)
		assert.Equal(t, "test", taskId)
		taskId, err = tuples.GetString(context.Background(), 1, 1)
		assert.NoError(t, err)
		assert.Equal(t, "test", taskId)
	} else {
		assert.Equal(t, []any{"2", "test", "db1", "t2", "2-1"}, row0)
		assert.Equal(t, []any{"1", "test", "db1", "t1", "1-1"}, row1)
		accountId, err := tuples.GetUint64(context.Background(), 0, 0)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), accountId)
		accountId, err = tuples.GetUint64(context.Background(), 1, 0)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), accountId)
		taskId, err := tuples.GetString(context.Background(), 0, 1)
		assert.NoError(t, err)
		assert.Equal(t, "test", taskId)
		taskId, err = tuples.GetString(context.Background(), 1, 1)
		assert.NoError(t, err)
		assert.Equal(t, "test", taskId)
	}

	for i, job := range jobs {
		job.Watermark = types.BuildTS(int64(i+10), 1)
	}

	keys2 := make(map[WatermarkKey]types.TS)
	keys2[*jobs[0].Key] = jobs[0].Watermark
	keys2[*jobs[1].Key] = jobs[1].Watermark

	insertUpdateSql := u.constructBatchUpdateWMSQL(keys2)
	t.Logf("insertUpdateSql: %s", insertUpdateSql)
	err = executor.Exec(context.Background(), insertUpdateSql, ie.SessionOverrideOptions{})
	assert.NoError(t, err)
	assert.Equal(t, 2, executor.RowCount("mo_catalog", "mo_cdc_watermark"))

	tuples = executor.Query(context.Background(), selectSql, ie.SessionOverrideOptions{})
	assert.NoError(t, tuples.Error())
	assert.Equal(t, uint64(2), tuples.RowCount())
	row0, err = tuples.Row(context.Background(), 0)
	assert.NoError(t, err)
	row1, err = tuples.Row(context.Background(), 1)
	assert.NoError(t, err)
	if row0[0] == "1" {
		assert.Equal(t, []any{"1", "test", "db1", "t1", "10-1"}, row0)
		assert.Equal(t, []any{"2", "test", "db1", "t2", "11-1"}, row1)
	} else {
		assert.Equal(t, []any{"2", "test", "db1", "t2", "11-1"}, row0)
		assert.Equal(t, []any{"1", "test", "db1", "t1", "10-1"}, row1)
	}

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
	var cronJobExecNum atomic.Int32
	var wg1 sync.WaitGroup
	wg1.Add(1)
	cronJob := func(ctx context.Context) {
		now := cronJobExecNum.Add(1)
		t.Logf("cronJobExecNum: %d", now)
		if now == 3 {
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
	assert.GreaterOrEqual(t, cronJobExecNum.Load(), int32(3))
	u.Stop()
	prevNum := cronJobExecNum.Load()
	time.Sleep(time.Millisecond * 5)
	assert.Equal(t, prevNum, cronJobExecNum.Load())
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
	err := u.UpdateWatermarkOnly(ctx, new(WatermarkKey), new(types.TS))
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
	err = u.UpdateWatermarkOnly(ctx, new(WatermarkKey), new(types.TS))
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
	key1.AccountId = 1
	wm1 := types.BuildTS(1, 1)
	wm2 := types.BuildTS(2, 1)
	err := u.UpdateWatermarkOnly(ctx, key1, &wm1)
	assert.NoError(t, err)

	key2 := new(WatermarkKey)
	key2.AccountId = 2

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
	key1.AccountId = 1
	key1.TaskId = "test"
	key1.DBName = "db1"
	key1.TableName = "t1"
	key2 := new(WatermarkKey)
	key2.AccountId = 2
	key2.TaskId = "test"
	key2.DBName = "db2"
	key2.TableName = "t2"
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
	expectedSql1 := "SELECT account_id, task_id, db_name, table_name, watermark FROM " +
		"`mo_catalog`.`mo_cdc_watermark` WHERE " +
		"(account_id = 1 AND task_id = 'test' AND db_name = 'db1' AND table_name = 't1') OR " +
		"(account_id = 2 AND task_id = 'test' AND db_name = 'db2' AND table_name = 't2')"
	expectedSql2 := "SELECT account_id, task_id, db_name, table_name, watermark FROM " +
		"`mo_catalog`.`mo_cdc_watermark` WHERE " +
		"(account_id = 2 AND task_id = 'test' AND db_name = 'db2' AND table_name = 't2') OR " +
		"(account_id = 1 AND task_id = 'test' AND db_name = 'db1' AND table_name = 't1')"
	realSql := u.constructReadWMSQL(keys)
	assert.True(t, expectedSql1 == realSql || expectedSql2 == realSql)
}

func TestCDCWatermarkUpdater_constructAddWMSQL(t *testing.T) {
	ie := newWmMockSQLExecutor()
	u := NewCDCWatermarkUpdater(
		t.Name(),
		ie,
	)
	keys := make([]*UpdaterJob, 0, 1)
	key1 := new(WatermarkKey)
	key1.AccountId = 1
	key1.TaskId = "test"
	key1.DBName = "db1"
	key1.TableName = "t1"
	ts1 := types.BuildTS(1, 1)
	keys = append(keys, &UpdaterJob{
		Key:       key1,
		Watermark: ts1,
	})
	key2 := new(WatermarkKey)
	key2.AccountId = 2
	key2.TaskId = "test"
	key2.DBName = "db2"
	key2.TableName = "t2"
	ts2 := types.BuildTS(2, 1)
	keys = append(keys, &UpdaterJob{
		Key:       key2,
		Watermark: ts2,
	})
	key3 := new(WatermarkKey)
	key3.AccountId = 3
	key3.TaskId = "test"
	key3.DBName = "db3"
	key3.TableName = "t3"
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
	keys := make(map[WatermarkKey]types.TS)
	key1 := new(WatermarkKey)
	key1.AccountId = 1
	key1.TaskId = "test"
	key1.DBName = "db1"
	key1.TableName = "t1"
	ts1 := types.BuildTS(1, 1)
	keys[*key1] = ts1
	key2 := new(WatermarkKey)
	key2.AccountId = 2
	key2.TaskId = "test"
	key2.DBName = "db2"
	key2.TableName = "t2"
	ts2 := types.BuildTS(2, 1)
	keys[*key2] = ts2
	key3 := new(WatermarkKey)
	key3.AccountId = 3
	key3.TaskId = "test"
	key3.DBName = "db3"
	key3.TableName = "t3"
	ts3 := types.BuildTS(3, 1)
	keys[*key3] = ts3
	expectedSql1 := "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, watermark) VALUES " +
		"(1, 'test', 'db1', 't1', '1-1')," +
		"(2, 'test', 'db2', 't2', '2-1')," +
		"(3, 'test', 'db3', 't3', '3-1') " +
		"ON DUPLICATE KEY UPDATE watermark = VALUES(watermark)"
	expectedSql2 := "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, watermark) VALUES " +
		"(3, 'test', 'db3', 't3', '3-1')," +
		"(2, 'test', 'db2', 't2', '2-1')," +
		"(1, 'test', 'db1', 't1', '1-1') " +
		"ON DUPLICATE KEY UPDATE watermark = VALUES(watermark)"
	expectedSql3 := "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, watermark) VALUES " +
		"(3, 'test', 'db3', 't3', '3-1')," +
		"(1, 'test', 'db1', 't1', '1-1')," +
		"(2, 'test', 'db2', 't2', '2-1') " +
		"ON DUPLICATE KEY UPDATE watermark = VALUES(watermark)"
	expectedSql4 := "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, watermark) VALUES " +
		"(2, 'test', 'db2', 't2', '2-1')," +
		"(3, 'test', 'db3', 't3', '3-1')," +
		"(1, 'test', 'db1', 't1', '1-1') " +
		"ON DUPLICATE KEY UPDATE watermark = VALUES(watermark)"
	expectedSql5 := "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, watermark) VALUES " +
		"(2, 'test', 'db2', 't2', '2-1')," +
		"(1, 'test', 'db1', 't1', '1-1')," +
		"(3, 'test', 'db3', 't3', '3-1') " +
		"ON DUPLICATE KEY UPDATE watermark = VALUES(watermark)"
	expectedSql6 := "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, watermark) VALUES " +
		"(1, 'test', 'db1', 't1', '1-1')," +
		"(2, 'test', 'db2', 't2', '2-1')," +
		"(3, 'test', 'db3', 't3', '3-1') " +
		"ON DUPLICATE KEY UPDATE watermark = VALUES(watermark)"
	realSql := u.constructBatchUpdateWMSQL(keys)
	assert.True(
		t,
		expectedSql1 == realSql ||
			expectedSql2 == realSql ||
			expectedSql3 == realSql ||
			expectedSql4 == realSql ||
			expectedSql5 == realSql ||
			expectedSql6 == realSql,
	)
}

func TestCDCWatermarkUpdater_constructBatchUpdateWMErrMsgSQL(t *testing.T) {
	ie := newWmMockSQLExecutor()
	u := NewCDCWatermarkUpdater(
		t.Name(),
		ie,
	)
	jobs := make([]*UpdaterJob, 0, 1)
	key1 := new(WatermarkKey)
	key1.AccountId = 1
	key1.TaskId = "test"
	key1.DBName = "db1"
	key1.TableName = "t1"
	ts1 := types.BuildTS(1, 1)
	jobs = append(jobs, &UpdaterJob{
		Key:       key1,
		Watermark: ts1,
		ErrMsg:    "err1",
	})
	key2 := new(WatermarkKey)
	key2.AccountId = 2
	key2.TaskId = "test"
	key2.DBName = "db2"
	key2.TableName = "t2"
	ts2 := types.BuildTS(2, 1)
	jobs = append(jobs, &UpdaterJob{
		Key:       key2,
		Watermark: ts2,
		ErrMsg:    "",
	})
	realSql := u.constructBatchUpdateWMErrMsgSQL(jobs)
	expectedSql := "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, err_msg) VALUES " +
		"(1, 'test', 'db1', 't1', 'err1')," +
		"(2, 'test', 'db2', 't2', '') " +
		"ON DUPLICATE KEY UPDATE err_msg = VALUES(err_msg)"
	assert.Equal(t, expectedSql, realSql)
}

func TestCDCWatermarkUpdater_execBatchUpdateWMErrMsg(t *testing.T) {
	ie := NewMockSQLExecutor()
	u := NewCDCWatermarkUpdater(
		t.Name(),
		ie,
	)
	jobs := make([]*UpdaterJob, 0, 2)
	key1 := new(WatermarkKey)
	key1.AccountId = 1
	key1.TaskId = "test"
	key1.DBName = "db1"
	key1.TableName = "t1"

	jobs = append(jobs, NewUpdateWMErrMsgJob(
		context.Background(),
		key1,
		"err1",
	))

	key2 := new(WatermarkKey)
	key2.AccountId = 2
	key2.TaskId = "test"
	key2.DBName = "db2"
	key2.TableName = "t2"
	jobs = append(jobs, NewUpdateWMErrMsgJob(
		context.Background(),
		key2,
		"err2",
	))

	err := ie.CreateTable(
		`mo_catalog`,
		`mo_cdc_watermark`,
		[]string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
		[]string{"account_id", "task_id", "db_name", "table_name"},
	)
	assert.NoError(t, err)

	err = ie.Insert(
		`mo_catalog`,
		`mo_cdc_watermark`,
		[]string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
		[][]string{{"1", "test", "db1", "t1", ""}, {"2", "test", "db2", "t2", ""}},
		false,
	)
	assert.NoError(t, err)

	u.committingErrMsgBuffer = jobs
	errMsg, err := u.execBatchUpdateWMErrMsg()
	assert.NoError(t, err)
	assert.Equal(t, "", errMsg)

	rowCount := ie.RowCount(`mo_catalog`, `mo_cdc_watermark`)
	assert.Equal(t, 2, rowCount)

	tuple, err := ie.GetTableDataByPK(
		`mo_catalog`,
		`mo_cdc_watermark`,
		[]string{"1", "test", "db1", "t1"},
	)
	assert.NoError(t, err)
	assert.Equal(t, []string{"1", "test", "db1", "t1", "err1"}, tuple)

	tuple, err = ie.GetTableDataByPK(
		`mo_catalog`,
		`mo_cdc_watermark`,
		[]string{"2", "test", "db2", "t2"},
	)
	assert.NoError(t, err)
	assert.Equal(t, []string{"2", "test", "db2", "t2", "err2"}, tuple)
}

func TestCDCWatermarkUpdater_ParseInsert(t *testing.T) {
	expectedSql := "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, watermark) VALUES " +
		"(1, 'test', 'db1', 't1', '1-1')," +
		"(2, 'test', 'db2', 't2', '2-1')," +
		"(3, 'test', 'db3', 't3', '3-1')"
	result, err := ParseInsert(expectedSql)
	assert.NoError(t, err)
	assert.Equal(t, 0, result.kind)
	assert.Equal(t, "mo_catalog", result.dbName)
	assert.Equal(t, "mo_cdc_watermark", result.tableName)
	assert.Equal(t, []string{"account_id", "task_id", "db_name", "table_name", "watermark"}, result.projectionColumns)
	assert.Equal(t, [][]string{{"1", "test", "db1", "t1", "1-1"}, {"2", "test", "db2", "t2", "2-1"}, {"3", "test", "db3", "t3", "3-1"}}, result.rows)

	expectedSql = "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, watermark, err_msg) VALUES " +
		"(1, 'test', 'db1', 't1', '1-1', '')," +
		"(2, 'test', 'db2', 't2', '2-1', 'err1')," +
		"(3, 'test', 'db3', 't3', '3-1', '') " +
		"ON DUPLICATE KEY UPDATE watermark = VALUES(watermark,err_msg)"
	result, err = ParseInsertOnDuplicateUpdate(expectedSql)
	assert.NoError(t, err)
	assert.Equal(t, 4, result.kind)
	assert.Equal(t, "mo_catalog", result.dbName)
	assert.Equal(t, "mo_cdc_watermark", result.tableName)
	assert.Equal(t, []string{"account_id", "task_id", "db_name", "table_name", "watermark", "err_msg"}, result.projectionColumns)
	assert.Equal(t, [][]string{{"1", "test", "db1", "t1", "1-1", ""}, {"2", "test", "db2", "t2", "2-1", "err1"}, {"3", "test", "db3", "t3", "3-1", ""}}, result.rows)
	assert.Equal(t, []string{"watermark", "err_msg"}, result.updateColumns)

	expectedSql = "INSERT INTO `mo_catalog`.`mo_cdc_watermark` VALUES (1, 'test', 'db1', 't1', '1-1', ''),(1, 'test', 'db1', 't2', '2-1', '')"
	result, err = ParseInsert(expectedSql)
	assert.NoError(t, err)
	assert.Equal(t, 0, result.kind)
	assert.Equal(t, "mo_catalog", result.dbName)
	assert.Equal(t, "mo_cdc_watermark", result.tableName)
	assert.Equal(t, []string{}, result.projectionColumns)
}

func TestCDCWatermarkUpdater_ParseUpdate(t *testing.T) {
	expectedSql := "UPDATE `db1`.`t1` SET col3 = '1-1' WHERE col1 = 1"
	result, err := ParseUpdate(expectedSql)
	assert.NoError(t, err)
	assert.Equal(t, 1, result.kind)
	assert.Equal(t, "db1", result.dbName)
	assert.Equal(t, "t1", result.tableName)
	assert.Equal(t, []string{"col3"}, result.updateColumns)
	assert.Equal(t, [][]string{{"1"}}, result.pkFilters)

	expectedSql = "UPDATE `db1`.`t1` SET col3 = '1-1', col4 = 4, col5 = '5-1' WHERE col1 = 1 AND col2 = 'test'"
	result, err = ParseUpdate(expectedSql)
	assert.NoError(t, err)
	assert.Equal(t, 1, result.kind)
	assert.Equal(t, "db1", result.dbName)
	assert.Equal(t, "t1", result.tableName)
	assert.Equal(t, []string{"col3", "col4", "col5"}, result.updateColumns)
	assert.Equal(t, [][]string{{"1", "test"}}, result.pkFilters)
}

func TestCDCWatermarkUpdater_ParseSelectByPKs(t *testing.T) {
	expectedSql := "SELECT col1, col2, col3 FROM `db1`.`t1` WHERE (col1 = 1 AND col2 = 'test') OR (col1 = 2 AND col2 = 'test2')"
	result, err := ParseSelectByPKs(expectedSql)
	assert.NoError(t, err)
	assert.Equal(t, 3, result.kind)
	assert.Equal(t, "db1", result.dbName)
	assert.Equal(t, "t1", result.tableName)
	assert.Equal(t, []string{"col1", "col2", "col3"}, result.projectionColumns)
	assert.Equal(t, [][]string{{"1", "test"}, {"2", "test2"}}, result.pkFilters)
}

func TestCDCWatermarkUpdater_CDCWatermarkUpdaterRun(t *testing.T) {
	ie := NewMockSQLExecutor()
	err := ie.CreateTable(
		"mo_catalog",
		"mo_cdc_watermark",
		[]string{"account_id", "task_id", "db_name", "table_name", "watermark", "err_msg"},
		[]string{"account_id", "task_id", "db_name", "table_name"},
	)
	assert.NoError(t, err)
	u := NewCDCWatermarkUpdater(
		t.Name(),
		ie,
		WithCronJobInterval(time.Millisecond*1),
	)
	u.Start()
	defer u.Stop()

	ctx := context.Background()

	ts := types.BuildTS(1, 1)
	key := &WatermarkKey{
		AccountId: 1,
		TaskId:    "task1",
		DBName:    "db1",
		TableName: "t1",
	}
	ret, err := u.GetOrAddCommitted(
		ctx,
		key,
		&ts,
	)
	assert.NoError(t, err)
	assert.Equal(t, ts, ret)

	ret, err = u.GetOrAddCommitted(
		ctx,
		key,
		&ts,
	)
	assert.NoError(t, err)
	assert.Equal(t, ts, ret)

	var smallTs types.TS
	ret, err = u.GetOrAddCommitted(
		ctx,
		key,
		&smallTs,
	)
	assert.NoError(t, err)
	assert.Equal(t, ts, ret)

	ret, err = u.GetFromCache(
		ctx,
		key,
	)
	assert.NoError(t, err)
	assert.Equal(t, ts, ret)

	assert.Equal(t, 1, ie.RowCount("mo_catalog", "mo_cdc_watermark"))

	for i := 0; i < 5; i++ {
		nts := types.BuildTS(int64(i+1), 1)
		err = u.UpdateWatermarkOnly(
			ctx,
			key,
			&nts,
		)
		assert.NoError(t, err)
		assert.NoError(t, err)
		ret, err = u.GetFromCache(
			ctx,
			key,
		)
		assert.NoError(t, err)
		assert.Equal(t, nts, ret)
		time.Sleep(time.Millisecond * 1)
	}
	testutils.WaitExpect(
		5000,
		func() bool {
			tuple, err := ie.GetTableDataByPK(
				"mo_catalog",
				"mo_cdc_watermark",
				[]string{"1", "task1", "db1", "t1"},
			)
			t.Logf("tuple: %v", tuple)
			return err == nil && tuple[4] == "5-1"
		},
	)
	assert.Equal(t, 1, ie.RowCount("mo_catalog", "mo_cdc_watermark"))

	var tasksWg sync.WaitGroup

	runTaskFunc := func(
		wg *sync.WaitGroup,
		key *WatermarkKey,
		physicalStart int64,
	) {
		defer wg.Done()
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(4)))

		logic := uint32(0)
		candidateTS := types.BuildTS(physicalStart, logic)
		logic++
		persistedTS, err := u.GetOrAddCommitted(
			ctx,
			key,
			&candidateTS,
		)
		assert.NoError(t, err)
		assert.True(t, candidateTS.LE(&persistedTS))

		for i := 0; i < 20; i++ {
			ts := types.BuildTS(physicalStart, logic)
			logic++
			err = u.UpdateWatermarkOnly(
				ctx,
				key,
				&ts,
			)
			assert.NoError(t, err)
			cacheTS, err := u.GetFromCache(
				ctx,
				key,
			)
			assert.NoError(t, err)
			assert.True(t, ts.EQ(&cacheTS))
			time.Sleep(time.Microsecond * time.Duration(rand.Intn(1000)))
		}
	}

	tasksWg.Add(5)
	keys := make([]*WatermarkKey, 0, 5)
	for i := 0; i < 5; i++ {
		key := &WatermarkKey{
			AccountId: 1,
			TaskId:    fmt.Sprintf("task%d", i+10),
			DBName:    "db1",
			TableName: "t1",
		}
		keys = append(keys, key)
		go runTaskFunc(&tasksWg, key, int64(i+100000))
	}

	tasksWg.Wait()
	assert.Equal(t, 6, ie.RowCount("mo_catalog", "mo_cdc_watermark"))
	for _, key := range keys {
		testutils.WaitExpect(
			5000,
			func() bool {
				tuple, err := ie.GetTableDataByPK(
					"mo_catalog",
					"mo_cdc_watermark",
					[]string{fmt.Sprintf("%d", key.AccountId), key.TaskId, key.DBName, key.TableName},
				)
				t.Logf("tuple: %v", tuple)
				if err != nil {
					return false
				}
				ts := types.StringToTS(tuple[4])
				return ts.Logical() >= 20
			},
		)
	}
}

func TestCDCWatermarkUpdater_UpdateWatermarkErrMsg(t *testing.T) {
	u, ie := InitCDCWatermarkUpdaterForTest(t)
	u.Start()
	defer u.Stop()

	key := &WatermarkKey{
		AccountId: 1,
		TaskId:    "task1",
		DBName:    "db1",
		TableName: "t1",
	}

	err := u.UpdateWatermarkErrMsg(
		context.Background(),
		key,
		"err1",
		nil, // Legacy format
	)
	// should be error because the watermark is not found
	assert.Error(t, err)

	ts := types.BuildTS(1, 1)
	ret, err := u.GetOrAddCommitted(
		context.Background(),
		key,
		&ts,
	)
	assert.NoError(t, err)
	assert.Equal(t, ts, ret)

	err = u.UpdateWatermarkErrMsg(
		context.Background(),
		key,
		"err1",
		nil, // Legacy format
	)
	assert.NoError(t, err)

	tuple, err := ie.GetTableDataByPK(
		"mo_catalog",
		"mo_cdc_watermark",
		[]string{fmt.Sprintf("%d", key.AccountId), key.TaskId, key.DBName, key.TableName},
	)
	assert.NoError(t, err)
	assert.Len(t, tuple, 5)
	assert.Equal(t, fmt.Sprintf("%d", key.AccountId), tuple[0])
	assert.Equal(t, key.TaskId, tuple[1])
	assert.Equal(t, key.DBName, tuple[2])
	assert.Equal(t, key.TableName, tuple[3])

	// Error message is now formatted as "N:timestamp:message" (non-retryable)
	// Note: In "N:timestamp:message" format, retry count is always 0 (not tracked for non-retryable)
	formattedErrMsg := tuple[4]
	metadata := ParseErrorMetadata(formattedErrMsg)
	require.NotNil(t, metadata)
	assert.False(t, metadata.IsRetryable)
	assert.Equal(t, 0, metadata.RetryCount) // Non-retryable format doesn't track retry count
	assert.Contains(t, metadata.Message, "err1")
}
