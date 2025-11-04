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
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const (
	DefaultSlowThreshold = 10 * time.Minute
	DefaultPrintInterval = 5 * time.Minute
)

var (
	detector *TableDetector
	once     sync.Once
)

var getSqlExecutor = func(cnUUID string) executor.SQLExecutor {
	v, _ := runtime.ServiceRuntime(cnUUID).GetGlobalVariables(runtime.InternalSQLExecutor)
	return v.(executor.SQLExecutor)
}

var GetTableDetector = func(cnUUID string) *TableDetector {
	once.Do(func() {
		detector = &TableDetector{
			Mp:                   make(map[uint32]TblMap),
			Callbacks:            make(map[string]TableCallback),
			exec:                 getSqlExecutor(cnUUID),
			CallBackAccountId:    make(map[string]uint32),
			SubscribedAccountIds: make(map[uint32][]string),
			CallBackDbName:       make(map[string][]string),
			SubscribedDbNames:    make(map[string][]string),
			CallBackTableName:    make(map[string][]string),
			SubscribedTableNames: make(map[string][]string),
			cdcStateManager:      NewCDCStateManager(),
		}
		detector.scanTableFn = detector.scanTable
	})
	return detector
}

// TblMap key is dbName.tableName, e.g. db1.t1
type TblMap map[string]*DbTableInfo

type TableCallback func(map[uint32]TblMap) error

type TableIterationState struct {
	CreateAt time.Time
	EndAt    time.Time
	FromTs   types.TS
	ToTs     types.TS
}
type CDCStateManager struct {
	activeRunners map[string]*TableIterationState
	mu            sync.RWMutex
}

func NewCDCStateManager() *CDCStateManager {
	return &CDCStateManager{
		activeRunners: make(map[string]*TableIterationState),
		mu:            sync.RWMutex{},
	}
}

func (s *CDCStateManager) AddActiveRunner(tblInfo *DbTableInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := GenDbTblKey(tblInfo.GetSourceDbName(), tblInfo.GetSourceTblName())
	s.activeRunners[key] = &TableIterationState{
		CreateAt: time.Now(),
		EndAt:    time.Now(),
		FromTs:   types.TS{},
		ToTs:     types.TS{},
	}
}

func (s *CDCStateManager) RemoveActiveRunner(tblInfo *DbTableInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := GenDbTblKey(tblInfo.GetSourceDbName(), tblInfo.GetSourceTblName())
	delete(s.activeRunners, key)
}

func (s *CDCStateManager) UpdateActiveRunner(tblInfo *DbTableInfo, fromTs, toTs types.TS, start bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := GenDbTblKey(tblInfo.GetSourceDbName(), tblInfo.GetSourceTblName())
	runner := s.activeRunners[key]
	if runner != nil {
		if start {
			runner.CreateAt = time.Now()
			runner.FromTs = fromTs
			runner.ToTs = toTs
			runner.EndAt = time.Time{}
		} else {
			runner.EndAt = time.Now()
		}
	}
}

func (s *CDCStateManager) PrintActiveRunners(slowThreshold time.Duration) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	slowRunners := ""
	now := time.Now()
	for info, state := range s.activeRunners {
		if state.EndAt.Equal(time.Time{}) && state.CreateAt.Before(now.Add(-slowThreshold)) {
			slowRunners = fmt.Sprintf(
				"%s, %v %v->%v %v",
				slowRunners,
				info,
				state.FromTs.ToString(),
				state.ToTs.ToString(),
				time.Since(state.CreateAt),
			)
		}
	}
	logutil.Info(
		"CDC-State",
		zap.Any("active runner count", len(s.activeRunners)),
		zap.Any("slow runners", slowRunners),
	)
}

type TableDetector struct {
	sync.Mutex

	Mp        map[uint32]TblMap
	Callbacks map[string]TableCallback
	exec      executor.SQLExecutor
	cancel    context.CancelFunc

	CallBackAccountId    map[string]uint32
	SubscribedAccountIds map[uint32][]string

	// taskname -> [db1, db2 ...]
	CallBackDbName map[string][]string
	// dbname -> [taska, taskb ...]
	SubscribedDbNames map[string][]string

	// taskname -> [tbl1, tbl2 ...]
	CallBackTableName map[string][]string
	// tablename -> [taska, taskb ...]
	SubscribedTableNames map[string][]string

	scanTableFn func() error

	lastMp          map[uint32]TblMap
	cdcStateManager *CDCStateManager

	// fastScan enables fast scanning for tests (1ms ticker instead of 15s)
	fastScan bool
}

// SetFastScan enables or disables fast scan mode (for testing)
func (s *TableDetector) SetFastScan(enabled bool) {
	s.fastScan = enabled
}

// SetExec sets the SQL executor (for testing)
func (s *TableDetector) SetExec(exec executor.SQLExecutor) {
	s.exec = exec
}

// SetCDCStateManager sets the CDC state manager (for testing)
func (s *TableDetector) SetCDCStateManager(manager *CDCStateManager) {
	s.cdcStateManager = manager
}

// SetScanTableFn sets the scan table function (for testing)
func (s *TableDetector) SetScanTableFn(fn func() error) {
	s.scanTableFn = fn
}

// ScanTable exposes the scanTable method (for testing)
func (s *TableDetector) ScanTable() error {
	return s.scanTable()
}

// getLastMp returns a copy of lastMp (with lock)
func (s *TableDetector) getLastMp() map[uint32]TblMap {
	s.Lock()
	defer s.Unlock()
	return s.getLastMpLocked()
}

// getLastMpLocked returns lastMp without locking (caller must hold lock)
func (s *TableDetector) getLastMpLocked() map[uint32]TblMap {
	return s.lastMp
}

// setLastMpWithCopy sets lastMp with a deep copy of Mp (with lock)
func (s *TableDetector) getLastMpWithCopy() map[uint32]TblMap {
	s.Lock()
	defer s.Unlock()
	return s.getLastMpWithCopyLocked()
}

// setLastMpWithCopyLocked sets lastMp with a deep copy of Mp without locking (caller must hold lock)
func (s *TableDetector) getLastMpWithCopyLocked() map[uint32]TblMap {
	// Deep copy Mp
	mpCopy := make(map[uint32]TblMap, len(s.Mp))
	for accountId, tblMap := range s.Mp {
		tblMapCopy := make(TblMap, len(tblMap))
		for key, info := range tblMap {
			tblMapCopy[key] = info
		}
		mpCopy[accountId] = tblMapCopy
	}
	s.lastMp = mpCopy
	return mpCopy
}

// setLastMp sets lastMp directly (with lock)
func (s *TableDetector) setLastMp(mp map[uint32]TblMap) {
	s.Lock()
	defer s.Unlock()
	s.setLastMpLocked(mp)
}

// setLastMpLocked sets lastMp directly without locking (caller must hold lock)
func (s *TableDetector) setLastMpLocked(mp map[uint32]TblMap) {
	s.lastMp = mp
}

// clearLastMp clears lastMp (with lock)
func (s *TableDetector) clearLastMp() {
	s.Lock()
	defer s.Unlock()
	s.clearLastMpLocked()
}

// clearLastMpLocked clears lastMp without locking (caller must hold lock)
func (s *TableDetector) clearLastMpLocked() {
	s.lastMp = nil
}

// getCallbacksCopy returns a copy of all callbacks (with lock)
func (s *TableDetector) getCallbacksCopy() []TableCallback {
	s.Lock()
	defer s.Unlock()
	return s.getCallbacksCopyLocked()
}

// getCallbacksCopyLocked returns a copy of all callbacks without locking (caller must hold lock)
func (s *TableDetector) getCallbacksCopyLocked() []TableCallback {
	callbacks := make([]TableCallback, 0, len(s.Callbacks))
	for _, cb := range s.Callbacks {
		callbacks = append(callbacks, cb)
	}
	return callbacks
}

func (s *TableDetector) Register(id string, accountId uint32, dbs []string, tables []string, cb TableCallback) {
	s.Lock()
	defer s.Unlock()

	s.SubscribedAccountIds[accountId] = append(s.SubscribedAccountIds[accountId], id)
	s.CallBackAccountId[id] = accountId

	for _, db := range dbs {
		s.SubscribedDbNames[db] = append(s.SubscribedDbNames[db], id)
	}
	s.CallBackDbName[id] = dbs

	for _, table := range tables {
		s.SubscribedTableNames[table] = append(s.SubscribedTableNames[table], id)
	}
	s.CallBackTableName[id] = tables

	if len(s.Callbacks) == 0 {
		ctx, cancel := context.WithCancel(
			defines.AttachAccountId(
				context.Background(),
				catalog.System_Account,
			),
		)
		s.cancel = cancel
		go s.scanTableLoop(ctx)
	}
	s.Callbacks[id] = cb
	logutil.Info(
		"CDC-TableDetector-Register",
		zap.String("task-id", id),
		zap.Uint32("account-id", accountId),
	)
}

func (s *TableDetector) UnRegister(id string) {
	s.Lock()
	defer s.Unlock()

	if accountID, ok := s.CallBackAccountId[id]; ok {
		if tasks, ok := s.SubscribedAccountIds[accountID]; ok {
			s.SubscribedAccountIds[accountID] = slices.DeleteFunc(tasks, func(taskID string) bool {
				return taskID == id
			})
			if len(s.SubscribedAccountIds[accountID]) == 0 {
				delete(s.SubscribedAccountIds, accountID)
			}
		}
		delete(s.CallBackAccountId, id)
	}

	if dbs, ok := s.CallBackDbName[id]; ok {
		for _, db := range dbs {
			if tasks, ok := s.SubscribedDbNames[db]; ok {
				s.SubscribedDbNames[db] = slices.DeleteFunc(tasks, func(taskID string) bool {
					return taskID == id
				})
				if len(s.SubscribedDbNames[db]) == 0 {
					delete(s.SubscribedDbNames, db)
				}
			}
		}
		delete(s.CallBackDbName, id)
	}

	if tables, ok := s.CallBackTableName[id]; ok {
		for _, table := range tables {
			if tasks, ok := s.SubscribedTableNames[table]; ok {
				s.SubscribedTableNames[table] = slices.DeleteFunc(tasks, func(taskID string) bool {
					return taskID == id
				})
				if len(s.SubscribedTableNames[table]) == 0 {
					delete(s.SubscribedTableNames, table)
				}
			}
		}
		delete(s.CallBackTableName, id)
	}

	delete(s.Callbacks, id)
	if len(s.Callbacks) == 0 {
		s.cancel()
		s.cancel = nil
	}

	logutil.Info(
		"CDC-TableDetector-UnRegister",
		zap.String("task-id", id),
	)
}

func (s *TableDetector) scanTableLoop(ctx context.Context) {
	logutil.Info("CDC-Scanner-Start")
	defer logutil.Info("CDC-Scanner-End")

	var tickerDuration, retryTickerDuration time.Duration // Use fastScan config first, fall back to injection for backward compatibility
	if s.fastScan {
		tickerDuration = 1 * time.Millisecond
		retryTickerDuration = 1 * time.Millisecond
	} else {
		tickerDuration = 15 * time.Second
		retryTickerDuration = 5 * time.Second
	}
	ticker := time.NewTicker(tickerDuration)
	defer ticker.Stop()

	retryTicker := time.NewTicker(retryTickerDuration)
	defer retryTicker.Stop()

	printStateTicker := time.NewTicker(DefaultPrintInterval)
	defer printStateTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.scanAndProcess(ctx)
		case <-retryTicker.C:
			lastMp := s.getLastMp()
			if lastMp == nil {
				continue
			}

			go s.processCallback(ctx, lastMp)
		case <-printStateTicker.C:
			s.cdcStateManager.PrintActiveRunners(DefaultSlowThreshold)
		}
	}
}

func (s *TableDetector) scanAndProcess(ctx context.Context) {
	if err := s.scanTableFn(); err != nil {
		logutil.Error("CDC-Scanner-ScanTable-Error", zap.Error(err))
		return
	}

	// Deep copy Mp to lastMp
	mp := s.getLastMpWithCopy()

	go s.processCallback(ctx, mp)
}

func (s *TableDetector) processCallback(ctx context.Context, tables map[uint32]TblMap) {
	callbacks := s.getCallbacksCopy()

	var err error
	for _, cb := range callbacks {
		err = cb(tables)
	}

	if err != nil {
		logutil.Warn("CDC-Scanner-Callback-Failed", zap.Error(err))
	} else {
		logutil.Info("CDC-Scanner-Callback-Success")
		s.clearLastMp()
	}
}

func (s *TableDetector) Close() {
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *TableDetector) scanTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var (
		accountIds string
		dbNames    string
		tableNames string
		mp         = make(map[uint32]TblMap)
	)

	s.Lock()

	if len(s.SubscribedAccountIds) == 0 || len(s.SubscribedDbNames) == 0 || len(s.SubscribedTableNames) == 0 {
		s.Mp = mp
		s.Unlock()
		return nil
	}
	var i int
	for accountId := range s.SubscribedAccountIds {
		if i != 0 {
			accountIds += ","
		}
		accountIds += fmt.Sprintf("%d", accountId)
		i++
	}
	dbNamesSlice := make([]string, 0, len(s.SubscribedDbNames))
	for dbName := range s.SubscribedDbNames {
		if dbName == "*" {
			dbNames = "*"
			break
		}
		dbNamesSlice = append(dbNamesSlice, dbName)
	}
	if dbNames != "*" {
		dbNames = AddSingleQuotesJoin(dbNamesSlice)
	}
	tableNamesSlice := make([]string, 0, len(s.SubscribedTableNames))
	for tableName := range s.SubscribedTableNames {
		if tableName == "*" {
			tableNames = "*"
			break
		}
		tableNamesSlice = append(tableNamesSlice, tableName)
	}
	if tableNames != "*" {
		tableNames = AddSingleQuotesJoin(tableNamesSlice)
	}
	s.Unlock()

	result, err := s.exec.Exec(
		ctx,
		CDCSQLBuilder.CollectTableInfoSQL(accountIds, dbNames, tableNames),
		executor.Options{}.WithStatementOption(executor.StatementOption{}.WithDisableLog()),
	)
	if err != nil {
		return err
	}
	defer result.Close()

	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			tblId := vector.MustFixedColWithTypeCheck[uint64](cols[0])[i]
			tblName := cols[1].GetStringAt(i)
			dbId := vector.MustFixedColWithTypeCheck[uint64](cols[2])[i]
			dbName := cols[3].GetStringAt(i)
			createSql := cols[4].GetStringAt(i)
			accountId := vector.MustFixedColWithTypeCheck[uint32](cols[5])[i]

			// skip table with foreign key
			if strings.Contains(strings.ToLower("createSql"), "foreign key") {
				continue
			}

			if _, ok := mp[accountId]; !ok {
				mp[accountId] = make(TblMap)
			}

			key := GenDbTblKey(dbName, tblName)

			// Lock to read from s.Mp
			s.Lock()
			var oldInfo *DbTableInfo
			var exists bool
			if tblMap, ok := s.Mp[accountId]; ok {
				oldInfo, exists = tblMap[key]
			}
			s.Unlock()

			newInfo := &DbTableInfo{}
			newInfo.SetSourceDbId(dbId)
			newInfo.SetSourceDbName(dbName)
			newInfo.SetSourceTblId(tblId)
			newInfo.SetSourceTblName(tblName)
			newInfo.SetSourceCreateSql(createSql)
			if !exists {
				mp[accountId][key] = newInfo
			} else {
				idChanged := oldInfo.OnlyDiffinTblId(newInfo)
				oldInfo.UpdateFields(dbId, dbName, tblId, tblName, createSql, idChanged)
				mp[accountId][key] = oldInfo
			}
		}
		return true
	})

	// replace the old table map
	s.Lock()
	s.Mp = mp
	s.Unlock()
	return nil
}
