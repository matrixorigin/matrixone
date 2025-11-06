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

	"github.com/matrixorigin/matrixone/pkg/objectio"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const (
	DefaultSlowThreshold = 5 * time.Minute // Reduced from 10 minutes for earlier detection
	DefaultPrintInterval = 1 * time.Minute // Reduced from 5 minutes for more frequent updates
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

	// Defensive: Ensure scanTableFn is always set (for testing scenarios)
	// In production, this should already be set by once.Do above
	// In tests with mocks, this prevents nil pointer dereference
	if detector != nil && detector.scanTableFn == nil {
		detector.scanTableFn = detector.scanTable
	}

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
	key := GenDbTblKey(tblInfo.SourceDbName, tblInfo.SourceTblName)
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
	key := GenDbTblKey(tblInfo.SourceDbName, tblInfo.SourceTblName)
	delete(s.activeRunners, key)
}

func (s *CDCStateManager) UpdateActiveRunner(tblInfo *DbTableInfo, fromTs, toTs types.TS, start bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := GenDbTblKey(tblInfo.SourceDbName, tblInfo.SourceTblName)
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

	now := time.Now()
	totalRunners := len(s.activeRunners)
	activeRunners := 0
	slowRunners := 0
	completedRunners := 0
	slowRunnersDetails := ""

	for info, state := range s.activeRunners {
		// Count active vs completed runners
		if state.EndAt.Equal(time.Time{}) {
			activeRunners++

			// Check if slow
			if state.CreateAt.Before(now.Add(-slowThreshold)) {
				slowRunners++
				duration := time.Since(state.CreateAt)
				slowRunnersDetails += fmt.Sprintf(
					"\n  %s: from=%s to=%s duration=%v",
					info,
					state.FromTs.ToString(),
					state.ToTs.ToString(),
					duration,
				)
			}
		} else {
			completedRunners++
		}
	}

	// Always log summary
	logutil.Info(
		"CDC-State-Summary",
		zap.Int("total-runners", totalRunners),
		zap.Int("active-runners", activeRunners),
		zap.Int("completed-runners", completedRunners),
		zap.Int("slow-runners", slowRunners),
		zap.Duration("slow-threshold", slowThreshold),
	)

	// Log slow runners details if any
	if slowRunners > 0 {
		logutil.Warn(
			"CDC-State-SlowRunners",
			zap.Int("count", slowRunners),
			zap.String("details", slowRunnersDetails),
		)
	}
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

	// to make sure there is at most only one handleNewTables running, so the truncate info will not be lost
	handling        bool
	lastMp          map[uint32]TblMap
	mu              sync.Mutex
	cdcStateManager *CDCStateManager
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
	if len(s.Callbacks) == 0 && s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}

	logutil.Info(
		"CDC-TableDetector-UnRegister",
		zap.String("task-id", id),
	)
}

func (s *TableDetector) scanTableLoop(ctx context.Context) {
	logutil.Info("CDC-TableDetector-Scan-Start")
	defer logutil.Info("CDC-TableDetector-Scan-End")

	var tickerDuration, retryTickerDuration time.Duration
	if msg, injected := objectio.CDCScanTableInjected(); injected || msg == "fast scan" {
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
			s.mu.Lock()

			if s.handling {
				s.mu.Unlock()
				continue
			}

			s.mu.Unlock()

			s.scanAndProcess(ctx)
		case <-retryTicker.C:
			s.mu.Lock()
			handling, lastMp := s.handling, s.lastMp
			s.mu.Unlock()
			if handling || lastMp == nil {
				continue
			}

			go s.processCallback(ctx, lastMp)
		case <-printStateTicker.C:
			s.cdcStateManager.PrintActiveRunners(DefaultSlowThreshold)
		}
	}
}

func (s *TableDetector) scanAndProcess(ctx context.Context) {
	// Defensive: ensure scanTableFn is set (for testing scenarios)
	if s.scanTableFn == nil {
		logutil.Warn("CDC-TableDetector-Scan-Skipped", zap.String("reason", "scanTableFn is nil"))
		return
	}

	if err := s.scanTableFn(); err != nil {
		logutil.Error("CDC-TableDetector-Scan-Error", zap.Error(err))
		return
	}

	s.mu.Lock()
	s.lastMp = s.Mp
	mp := s.lastMp
	s.mu.Unlock()

	go s.processCallback(ctx, mp)
}

func (s *TableDetector) processCallback(ctx context.Context, tables map[uint32]TblMap) {
	s.mu.Lock()
	s.handling = true
	s.mu.Unlock()

	var err error
	for _, cb := range s.Callbacks {
		err = cb(tables)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err != nil {
		logutil.Warn("CDC-TableDetector-Callback-Failed", zap.Error(err))
	} else {
		logutil.Info("CDC-TableDetector-Callback-Success")
		s.lastMp = nil
	}

	s.handling = false
}

func (s *TableDetector) Close() {
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *TableDetector) scanTable() error {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 10*time.Second, moerr.CauseTableDetectorScan)
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

			oldInfo, exists := s.Mp[accountId][key]
			newInfo := &DbTableInfo{
				SourceDbId:      dbId,
				SourceDbName:    dbName,
				SourceTblId:     tblId,
				SourceTblName:   tblName,
				SourceCreateSql: createSql,
			}
			if !exists {
				mp[accountId][key] = newInfo
			} else {
				idChanged := oldInfo.OnlyDiffinTblId(newInfo)
				oldInfo.SourceDbId = dbId
				oldInfo.SourceDbName = dbName
				oldInfo.SourceTblId = tblId
				oldInfo.SourceTblName = tblName
				oldInfo.SourceCreateSql = createSql
				oldInfo.IdChanged = oldInfo.IdChanged || idChanged
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
