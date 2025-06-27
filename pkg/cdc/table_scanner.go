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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
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
			Callbacks:            make(map[string]func(map[uint32]TblMap)),
			exec:                 getSqlExecutor(cnUUID),
			CallBackAccountId:    make(map[string]uint32),
			SubscribedAccountIds: make(map[uint32][]string),
			CallBackDbName:       make(map[string][]string),
			SubscribedDbNames:    make(map[string][]string),
			CallBackTableName:    make(map[string][]string),
			SubscribedTableNames: make(map[string][]string),
		}
	})
	return detector
}

// TblMap key is dbName.tableName, e.g. db1.t1
type TblMap map[string]*DbTableInfo

type TableDetector struct {
	sync.Mutex

	Mp        map[uint32]TblMap
	Callbacks map[string]func(map[uint32]TblMap)
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
}

func (s *TableDetector) Register(id string, accountId uint32, dbs []string, tables []string, cb func(map[uint32]TblMap)) {
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

	logutil.Info(
		"CDC-TableDetector-UnRegister",
		zap.String("task-id", id),
	)
}

func (s *TableDetector) scanTableLoop(ctx context.Context) {
	logutil.Info("CDC-TableDetector-Scan-Start")
	defer func() {
		logutil.Info("CDC-TableDetector-Scan-End")
	}()

	timeTick := time.Tick(15 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timeTick:
			if err := s.scanTable(); err != nil {
				logutil.Error(
					"CDC-TableDetector-Scan-Error",
					zap.Error(err),
				)
			}
			// do callbacks
			s.Lock()
			for _, cb := range s.Callbacks {
				go cb(s.Mp)
			}
			s.Unlock()
		}
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
		executor.Options{},
	)
	if err != nil {
		return err
	}
	defer result.Close()

	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			tblId := vector.MustFixedColWithTypeCheck[uint64](cols[0])[i]
			tblName := cols[1].UnsafeGetStringAt(i)
			dbId := vector.MustFixedColWithTypeCheck[uint64](cols[2])[i]
			dbName := cols[3].UnsafeGetStringAt(i)
			createSql := cols[4].UnsafeGetStringAt(i)
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
				mp[accountId][key] = &DbTableInfo{
					SourceDbId:      dbId,
					SourceDbName:    dbName,
					SourceTblId:     tblId,
					SourceTblName:   tblName,
					SourceCreateSql: createSql,
					IdChanged:       oldInfo.OnlyDiffinTblId(newInfo),
				}
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
