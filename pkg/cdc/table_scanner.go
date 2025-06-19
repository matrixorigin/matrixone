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
			SubscribedAccountIds: make(map[uint32]bool),
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
	SubscribedAccountIds map[uint32]bool
}

func (s *TableDetector) Register(id string, accountId uint32, cb func(map[uint32]TblMap)) {
	s.Lock()
	defer s.Unlock()

	s.SubscribedAccountIds[accountId] = true
	s.CallBackAccountId[id] = accountId

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

	accountId := s.CallBackAccountId[id]
	delete(s.Callbacks, id)
	delete(s.CallBackAccountId, id)
	if len(s.Callbacks) == 0 && s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	found := false
	for _, cbAccountId := range s.CallBackAccountId {
		if cbAccountId == accountId {
			found = true
			break
		}
	}
	if !found {
		delete(s.SubscribedAccountIds, accountId)
	}
	logutil.Info(
		"CDC-TableDetector-UnRegister",
		zap.String("task-id", id),
		zap.Uint32("account-id", accountId),
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
		mp         = make(map[uint32]TblMap)
	)
	s.Lock()
	if len(s.SubscribedAccountIds) == 0 {
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
	s.Unlock()

	result, err := s.exec.Exec(
		ctx,
		CDCSQLBuilder.CollectTableInfoSQL(accountIds),
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
