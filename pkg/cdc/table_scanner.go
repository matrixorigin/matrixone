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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var (
	tableScanner *TableScanner
	once         sync.Once

	scanSql = fmt.Sprintf("select "+
		"  rel_id, "+
		"  relname, "+
		"  reldatabase_id, "+
		"  reldatabase, "+
		"  rel_createsql, "+
		"  account_id "+
		"from "+
		"  mo_catalog.mo_tables "+
		"where "+
		"  relkind = '%s'"+
		"  and reldatabase not in (%s)",
		catalog.SystemOrdinaryRel,                    // only scan ordinary tables
		AddSingleQuotesJoin(catalog.SystemDatabases), // skip system databases
	)
)

func GetTableScanner(cnUUID string) *TableScanner {
	once.Do(func() {
		tableScanner = &TableScanner{
			Mutex: sync.Mutex{},
			mp:    make(map[uint32]TblMap),
			cbs:   make(map[string]func(map[uint32]TblMap)),
		}
		v, _ := runtime.ServiceRuntime(cnUUID).GetGlobalVariables(runtime.InternalSQLExecutor)
		tableScanner.exec = v.(executor.SQLExecutor)
	})
	return tableScanner
}

// TblMap key is dbName.tableName, e.g. db1.t1
type TblMap map[string]*DbTableInfo

type TableScanner struct {
	sync.Mutex

	mp map[uint32]TblMap
	// callback
	cbs  map[string]func(map[uint32]TblMap)
	exec executor.SQLExecutor

	cancel context.CancelFunc
}

func (s *TableScanner) Register(id string, cb func(map[uint32]TblMap)) {
	s.Lock()
	defer s.Unlock()

	if len(s.cbs) == 0 {
		ctx, cancel := context.WithCancel(defines.AttachAccountId(context.Background(), catalog.System_Account))
		s.cancel = cancel
		go s.scanTableLoop(ctx)
	}
	s.cbs[id] = cb
}

func (s *TableScanner) UnRegister(id string) {
	s.Lock()
	defer s.Unlock()

	delete(s.cbs, id)
	if len(s.cbs) == 0 {
		s.cancel()
		s.cancel = nil
	}
}

func (s *TableScanner) scanTableLoop(ctx context.Context) {
	logutil.Infof("cdc TableScanner.scanTableLoop: start")
	defer func() {
		logutil.Infof("cdc TableScanner.scanTableLoop: end")
	}()

	timeTick := time.Tick(10 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timeTick:
			s.scanTable()
			// do callbacks
			for _, cb := range s.cbs {
				go cb(s.mp)
			}
		}
	}
}

func (s *TableScanner) scanTable() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := s.exec.Exec(ctx, scanSql, executor.Options{})
	if err != nil {
		return
	}
	defer result.Close()

	mp := make(map[uint32]TblMap)
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
			mp[accountId][key] = &DbTableInfo{
				SourceDbId:      dbId,
				SourceDbName:    dbName,
				SourceTblId:     tblId,
				SourceTblName:   tblName,
				SourceCreateSql: createSql,
			}
		}
		return true
	})

	// replace the old table map
	s.Lock()
	s.mp = mp
	s.Unlock()
}
