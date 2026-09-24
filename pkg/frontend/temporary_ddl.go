// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"sort"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

// Bound retained generations even when a user keeps one transaction open.
// This is also the existing connection migration table-count limit.
const maxSessionTemporaryTableGenerations = maxMigrateTempTableCount

// OwnsTemporaryTable authorizes schema use by physical identity (for CTAS and
// prepared/internal SQL) without inferring ownership from a name prefix.
func (ses *Session) OwnsTemporaryTable(db, physical string) bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	key, ok := ses.tempTablesRev[physical]
	return ok && ses.tempTableIdentityLocked(key).dbName == db
}

func (ses *Session) CheckTemporaryTableCapacity(ctx context.Context) error {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if len(ses.tempTables)+len(ses.retiredTempTables) >= maxSessionTemporaryTableGenerations {
		return moerr.NewInvalidInput(ctx, "temporary table generation limit reached; finish the active transaction before creating more tables")
	}
	return nil
}

func (ses *Session) forgetTemporaryTableUndoLocked(key string) {
	for _, journal := range ses.tempTableTxnJournals {
		delete(journal.before, key)
		for _, statement := range journal.statements {
			delete(statement, key)
		}
	}
}

func (ses *Session) PublishTemporaryTable(db, alias, physical string) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	key := db + "." + alias
	ses.forgetTemporaryTableUndoLocked(key)
	if ses.tempTables == nil {
		ses.tempTables = make(map[string]string)
	}
	if ses.tempTablesRev == nil {
		ses.tempTablesRev = make(map[string]string)
	}
	if ses.tempTableIdentities == nil {
		ses.tempTableIdentities = make(map[string]tempTableIdentity)
	}
	if old, ok := ses.tempTables[key]; ok {
		delete(ses.tempTablesRev, old)
	}
	ses.tempTables[key] = physical
	ses.tempTablesRev[physical] = key
	ses.tempTableIdentities[key] = tempTableIdentity{dbName: db, alias: alias}
	ses.tempTableVersion++
}

func (ses *Session) RetireTemporaryTable(db, alias, physical string, indexes []string) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	remove := func(name string) {
		key, exists := ses.tempTablesRev[name]
		if !exists {
			return
		}
		ses.forgetTemporaryTableUndoLocked(key)
		delete(ses.tempTables, key)
		delete(ses.tempTablesRev, name)
		delete(ses.tempTableIdentities, key)
		ses.tempTableVersion++
	}
	remove(physical)
	for _, name := range indexes {
		remove(name)
	}
	if ses.retiredTempTables == nil {
		ses.retiredTempTables = make(map[string]sessionTempTable)
	}
	// The physical root owns its index tables. Never independently delete a
	// child before the root DROP has traversed that index's metadata.
	ses.retiredTempTables[physical] = sessionTempTable{dbName: db, realName: physical, retired: true}
}

// Called only after the session's data transaction ends, outside th.mu. Failed
// cleanup cannot turn a successful data COMMIT into a retryable SQL error.
// Its exact physical identity remains owned here and by reset/disconnect.
func (ses *Session) cleanupRetiredTempTables(ctx context.Context) {
	ses.mu.Lock()
	if len(ses.retiredTempTables) == 0 {
		ses.mu.Unlock()
		return
	}
	tables := make([]sessionTempTable, 0, len(ses.retiredTempTables))
	for _, table := range ses.retiredTempTables {
		tables = append(tables, table)
	}
	tenant := ses.tenant
	if tenant != nil {
		tenant = tenant.Copy()
	}
	ses.mu.Unlock()
	sort.Slice(tables, func(i, j int) bool { return tables[i].realName < tables[j].realName })
	if ctx == nil {
		ctx = context.Background()
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	if err := dropSessionTempTables(cleanupCtx, ses.GetService(), ses.GetTimeZone(), tenant, tables); err != nil {
		ses.Error(cleanupCtx, "temporary table reclamation deferred", zap.Error(err))
		return
	}
	ses.mu.Lock()
	defer ses.mu.Unlock()
	for _, table := range tables {
		delete(ses.retiredTempTables, table.realName)
	}
}

var _ process.TemporaryTableDDL = (*Session)(nil)
