// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnimpl

import (
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

var sysTableNames map[string]bool
var sysTableIds map[uint64]bool

// any tenant is able to see these db, but access them is required to be upgraded as Sys tenant in a tricky way.
// this can be done in frontend
var sysSharedDBNames map[string]bool

func init() {
	sysTableNames = make(map[string]bool)
	sysTableNames[pkgcatalog.MO_COLUMNS] = true
	sysTableNames[pkgcatalog.MO_TABLES] = true
	sysTableNames[pkgcatalog.MO_DATABASE] = true

	sysTableIds = make(map[uint64]bool)
	sysTableIds[pkgcatalog.MO_TABLES_ID] = true
	sysTableIds[pkgcatalog.MO_DATABASE_ID] = true
	sysTableIds[pkgcatalog.MO_COLUMNS_ID] = true

	sysSharedDBNames = make(map[string]bool)
	sysSharedDBNames[pkgcatalog.MO_CATALOG] = true
	sysSharedDBNames[metric.MetricDBConst] = true
	sysSharedDBNames[trace.SystemDBConst] = true
}

func isSysTable(name string) bool {
	return sysTableNames[name]
}

func isSysTableId(id uint64) bool {
	return sysTableIds[id]
}

func isSysSharedDB(name string) bool {
	return sysSharedDBNames[name]
}

func buildDB(db *txnDB) handle.Database {
	if db.entry.IsSystemDB() {
		return newSysDB(db)
	}
	return newDatabase(db)
}

type txnSysDB struct {
	*txnDatabase
}

func newSysDB(db *txnDB) *txnSysDB {
	sysDB := &txnSysDB{
		txnDatabase: newDatabase(db),
	}
	return sysDB
}

func (db *txnSysDB) DropRelationByName(name string) (rel handle.Relation, err error) {
	if isSysTable(name) {
		err = moerr.NewInternalError("drop relation %s is not permitted", name)
		return
	}
	return db.txnDatabase.DropRelationByName(name)
}

func (db *txnSysDB) DropRelationByID(id uint64) (rel handle.Relation, err error) {
	if isSysTableId(id) {
		err = moerr.NewInternalError("drop relation %d is not permitted", id)
		return
	}
	return db.txnDatabase.DropRelationByID(id)
}

func (db *txnSysDB) TruncateByName(name string) (rel handle.Relation, err error) {
	if isSysTable(name) {
		err = moerr.NewInternalError("truncate relation %s is not permitted", name)
		return
	}
	return db.txnDatabase.TruncateByName(name)
}

func (db *txnSysDB) TruncateWithID(name string, newTableId uint64) (rel handle.Relation, err error) {
	if isSysTable(name) {
		err = moerr.NewInternalError("truncate relation %s is not permitted", name)
		return
	}
	return db.txnDatabase.TruncateWithID(name, newTableId)
}

func (db *txnSysDB) TruncateByID(id uint64, newTableId uint64) (rel handle.Relation, err error) {
	if isSysTableId(id) {
		err = moerr.NewInternalError("truncate relation %d is not permitted", id)
		return
	}
	return db.txnDatabase.TruncateByID(id, newTableId)
}
