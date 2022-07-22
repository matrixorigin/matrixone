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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

var sysTableNames map[string]bool

func init() {
	sysTableNames = make(map[string]bool)
	sysTableNames[catalog.SystemTable_Columns_Name] = true
	sysTableNames[catalog.SystemTable_Table_Name] = true
	sysTableNames[catalog.SystemTable_DB_Name] = true
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
	if isSys := sysTableNames[name]; isSys {
		err = catalog.ErrNotPermitted
		return
	}
	return db.txnDatabase.DropRelationByName(name)
}

func (db *txnSysDB) TruncateByName(name string) (rel handle.Relation, err error) {
	if isSys := sysTableNames[name]; isSys {
		err = catalog.ErrNotPermitted
		return
	}
	return db.txnDatabase.TruncateByName(name)
}
