// Copyright 2021 Matrix Origin
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

package gc

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type dropDB struct {
	dbid   uint32
	drop   bool
	tables map[uint64]*dropTable
}

func NewDropDB(id uint32) *dropDB {
	return &dropDB{
		dbid:   id,
		drop:   false,
		tables: make(map[uint64]*dropTable),
	}
}

func (d *dropDB) getTable(id common.ID) *dropTable {
	table := d.tables[id.TableID]
	if table == nil {
		table = NewDropTable(id.TableID)
	}
	d.tables[id.TableID] = table
	return table
}

func (d *dropDB) addBlock(id common.ID, name string) {
	table := d.getTable(id)
	table.addBlock(id, name)
}

func (d *dropDB) deleteBlock(id common.ID, name string) {
	table := d.getTable(id)
	table.deleteBlock(id, name)

}

func (d *dropDB) merge(dropDB *dropDB) {
	for tid, entry := range dropDB.tables {
		table := d.tables[tid]
		if table == nil {
			table = NewDropTable(tid)
		}
		table.merge(entry)
		if !table.drop {
			table.drop = entry.drop
		}
		d.tables[tid] = table
	}
}

func (d *dropDB) softGC() []string {
	gc := make([]string, 0)
	for _, entry := range d.tables {
		if d.drop {
			entry.drop = d.drop
		}
		objects := entry.softGC()
		if len(objects) > 0 {
			gc = append(gc, objects...)
		}
	}
	return gc
}

func (d *dropDB) DropTable(id common.ID) {
	table := d.getTable(id)
	table.drop = true
	d.tables[id.TableID] = table
}

func (d *dropDB) Compare(db *dropDB) bool {
	if d.drop != db.drop {
		return false
	}
	if len(d.tables) != len(db.tables) {
		return false
	}
	for id, entry := range d.tables {
		table := db.tables[id]
		if table == nil {
			return false
		}
		ok := entry.Compare(table)
		if !ok {
			return ok
		}

	}
	return true
}

func (d *dropDB) String() string {
	if len(d.tables) == 0 {
		return ""
	}
	var w bytes.Buffer
	_, _ = w.WriteString("tables:[")
	for id, entry := range d.tables {
		_, _ = w.WriteString(fmt.Sprintf("table: %d, isdrop: %t", id, entry.drop))
		_, _ = w.WriteString(entry.String())
	}
	_, _ = w.WriteString("]")
	return w.String()
}
