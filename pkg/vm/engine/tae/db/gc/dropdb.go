package gc

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"

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
