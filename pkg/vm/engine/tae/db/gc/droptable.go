package gc

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type dropTable struct {
	tid    uint64
	drop   bool
	object map[string]*ObjectEntry
}

func NewDropTable(id uint64) *dropTable {
	return &dropTable{
		tid:    id,
		drop:   false,
		object: make(map[string]*ObjectEntry),
	}
}

func (d *dropTable) getObject(id common.ID, name string) *ObjectEntry {
	object := d.object[name]
	if object == nil {
		object = NewObjectEntry()
		object.table.tid = id.TableID
	}
	d.object[name] = object
	return object
}

func (d *dropTable) addBlock(id common.ID, name string) {
	object := d.getObject(id, name)
	object.AddBlock(id)
}
func (d *dropTable) deleteBlock(id common.ID, name string) {
	object := d.getObject(id, name)
	object.DelBlock(id)
}

func (d *dropTable) merge(dropTable *dropTable) {
	for name, entry := range dropTable.object {
		object := d.object[name]
		if object == nil {
			object = NewObjectEntry()
		}
		object.MergeEntry(*entry)
		d.object[name] = object
	}
}

func (d *dropTable) softGC() []string {
	gc := make([]string, 0)
	for name := range d.object {
		if d.object[name] == nil {
			panic(any("error"))
		}
		if d.drop {
			gc = append(gc, name)
			delete(d.object, name)
			return gc
		}
		if d.object[name].AllowGC() {
			gc = append(gc, name)
			delete(d.object, name)
		}
	}
	return gc
}

func (d *dropTable) String() string {
	if len(d.object) == 0 {
		return ""
	}
	var w bytes.Buffer
	_, _ = w.WriteString("object:[\n")
	for name, entry := range d.object {
		_, _ = w.WriteString(fmt.Sprintf("name: %v", name))
		_, _ = w.WriteString(entry.String())
	}
	_, _ = w.WriteString("]\n")
	return w.String()
}
