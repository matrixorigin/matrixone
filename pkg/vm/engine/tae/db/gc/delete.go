package gc

import "context"

type GcTask struct {
	object []string
	table  *GcTable
}

func NewGcTask(table *GcTable) GcTask {
	return GcTask{
		object: make([]string, 0),
		table:  table,
	}
}

func (g *GcTask) ExecDelete() error {
	if len(g.table.delete) == 0 {
		return nil
	}

	for name, ids := range g.table.delete {
		blocks := g.table.table[name]
		if blocks == nil {
			panic(any("error"))
		}
		if len(blocks) == len(ids) {
			err := g.table.fs.DelFile(context.Background(), name)
			if err != nil {
				return err
			}
			delete(g.table.table, name)
			delete(g.table.delete, name)
		}
	}

	return nil
}
