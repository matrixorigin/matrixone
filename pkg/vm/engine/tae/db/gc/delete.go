package gc

import "context"

func ExecDelete(table *GcEntry) error {
	if len(table.delete) == 0 {
		return nil
	}

	for name, ids := range table.delete {
		blocks := table.table[name]
		if blocks == nil {
			panic(any("error"))
		}
		if len(blocks) == len(ids) {
			err := table.fs.DelFile(context.Background(), name)
			if err != nil {
				return err
			}
			delete(table.table, name)
			delete(table.delete, name)
		}
	}

	return nil
}
