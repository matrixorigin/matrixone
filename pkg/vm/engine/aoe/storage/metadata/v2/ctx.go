package metadata

type writeCtx struct {
	op      CtxT
	exIndex *LogIndex
}

type createTableCtx struct {
	writeCtx
	schema *Schema
	table  *Table
}

type dropTableCtx struct {
	writeCtx
	name  string
	table *Table
}

func newCreateTableCtx(schema *Schema, exIndex *LogIndex) *createTableCtx {
	return &createTableCtx{
		writeCtx: writeCtx{
			op:      ETCreateTable,
			exIndex: exIndex,
		},
		schema: schema,
	}
}

func newDropTableCtx(name string, exIndex *LogIndex) *dropTableCtx {
	return &dropTableCtx{
		writeCtx: writeCtx{
			op:      ETSoftDeleteTable,
			exIndex: exIndex,
		},
		name: name,
	}
}
