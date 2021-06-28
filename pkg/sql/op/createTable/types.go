package createTable

import "matrixone/pkg/vm/engine"

type CreateTable struct {
	Flg  bool // flg = true, indicates no error is reported if table exists
	Id   string
	Db   engine.Database
	Defs []engine.TableDef
	Pdef *engine.PartitionBy
}
