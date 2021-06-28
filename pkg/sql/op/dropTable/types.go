package dropTable

import "matrixone/pkg/vm/engine"

type DropTable struct {
	Flg bool // flg = true, indicates  no error is reported if table does not exist
	Dbs []string
	Ids []string
	E   engine.Engine
}
