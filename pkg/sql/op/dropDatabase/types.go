package dropDatabase

import "matrixone/pkg/vm/engine"

type DropDatabase struct {
	Flg bool // flg = true, indicates  no error is reported if database does not exist
	Id  string
	E   engine.Engine
}
