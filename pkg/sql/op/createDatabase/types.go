package createDatabase

import "matrixone/pkg/vm/engine"

type CreateDatabase struct {
	Flg bool // flg = true, indicates no error is reported if database exists
	Id  string
	E   engine.Engine
}
