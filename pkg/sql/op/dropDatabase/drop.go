package dropDatabase

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine"
)

func New(flg bool, id string, e engine.Engine) *DropDatabase {
	return &DropDatabase{
		E:   e,
		Id:  id,
		Flg: flg,
	}
}

func (n *DropDatabase) String() string {
	if n.Flg {
		return fmt.Sprintf("DROP DATABSE IF EXISTS %s", n.Id)
	}
	return fmt.Sprintf("DROP DATABSE %s", n.Id)
}

func (n *DropDatabase) Name() string                     { return "" }
func (n *DropDatabase) Rename(_ string)                  {}
func (n *DropDatabase) Columns() []string                { return nil }
func (n *DropDatabase) Attribute() map[string]types.Type { return nil }
