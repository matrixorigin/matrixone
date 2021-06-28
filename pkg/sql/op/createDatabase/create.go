package createDatabase

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine"
)

func New(flg bool, id string, e engine.Engine) *CreateDatabase {
	return &CreateDatabase{
		E:   e,
		Id:  id,
		Flg: flg,
	}
}

func (n *CreateDatabase) String() string {
	if n.Flg {
		return fmt.Sprintf("CREATE DATABSE IF NOT EXISTS %s", n.Id)
	}
	return fmt.Sprintf("CREATE DATABSE %s", n.Id)
}

func (n *CreateDatabase) Name() string                     { return "" }
func (n *CreateDatabase) Rename(_ string)                  {}
func (n *CreateDatabase) Columns() []string                { return nil }
func (n *CreateDatabase) Attribute() map[string]types.Type { return nil }
