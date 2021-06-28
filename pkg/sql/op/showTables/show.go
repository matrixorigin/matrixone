package showTables

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine"
)

func New(db engine.Database) *ShowTables {
	return &ShowTables{db}
}

func (n *ShowTables) String() string {
	return "SHOW TABLES"
}

func (n *ShowTables) Name() string                     { return "" }
func (n *ShowTables) Rename(_ string)                  {}
func (n *ShowTables) Columns() []string                { return nil }
func (n *ShowTables) Attribute() map[string]types.Type { return nil }
