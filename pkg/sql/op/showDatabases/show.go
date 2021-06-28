package showDatabases

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine"
)

func New(e engine.Engine) *ShowDatabases {
	return &ShowDatabases{e}
}

func (n *ShowDatabases) String() string {
	return "SHOW DATABASES"
}

func (n *ShowDatabases) Name() string                     { return "" }
func (n *ShowDatabases) Rename(_ string)                  {}
func (n *ShowDatabases) Columns() []string                { return nil }
func (n *ShowDatabases) Attribute() map[string]types.Type { return nil }
