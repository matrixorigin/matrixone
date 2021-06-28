package createTable

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine"
)

func New(flg bool, id string, defs []engine.TableDef, pdef *engine.PartitionBy, db engine.Database) *CreateTable {
	return &CreateTable{
		Db:   db,
		Id:   id,
		Flg:  flg,
		Defs: defs,
		Pdef: pdef,
	}
}

func (n *CreateTable) String() string {
	if n.Flg {
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s", n.Id)
	}
	return fmt.Sprintf("CREATE TABLE %s", n.Id)
}

func (n *CreateTable) Name() string                     { return "" }
func (n *CreateTable) Rename(_ string)                  {}
func (n *CreateTable) Columns() []string                { return nil }
func (n *CreateTable) Attribute() map[string]types.Type { return nil }
