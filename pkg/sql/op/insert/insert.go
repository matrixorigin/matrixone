package insert

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine"
)

func New(id, db string, bat *batch.Batch, r engine.Relation) *Insert {
	return &Insert{
		R:   r,
		ID:  id,
		DB:  db,
		Bat: bat,
	}
}

func (n *Insert) String() string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("INSERT INTO %s.%s\n", n.DB, n.ID))
	buf.WriteString(fmt.Sprintf("%v\n", n.Bat))
	return buf.String()
}

func (n *Insert) Name() string                     { return "" }
func (n *Insert) Rename(_ string)                  {}
func (n *Insert) Columns() []string                { return nil }
func (n *Insert) Attribute() map[string]types.Type { return nil }
