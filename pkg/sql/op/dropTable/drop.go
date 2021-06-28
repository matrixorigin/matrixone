package dropTable

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine"
)

func New(flg bool, dbs, ids []string, e engine.Engine) *DropTable {
	return &DropTable{
		E:   e,
		Dbs: dbs,
		Ids: ids,
		Flg: flg,
	}
}

func (n *DropTable) String() string {
	var buf bytes.Buffer

	buf.WriteString("DROP TABLE ")
	if n.Flg {
		buf.WriteString("IF EXISTS ")
	}
	for i, db := range n.Dbs {
		if i > 0 {
			buf.WriteString(fmt.Sprintf(",%s.%s", db, n.Ids[i]))
		} else {
			buf.WriteString(fmt.Sprintf("%s.%s", db, n.Ids[i]))
		}
	}
	return buf.String()
}

func (n *DropTable) Name() string                     { return "" }
func (n *DropTable) Rename(_ string)                  {}
func (n *DropTable) Columns() []string                { return nil }
func (n *DropTable) Attribute() map[string]types.Type { return nil }
