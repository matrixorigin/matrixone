// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
func (n *DropTable) ResultColumns() []string           { return nil }
func (n *DropTable) SetColumns(_ []string)            {}
func (n *DropTable) Attribute() map[string]types.Type { return nil }
