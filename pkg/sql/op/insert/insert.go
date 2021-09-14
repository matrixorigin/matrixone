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
