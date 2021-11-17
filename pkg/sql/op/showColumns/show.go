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

package showColumns

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func New(R engine.Relation, like []byte) *ShowColumns {
	return &ShowColumns{R, like}
}

func (n *ShowColumns) String() string {
	return "SHOW COLUMNS"
}

func (n *ShowColumns) Name() string                     { return "" }
func (n *ShowColumns) Rename(_ string)                  {}
func (n *ShowColumns) ResultColumns() []string          { return nil }
func (n *ShowColumns) SetColumns(_ []string)            {}
func (n *ShowColumns) Attribute() map[string]types.Type { return nil }