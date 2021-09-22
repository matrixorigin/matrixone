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
func (n *DropDatabase) ResultColumns() []string          { return nil }
func (n *DropDatabase) SetColumns(_ []string)            {}
func (n *DropDatabase) Attribute() map[string]types.Type { return nil }
