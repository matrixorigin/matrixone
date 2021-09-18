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
func (n *CreateDatabase) ResultColumns() []string          { return nil }
func (n *CreateDatabase) Attribute() map[string]types.Type { return nil }
