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
