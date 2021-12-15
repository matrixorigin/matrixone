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

package dropIndex

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func New(ifNotExists bool, r engine.Relation, name string) *DropIndex {
	return &DropIndex{IfNotExists: ifNotExists, R: r, IndexName: name}
}

func (n *DropIndex) String() string {
	return "DROP INDEX"
}

func (n *DropIndex) Name() string                     { return "" }
func (n *DropIndex) Rename(_ string)                  {}
func (n *DropIndex) ResultColumns() []string          { return nil }
func (n *DropIndex) SetColumns(_ []string)            {}
func (n *DropIndex) Attribute() map[string]types.Type { return nil }
