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

package restrict

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
)

func New(prev op.OP, e extend.Extend) *Restrict {
	return &Restrict{
		E:    e,
		Prev: prev,
	}
}

func (n *Restrict) String() string {
	return fmt.Sprintf("%s -> σ(%s)", n.Prev, n.E)
}

func (n *Restrict) Name() string {
	return n.ID
}

func (n *Restrict) Rename(name string) {
	n.ID = name
}

func (n *Restrict) ResultColumns() []string {
	return n.Prev.ResultColumns()
}

func (n *Restrict) Attribute() map[string]types.Type {
	return n.Prev.Attribute()
}
