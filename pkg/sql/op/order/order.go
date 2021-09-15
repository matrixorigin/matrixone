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

package order

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
)

func New(prev op.OP, gs []Attribute) *Order {
	return &Order{
		Gs:   gs,
		Prev: prev,
	}
}

func (n *Order) String() string {
	r := fmt.Sprintf("%s -> τ([", n.Prev)
	for i, g := range n.Gs {
		switch i {
		case 0:
			r += fmt.Sprintf("%s", g.Name)
		default:
			r += fmt.Sprintf(", %s", g.Name)
		}
	}
	r += fmt.Sprintf("])")
	return r
}

func (n *Order) Name() string {
	return n.ID
}

func (n *Order) Rename(name string) {
	n.ID = name
}

func (n *Order) Columns() []string {
	return n.Prev.Columns()
}

func (n *Order) SetColumns(cs []string) {
	n.Prev.SetColumns(cs)
}

func (n *Order) Attribute() map[string]types.Type {
	return n.Prev.Attribute()
}
