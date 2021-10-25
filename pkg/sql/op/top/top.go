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

package top

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/op"
	"github.com/matrixorigin/matrixone/pkg/sql/op/order"
)

func New(prev op.OP, limit int64, gs []order.Attribute) *Top {
	return &Top{
		Gs:    gs,
		Prev:  prev,
		Limit: limit,
	}
}

func (n *Top) String() string {
	r := fmt.Sprintf("%s -> τ([", n.Prev)
	for i, g := range n.Gs {
		switch i {
		case 0:
			r += fmt.Sprintf("%s", g.Name)
		default:
			r += fmt.Sprintf(", %s", g.Name)
		}
	}
	r += fmt.Sprintf("], %v)", n.Limit)
	return r
}

func (n *Top) Name() string {
	return n.ID
}

func (n *Top) Rename(name string) {
	n.ID = name
}

func (n *Top) ResultColumns() []string {
	return n.Prev.ResultColumns()
}

func (n *Top) SetColumns(cs []string) {
	n.Prev.SetColumns(cs)
}

func (n *Top) Attribute() map[string]types.Type {
	return n.Prev.Attribute()
}
