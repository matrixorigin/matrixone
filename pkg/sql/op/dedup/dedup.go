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

package dedup

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/op"
)

func New(prev op.OP, gs []*extend.Attribute) *Dedup {
	cs := make([]string, 0, len(gs))
	attrs := make(map[string]types.Type)
	{
		for _, g := range gs {
			cs = append(cs, g.Name)
			attrs[g.Name] = g.Type.ToType()
		}
	}
	return &Dedup{
		Rs:    cs,
		Cs:    cs,
		Gs:    gs,
		Prev:  prev,
		Attrs: attrs,
	}
}

func (n *Dedup) String() string {
	r := fmt.Sprintf("%s -> δ([", n.Prev)
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

func (n *Dedup) Name() string {
	return n.ID
}

func (n *Dedup) Rename(name string) {
	n.ID = name
}

func (n *Dedup) SetColumns(cs []string) {
	n.Rs = cs
}

func (n *Dedup) ResultColumns() []string {
	return n.Rs
}

func (n *Dedup) Attribute() map[string]types.Type {
	return n.Attrs
}
