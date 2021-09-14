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

package relation

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine"
)

func New(s bool, name, schema string, r engine.Relation) *Relation {
	var us []*Unit
	var cols []string

	attrs := make(map[string]types.Type)
	{
		attrDefs := r.Attribute()
		for _, attr := range attrDefs {
			attrs[attr.Name] = attr.Type
			cols = append(cols, attr.Name)
		}
	}
	{
		segs := r.Segments()
		defer r.Close()
		mp := make(map[string]*Unit)
		for _, seg := range segs {
			if u, ok := mp[seg.Node.Addr]; ok {
				u.Segs = append(u.Segs, &Segment{
					IsRemote: false,
					Id:       seg.Id,
					Node:     seg.Node,
					Version:  seg.Version,
					GroupId:  seg.GroupId,
					TabletId: seg.TabletId,
				})
			} else {
				mp[seg.Node.Addr] = &Unit{[]*Segment{&Segment{
					IsRemote: false,
					Id:       seg.Id,
					Node:     seg.Node,
					Version:  seg.Version,
					GroupId:  seg.GroupId,
					TabletId: seg.TabletId,
				}}, seg.Node}
			}
		}
		for _, u := range mp {
			us = append(us, u)
		}
	}
	return &Relation{
		S:     s,
		R:     r,
		Us:    us,
		Cols:  cols,
		Rid:   name,
		ID:    name,
		Attrs: attrs,
		DB:    schema,
	}
}

func (n *Relation) Name() string {
	return n.ID
}

func (n *Relation) Rename(name string) {
	n.ID = name
}

func (n *Relation) String() string {
	if n.S {
		return n.ID
	}
	return fmt.Sprintf("%s.%s", n.DB, n.Rid)
}

func (n *Relation) Columns() []string {
	return n.Cols
}

func (n *Relation) Attribute() map[string]types.Type {
	return n.Attrs
}
