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

package product

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
)

func New(r, s op.OP) *Product {
	attrs := make(map[string]types.Type)
	{
		rname := r.Name()
		rattrs := r.Attribute()
		if len(rname) > 0 {
			for k, v := range rattrs {
				attrs[rname+"."+k] = v
			}
		} else {
			for k, v := range rattrs {
				attrs[k] = v
			}
		}
	}
	{
		sname := s.Name()
		sattrs := s.Attribute()
		if len(sname) > 0 {
			for k, v := range sattrs {
				attrs[sname+"."+k] = v
			}
		} else {
			for k, v := range sattrs {
				attrs[k] = v
			}
		}
	}
	return &Product{
		R:     r,
		S:     s,
		Attrs: attrs,
	}
}

func (n *Product) Name() string {
	return n.ID
}

func (n *Product) String() string {
	return fmt.Sprintf("(%s) ⨯  (%s)", n.R, n.S)
}

func (n *Product) Rename(name string) {
	n.ID = name
}

func (n *Product) ResultColumns() []string {
	return nil
}

func (n *Product) Attribute() map[string]types.Type {
	return n.Attrs
}
