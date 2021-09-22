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

package naturalJoin

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
)

func New(r, s op.OP) *Join {
	pub := make([]string, 0, 1)
	attrs := make(map[string]types.Type)
	{
		rattrs := r.Attribute()
		for k, v := range rattrs {
			attrs[k] = v
		}
	}
	{
		sattrs := s.Attribute()
		for k, v := range sattrs {
			if _, ok := attrs[k]; !ok {
				attrs[k] = v
			} else {
				pub = append(pub, k)
			}
		}
	}
	return &Join{
		R:     r,
		S:     s,
		Pub:   pub,
		Attrs: attrs,
	}
}

func (n *Join) String() string {
	return fmt.Sprintf("(%s) ⨝ (%s)", n.R, n.S)
}

func (n *Join) Name() string {
	return n.ID
}

func (n *Join) Rename(name string) {
	n.ID = name
}

func (n *Join) ResultColumns() []string {
	return nil
}

func (n *Join) SetColumns(_ []string) {
}

func (n *Join) Attribute() map[string]types.Type {
	return n.Attrs
}
