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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
)

func (b *build) buildRename(child *Scope, attrs, aliases []string, ts []types.Type) *Scope {
	s := &Scope{
		Children: []*Scope{child},
	}
	op := &Projection{}
	{ // construct result
		s.Result.AttrsMap = make(map[string]*Attribute)
		for i, attr := range attrs {
			{ // construct operator
				op.Rs = append(op.Rs, 0)
				op.As = append(op.As, aliases[i])
				op.Es = append(op.Es, &extend.Attribute{
					Name: attr,
					Type: ts[i].Oid,
				})
			}
			s.Result.Attrs = append(s.Result.Attrs, aliases[i])
			s.Result.AttrsMap[aliases[i]] = &Attribute{
				Type: ts[i],
				Name: aliases[i],
			}
		}
	}
	s.Op = op
	return s
}
