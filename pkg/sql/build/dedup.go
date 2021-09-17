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

package build

import (
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/dedup"
)

func (b *build) buildDedup(o op.OP) (op.OP, error) {
	var gs []*extend.Attribute

	attrs := o.Columns()
	mp := o.Attribute()
	for _, attr := range attrs {
		gs = append(gs, &extend.Attribute{
			Name: attr,
			Type: mp[attr].Oid,
		})

	}
	return dedup.New(o, gs), nil

}
