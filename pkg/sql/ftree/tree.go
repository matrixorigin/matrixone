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

package ftree

import (
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// exclude some queries that are not currently supported
func (f *FTree) check(qry *plan.Query) error {
	mp := make(map[string]uint8)
	{
		for _, rel := range qry.Rels {
			mp[rel] = 0
		}
	}
	for _, root := range f.Roots {
		root.check(mp)
	}
	for _, v := range mp {
		if v == 0 {
			return errors.New(errno.SQLStatementNotYetComplete, "Product not support now")
		}
		if v > 1 {
			return errors.New(errno.SQLStatementNotYetComplete, "Triangle query not support now")
		}
	}
	return nil
}

func (n *FNode) check(mp map[string]uint8) {
	if rn, ok := n.Root.(*Relation); ok {
		mp[rn.Rel.Alias]++
	}
	for _, chd := range n.Children {
		chd.check(mp)
	}
}
