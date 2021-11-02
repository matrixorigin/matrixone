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
	"fmt"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/errors"
	"matrixone/pkg/sql/plan"
)

// simply pick one, and do not pursue the best
func selectRoot(qry *plan.Query) (string, error) {
	var cnt int
	var rel string

	if len(qry.Conds) == 0 {
		return "", errors.New(errno.SQLStatementNotYetComplete, "Product not support now")
	}
	mp := make(map[string]int)
	for _, cond := range qry.Conds {
		if cond.R == cond.S {
			return "", errors.New(errno.SQLStatementNotYetComplete, "Self join not support now")
		}
		mp[cond.R]++
		mp[cond.S]++
	}
	for k, v := range mp {
		if v > cnt {
			rel = k
			cnt = v
		}
	}
	return rel, nil
}

func buildNodes(rel *plan.Relation, qry *plan.Query) ([]*Fnode, error) {
	conds, err := getConditions(rel.Alias, qry)
	if err != nil {
		return nil, err
	}
	fs := buildPath(rel.Alias, qry, conds)
	if len(conds) > 0 {
		for i := range fs {
			if err := fs[i].AddChildren(conds, qry); err != nil {
				return nil, err
			}
		}
	}
	return fs, nil
}

func (n *Fnode) AddChildren(conds []*plan.JoinCondition, qry *plan.Query) error {
	rns := getRelationFromConditions(n.Root, conds)
	if len(rns) == 0 {
		return nil
	}
	if len(rns) > 1 {
		return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("multi-relation join on same key not support now"))
	}
	chd, err := buildNodes(qry.RelsMap[rns[0]], qry)
	if err != nil {
		return err
	}
	n.Children = chd
	return nil
}
