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

func getConditions(rn string, qry *plan.Query) ([]*plan.JoinCondition, error) {
	var conds []*plan.JoinCondition

	for i := 0; i < len(qry.Conds); i++ {
		cond := qry.Conds[i]
		if cond.R == cond.S {
			return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("self join not support now"))
		}
		if cond.R == rn {
			conds = append(conds, cond)
			qry.Conds = append(qry.Conds[:i], qry.Conds[i+1:]...)
			i--
		} else if cond.S == rn {
			cond.R, cond.S = cond.S, cond.R
			cond.Rattr, cond.Sattr = cond.Sattr, cond.Rattr
			conds = append(conds, cond)
			qry.Conds = append(qry.Conds[:i], qry.Conds[i+1:]...)
			i--
		}
	}
	return fusionJoinConditions(conds)
}

func fusionJoinConditions(conds []*plan.JoinCondition) ([]*plan.JoinCondition, error) {
	mp := make(map[string]int)
	for _, cond := range conds {
		mp[cond.S]++
	}
	for _, v := range mp {
		if v > 1 {
			return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("multi-attribute join not support now"))
		}
	}
	return conds, nil
}

func getRelationFromConditions(root Node, conds []*plan.JoinCondition) ([]string, []string) {
	var rns []string
	var attrs []string

	v, ok := root.(*Variable)
	if !ok {
		return rns, attrs
	}
	for _, cond := range conds {
		if cond.Rattr == v.Name {
			rns = append(rns, cond.S)
			attrs = append(attrs, cond.Sattr)
		}
	}
	return rns, attrs
}
