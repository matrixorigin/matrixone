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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// compilePreInsert Compile PreInsert Node and set it as the root operator for each Scope.
func (c *Compile) compilePreInsert(ns []*plan.Node, n *plan.Node, ss []*Scope) ([]*Scope, error) {
	defer func() {
		c.anal.isFirst = false
	}()

	currentFirstFlag := c.anal.isFirst

	for i := range ss {
		preInsertArg, err := constructPreInsert(ns, n, c.e, c.proc)
		if err != nil {
			return nil, err
		}
		preInsertArg.SetIdx(c.anal.curNodeIdx)
		preInsertArg.SetIsFirst(currentFirstFlag)
		ss[i].setRootOperator(preInsertArg)
	}
	return ss, nil
}
