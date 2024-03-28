// Copyright 2021 - 2022 Matrix Origin
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

package tree

import "github.com/matrixorigin/matrixone/pkg/common/reuse"

func init() {
	reuse.CreatePool[Execute](
		func() *Execute { return &Execute{} },
		func(e *Execute) { e.reset() },
		reuse.DefaultOptions[Execute](),
	)
}

type Execute struct {
	Statement
	Name      Identifier
	Variables []*VarExpr
}

func (node *Execute) Format(ctx *FmtCtx) {
	ctx.WriteString("execute ")
	node.Name.Format(ctx)
	if len(node.Variables) > 0 {
		ctx.WriteString(" using ")
		for i, varExpr := range node.Variables {
			if i > 0 {
				ctx.WriteString(",")
			}
			varExpr.Format(ctx)
		}
	}
}

func (node *Execute) Free() {
	reuse.Free[Execute](node, nil)
}

func (node *Execute) GetStatementType() string { return "Execute" }

func (node *Execute) GetQueryType() string { return QueryTypeOth }

func (node Execute) TypeName() string { return "tree.Execute" }

func NewExecute(name Identifier) *Execute {
	e := reuse.Alloc[Execute](nil)
	e.Name = name
	return e
}

func NewExecuteWithVariables(name Identifier, variables []*VarExpr) *Execute {
	e := reuse.Alloc[Execute](nil)
	e.Name = name
	e.Variables = variables
	return e
}

func (node *Execute) reset() {
	// if node.Variables != nil {
	// for _, item := range node.Variables {
	// switch item.(type) {
	// case *IntVal:
	// }
	// }
	*node = Execute{}
}
