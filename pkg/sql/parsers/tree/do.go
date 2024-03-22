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

package tree

import "github.com/matrixorigin/matrixone/pkg/common/reuse"

func init() {
	reuse.CreatePool[Do](
		func() *Do { return &Do{} },
		func(d *Do) { d.reset() },
		reuse.DefaultOptions[Do](),
	)
}

// Do statement
type Do struct {
	statementImpl
	Exprs []Expr
}

func NewDo(e []Expr) *Do {
	do := reuse.Alloc[Do](nil)
	do.Exprs = e
	return do
}

func (node *Do) Format(ctx *FmtCtx) {
	ctx.WriteString("do ")
	for _, e := range node.Exprs {
		e.Format(ctx)
	}
}

func (node *Do) Free() {
	reuse.Free[Do](node, nil)
}

func (node *Do) GetStatementType() string { return "Do" }

func (node *Do) GetQueryType() string { return QueryTypeOth }

func (node Do) TypeName() string { return "tree.Do" }

func (node *Do) reset() {
	if node.Exprs != nil {
		// for _, item := range node.Exprs {
		// switch item.(type) {
		// case *IntVal:
		// }
	}
	*node = Do{}
}
