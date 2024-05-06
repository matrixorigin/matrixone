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
	reuse.CreatePool[Declare](
		func() *Declare { return &Declare{} },
		func(d *Declare) { d.reset() },
		reuse.DefaultOptions[Declare](),
	)
}

// Declare statement
type Declare struct {
	statementImpl
	Variables  []string
	ColumnType *T
	DefaultVal Expr
}

func (node *Declare) Format(ctx *FmtCtx) {
	ctx.WriteString("declare ")
	for _, v := range node.Variables {
		ctx.WriteString(v + " ")
	}
	node.ColumnType.InternalType.Format(ctx)
	ctx.WriteString(" default ")
	node.DefaultVal.Format(ctx)
}

func NewDeclare(v []string, t *T, d Expr) *Declare {
	de := reuse.Alloc[Declare](nil)
	de.Variables = v
	de.ColumnType = t
	de.DefaultVal = d
	return de
}

func (node *Declare) GetStatementType() string { return "Declare" }

func (node *Declare) GetQueryType() string { return QueryTypeOth }

func (node Declare) TypeName() string { return "tree.Declare" }

func (node *Declare) Free() {
	reuse.Free[Declare](node, nil)
}

func (node *Declare) reset() {
	// if node.ColumnType != nil {
	// 	reuse.Free[T](node.ColumnType, nil)
	// }
	// if node.DefaultVal != nil {
	// switch node.DefaultVal.(type) {
	// case *IntVal:
	// }
	// }
	*node = Declare{}
}
