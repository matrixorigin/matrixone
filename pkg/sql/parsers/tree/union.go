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

import "fmt"

//the UNION statement
type UnionClause struct {
	SelectStatement
	Type UnionType
	//Left, Right *Select
	Left, Right SelectStatement
	All         bool
	Distinct    bool
}

func (node *UnionClause) Format(ctx *FmtCtx) {
	node.Left.Format(ctx)
	ctx.WriteByte(' ')
	ctx.WriteString(node.Type.String())
	if node.All {
		ctx.WriteString(" all")
	}
	if node.Distinct {
		ctx.WriteString(" distinct")
	}
	ctx.WriteByte(' ')
	node.Right.Format(ctx)
}

type UnionTypeRecord struct {
	Type     UnionType
	All      bool
	Distinct bool
}

//set operations
type UnionType int

const (
	UNION UnionType = iota
	INTERSECT
	EXCEPT
	UT_MINUS
)

var unionTypeName = [...]string{
	UNION:     "union",
	INTERSECT: "intersect",
	EXCEPT:    "except",
	UT_MINUS:  "minus",
}

func (i UnionType) String() string {
	if i < 0 || i > UnionType(len(unionTypeName)-1) {
		return fmt.Sprintf("UnionType(%d)", i)
	}
	return unionTypeName[i]
}

//func NewUnionClause(t UnionType,l,r *Select,a bool)*UnionClause{
func NewUnionClause(t UnionType, l, r SelectStatement, a bool) *UnionClause {
	return &UnionClause{
		Type:  t,
		Left:  l,
		Right: r,
		All:   a,
	}
}
