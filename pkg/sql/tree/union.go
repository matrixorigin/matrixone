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

//the UNION statement
type UnionClause struct {
	SelectStatement
	Type        UnionType
	//Left, Right *Select
	Left, Right SelectStatement
	All         bool
}

//set operations
type UnionType int

const (
	UNION UnionType = iota
	INTERSECT
	EXCEPT
)

var unionTypeName = []string{
	"UNION",
	"INTERSECT",
	"EXCEPT",
}

//func NewUnionClause(t UnionType,l,r *Select,a bool)*UnionClause{
func NewUnionClause(t UnionType,l,r SelectStatement,a bool)*UnionClause{
	return &UnionClause{
		Type:            t,
		Left:            l,
		Right:           r,
		All:             a,
	}
}