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
	"bytes"
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/plan"
)

type Node interface {
	fmt.Stringer
}

type Variable struct {
	Ref  int     // reference count
	Name string  // name of attribute
	Type types.T // type of attribute
}

type Relation struct {
	Vars    []string
	Rel     *plan.Relation
	VarsMap map[string]*Variable
}

type FNode struct {
	Root     Node
	Children []*FNode
}

type FTree struct {
	Roots    []*FNode
	FreeVars []string
	Qry      *plan.Query
}

type build struct {
}

func (v *Variable) String() string {
	return fmt.Sprintf("%s[%s] = %v", v.Name, v.Type, v.Ref)
}

func (r *Relation) String() string {
	return fmt.Sprintf("%s - %s", r.Rel.Alias, r.Vars)
}

func (f *FTree) String() string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("free variables: %v\n", f.FreeVars))
	for _, root := range f.Roots {
		buf.WriteString(root.format("\t"))
	}
	return buf.String()
}

func (n *FNode) format(prefix string) string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("%s%s\n", prefix, n.Root))
	for _, chd := range n.Children {
		buf.WriteString(chd.format(prefix + "\t"))
	}
	return buf.String()
}
