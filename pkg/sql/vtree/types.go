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

package vtree

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/transform"
)

type Variable struct {
	Ref  int     // reference count
	Name string  // name of variable
	Type types.T // type of variable
}

type Relation struct {
	Alias  string
	Name   string // table name
	Schema string // schema name
	Vars   []*Variable
}

type View struct {
	Name     string // name of view
	Children []*View
	FreeVars []string
	Var      *Variable
	Rel      *Relation
	Arg      *transform.Argument // arguments for constructing a view
}

type ViewTree struct {
	Views           []*View
	FreeVars        []string
	ResultVariables []*Variable
	Top             *top.Argument
	Order           *order.Argument
	Dedup           *dedup.Argument
	Limit           *limit.Argument
	Offset          *offset.Argument
	Restrict        *restrict.Argument
	Projection      *projection.Argument
}

type node struct {
	rns         []string // relation name list
	children    []*node
	freeVars    []string
	boundVars   []string
	content     interface{} // ftree.Relation or ftree.Variable
	freeVarsMap map[string]uint8
}

type build struct {
}

type path struct {
	freeVars    []string
	freeVarsMap map[string]uint8
}

func (vt *ViewTree) IsBare() bool {
	if len(vt.FreeVars) > 0 {
		return false
	}
	return isBare(vt.Views)
}

func (vt *ViewTree) String() string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("result variables: %v\n", vt.ResultVariables))
	buf.WriteString("View Tree -> ")
	if vt.Restrict != nil {
		restrict.String(vt.Restrict, &buf)
		buf.WriteString(" -> ")
	}
	if vt.Projection != nil {
		projection.String(vt.Projection, &buf)
		buf.WriteString(" -> ")
	}
	if vt.Dedup != nil {
		dedup.String(vt.Dedup, &buf)
		buf.WriteString(" -> ")
	}
	if vt.Top != nil {
		top.String(vt.Top, &buf)
		buf.WriteString(" -> ")
	}
	if vt.Order != nil {
		order.String(vt.Order, &buf)
		buf.WriteString(" -> ")
	}
	if vt.Offset != nil {
		offset.String(vt.Offset, &buf)
		buf.WriteString(" -> ")
	}
	if vt.Limit != nil {
		limit.String(vt.Limit, &buf)
		buf.WriteString(" -> ")
	}
	buf.WriteString(fmt.Sprintf("output\nView Tree: %v\n", vt.FreeVars))
	for _, v := range vt.Views {
		buf.WriteString(v.format("\t"))
	}
	return buf.String()
}

func (v *View) String() string {
	var buf bytes.Buffer

	if v.Rel != nil {
		buf.WriteString(fmt.Sprintf("V{%s}<%v> <- ", v.Name, v.FreeVars))
		buf.WriteString(fmt.Sprintf("(%s <- %s.%s[%v]) -> ", v.Rel.Alias, v.Rel.Schema, v.Rel.Name, v.Rel.Vars))
		transform.String(v.Arg, &buf)
	} else {
		buf.WriteString(fmt.Sprintf("V{%s}[%s]<%v>", v.Name, v.Var.Name, v.FreeVars))
	}
	return buf.String()
}

func (v *View) format(prefix string) string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("%s%s\n", prefix, v))
	for _, chd := range v.Children {
		buf.WriteString(chd.format(prefix + "\t"))
	}
	return buf.String()
}

func (v *Variable) String() string {
	if v.Type == 0 {
		return fmt.Sprintf("%s:%v", v.Name, v.Ref)
	} else {
		return fmt.Sprintf("%s:%v:%v", v.Name, v.Ref, v.Type)
	}
}
