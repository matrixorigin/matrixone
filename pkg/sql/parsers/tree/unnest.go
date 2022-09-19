// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

var (
	// use this to avoid the error: gob: type not registered for interface: tree.UnresolvedName
	registered = false
)

type UnnestParam struct {
	Origin interface{}
	Path   string
	Outer  bool
}

type Unnest struct {
	statementImpl
	Param *UnnestParam
}

func (node *Unnest) Format(ctx *FmtCtx) {
	ctx.WriteString("unnest(")
	ctx.WriteString(node.getOrigin())
	ctx.WriteString(", ")
	ctx.WriteString(node.Param.Path)
	ctx.WriteString(", ")
	ctx.WriteString(fmt.Sprintf("%v", node.Param.Outer))
	ctx.WriteByte(')')
}

func (node Unnest) String() string {
	originStr := node.getOrigin()
	return fmt.Sprintf("unnest(%s, %s, %v)", originStr, node.Param.Path, node.Param.Outer)
}

func (node Unnest) getOrigin() string {
	switch node.Param.Origin.(type) {
	case string:
		return fmt.Sprintf("%s", node.Param.Origin)
	case *UnresolvedName:
		dbName, tableName, colName := node.Param.Origin.(*UnresolvedName).GetNames()
		if len(dbName) > 0 {
			return fmt.Sprintf("%s.%s.%s", dbName, tableName, colName)
		} else if len(tableName) > 0 {
			return fmt.Sprintf("%s.%s", tableName, colName)
		} else {
			return colName
		}
	default:
		panic("unknown type")
	}
}

func (p UnnestParam) maybeRegister() {
	if !registered {
		switch p.Origin.(type) {
		case string:
			break
		case *UnresolvedName:
			gob.Register(p.Origin)
			registered = true
		}
	}
}

func (p UnnestParam) Marshal() ([]byte, error) {
	p.maybeRegister()
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(p)
	return buf.Bytes(), err
}
func (p *UnnestParam) Unmarshal(data []byte) error {
	p.maybeRegister()
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	return dec.Decode(p)
}
