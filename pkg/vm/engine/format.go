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

package engine

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"strconv"
	"strings"
)

func (node *AttributeDef) Format(buf *bytes.Buffer) {
	node.Attr.Format(buf)
}

func (node *Attribute) Format(buf *bytes.Buffer) {
	buf.WriteString("`")
	buf.WriteString(node.Name)
	buf.WriteString("`")

	buf.WriteByte(' ')
	buf.WriteString(node.Type.String())

	if node.Type.Width > 0 && node.Type.Precision > 0 {
		buf.WriteString("(")
		str := fmt.Sprintf("%d", node.Type.Width)
		buf.WriteString(str)
		buf.WriteString(", ")
		str = fmt.Sprintf("%d", node.Type.Precision)
		buf.WriteByte(')')
	} else if node.Type.Width > 0 {
		buf.WriteString("(")
		str := fmt.Sprintf("%d", node.Type.Width)
		buf.WriteString(str)
		buf.WriteByte(')')
	}

	val := makeVal2Str(node.Type, node.Default.Value, node.Default.Exist)
	if val != "" {
		buf.WriteString(" DEFAULT ")
		buf.WriteString(val)
	}
}

func (node *PrimaryIndexDef) Format(buf *bytes.Buffer) {
	buf.WriteString("PRIMARY KEY")
	prefix := " ("
	for _, n := range node.Names {
		buf.WriteString(prefix)
		buf.WriteString("`")
		buf.WriteString(n)
		buf.WriteString("`")
		prefix = ", "
	}
	buf.WriteString(")")
}

func (node *IndexTableDef) Format(buf *bytes.Buffer) {
	buf.WriteString("KEY")
	buf.WriteString(" `")
	buf.WriteString(node.Name)
	buf.WriteString("`")

	prefix := " ("
	for _, c := range node.ColNames {
		buf.WriteString(prefix)
		buf.WriteString("`")
		buf.WriteString(c)
		buf.WriteString("`")
		prefix = ", "
	}
	buf.WriteString(")")

	buf.WriteString(" USING ")
	typ := tree.IndexType(node.Typ)
	buf.WriteString(strings.ToUpper(typ.ToString()))
}

func makeVal2Str(typ types.Type, value interface{}, isNull bool) string {
	if isNull {
		return "NULL"
	}
	switch typ.Oid {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		res := value.(int64)
		str := strconv.FormatInt(res, 10)
		return str
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		res := value.(uint64)
		str := strconv.FormatUint(res, 10)
		return str
	case types.T_float32, types.T_float64:
		res := value.(float64)
		str := strconv.FormatFloat(res, 'f', 10, 64)
		return str
	case types.T_char, types.T_varchar:
		res := value.(string)
		return res
	case types.T_date:
		res := value.(types.Date).String()
		return res
	}
	return "NULL"
}