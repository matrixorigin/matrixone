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
)

func (def *AttributeDef) Format(buf *bytes.Buffer) {
	def.Attr.Format(buf)
}

func (def *Attribute) Format(buf *bytes.Buffer) {
	buf.WriteString("`")
	buf.WriteString(def.Name)
	buf.WriteString("`")

	buf.WriteByte(' ')
	buf.WriteString(def.Type.String())

	if def.Type.Width > 0 && def.Type.Scale > 0 {
		buf.WriteString("(")
		str := fmt.Sprintf("%d", def.Type.Width)
		buf.WriteString(str)
		buf.WriteString(", ")
		buf.WriteByte(')')
	} else if def.Type.Width > 0 {
		buf.WriteString("(")
		str := fmt.Sprintf("%d", def.Type.Width)
		buf.WriteString(str)
		buf.WriteByte(')')
	}
	if def.Default.NullAbility {
		buf.WriteString(" NULL ")
	}
	val := def.Default.Expr.String()
	if val != "" {
		buf.WriteString(" DEFAULT ")
		buf.WriteString(val)
	}
}

func (def *IndexTableDef) Format(buf *bytes.Buffer) {
	buf.WriteString("KEY")
	buf.WriteString(" `")
	buf.WriteString(def.Name)
	buf.WriteString("`")

	prefix := " ("
	for _, c := range def.ColNames {
		buf.WriteString(prefix)
		buf.WriteString("`")
		buf.WriteString(c)
		buf.WriteString("`")
		prefix = ", "
	}
	buf.WriteString(")")

	buf.WriteString(" USING ")
	buf.WriteString(def.Typ.ToString())
}
