// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

import (
	"strconv"
	"strings"
)

// TableOptionTTL is a table-level expiration policy. Remove is ALTER-only.
type TableOptionTTL struct {
	tableOptionImpl
	Column Identifier
	Amount uint64
	Unit   string
	Remove bool
}

func (node *TableOptionTTL) Format(ctx *FmtCtx) {
	if node.Remove {
		ctx.WriteString("remove ttl")
		return
	}
	ctx.WriteString("ttl ")
	ctx.WriteString("`" + strings.ReplaceAll(string(node.Column), "`", "``") + "`")
	ctx.WriteString(" + interval ")
	ctx.WriteString(strconv.FormatUint(node.Amount, 10))
	ctx.WriteByte(' ')
	ctx.WriteString(strings.ToLower(node.Unit))
	ctx.WriteString(" delete")
}
func (node TableOptionTTL) TypeName() string { return "tree.TableOptionTTL" }
func (node *TableOptionTTL) Free()           {}
