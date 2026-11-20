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

import "strconv"

// StoredEnumValue transfers an already-stored ordinal, including error member
// zero, between equal ENUM domains. Only internal Go callers construct it;
// ordinary SQL numeric literals retain strict ENUM assignment semantics.
// EnumValues must describe the source column, not just the destination.
type StoredEnumValue struct {
	Ordinal       uint16
	EnumValues    string
	EnumCollation uint32
}

func (node *StoredEnumValue) String() string {
	return strconv.FormatUint(uint64(node.Ordinal), 10)
}

func (node *StoredEnumValue) Format(ctx *FmtCtx) {
	ctx.WriteString(node.String())
}

func (node *StoredEnumValue) Accept(v Visitor) (Expr, bool) {
	nodeExpr, _ := v.Enter(node)
	return v.Exit(nodeExpr)
}
