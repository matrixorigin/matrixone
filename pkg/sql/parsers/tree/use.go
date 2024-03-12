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
	reuse.CreatePool[Use](
		func() *Use { return &Use{} },
		func(u *Use) { u.reset() },
		reuse.DefaultOptions[Use](), //.
	) //WithEnableChecker()
}

type SecondaryRoleType int

const (
	SecondaryRoleTypeAll SecondaryRoleType = iota
	SecondaryRoleTypeNone
)

// Use statement
type Use struct {
	statementImpl
	Name              *CStr
	SecondaryRole     bool
	SecondaryRoleType SecondaryRoleType
	Role              *Role
}

func NewUse(name *CStr, secondaryRole bool, secondaryRoleType SecondaryRoleType, role *Role) *Use {
	use := reuse.Alloc[Use](nil)
	use.Name = name
	use.SecondaryRole = secondaryRole
	use.SecondaryRoleType = secondaryRoleType
	use.Role = role
	return use
}

func (node *Use) Format(ctx *FmtCtx) {
	ctx.WriteString("use")
	if !node.SecondaryRole {
		if node.Role != nil {
			ctx.WriteString(" role ")
			node.Role.Format(ctx)
		} else if node.Name != nil && !node.Name.Empty() {
			ctx.WriteByte(' ')
			ctx.WriteString(node.Name.ToLower())
		}
	} else {
		ctx.WriteString(" secondary role ")
		switch node.SecondaryRoleType {
		case SecondaryRoleTypeAll:
			ctx.WriteString("all")
		case SecondaryRoleTypeNone:
			ctx.WriteString("none")
		}
	}
}

func (node *Use) GetStatementType() string { return "Use" }
func (node *Use) GetQueryType() string     { return QueryTypeOth }

// IsUseRole checks the statement is:
//
//	USE SECONDARY ROLE { ALL | NONE };
//	USE ROLE role;
func (node *Use) IsUseRole() bool {
	return node.SecondaryRole || node.Role != nil
}

func (node *Use) Free() {
	reuse.Free[Use](node, nil)
}

func (node *Use) reset() {
	// if node.Name != nil {
	// node.Free()
	// }
	if node.Role != nil {
		node.Role.Free()
	}
	*node = Use{}
}

func (node Use) TypeName() string { return "tree.Use" }
