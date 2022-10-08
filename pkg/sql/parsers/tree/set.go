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

type SetVar struct {
	statementImpl
	Assignments []*VarAssignmentExpr
}

func (node *SetVar) Format(ctx *FmtCtx) {
	ctx.WriteString("set")
	if node.Assignments != nil {
		prefix := " "
		for _, a := range node.Assignments {
			ctx.WriteString(prefix)
			a.Format(ctx)
			prefix = ", "
		}
	}
}

func NewSetVar(a []*VarAssignmentExpr) *SetVar {
	return &SetVar{
		Assignments: a,
	}
}

// for variable = expr
type VarAssignmentExpr struct {
	NodeFormatter
	System   bool
	Global   bool
	Name     string
	Value    Expr
	Reserved Expr
}

func (node *VarAssignmentExpr) Format(ctx *FmtCtx) {
	if node.Global {
		ctx.WriteString("global ")
	}
	ctx.WriteString(node.Name)
	ctx.WriteString(" =")
	if node.Value != nil {
		ctx.WriteByte(' ')
		node.Value.Format(ctx)
	}
	if node.Reserved != nil {
		ctx.WriteByte(' ')
		node.Reserved.Format(ctx)
	}
}

func NewVarAssignmentExpr(s bool, g bool, n string, v Expr, r Expr) *VarAssignmentExpr {
	return &VarAssignmentExpr{
		System:   s,
		Global:   g,
		Name:     n,
		Value:    v,
		Reserved: r,
	}
}

type SetDefaultRoleType int

const (
	SET_DEFAULT_ROLE_TYPE_NONE SetDefaultRoleType = iota
	SET_DEFAULT_ROLE_TYPE_ALL
	SET_DEFAULT_ROLE_TYPE_NORMAL
)

type SetDefaultRole struct {
	statementImpl
	Type  SetDefaultRoleType
	Roles []*Role
	Users []*User
}

func (node *SetDefaultRole) Format(ctx *FmtCtx) {
	ctx.WriteString("set default role")
	switch node.Type {
	case SET_DEFAULT_ROLE_TYPE_NONE:
		ctx.WriteString(" none")
	case SET_DEFAULT_ROLE_TYPE_ALL:
		ctx.WriteString(" all")
	case SET_DEFAULT_ROLE_TYPE_NORMAL:
		prefix := " "
		for _, r := range node.Roles {
			ctx.WriteString(prefix)
			r.Format(ctx)
			prefix = ", "
		}
	}
	ctx.WriteString(" to")
	prefix := " "
	for _, u := range node.Users {
		ctx.WriteString(prefix)
		u.Format(ctx)
		prefix = ", "
	}
}

func NewSetDefaultRole(t SetDefaultRoleType, r []*Role, u []*User) *SetDefaultRole {
	return &SetDefaultRole{
		Type:  t,
		Roles: r,
		Users: u,
	}
}

type SetRoleType int

const (
	SET_ROLE_TYPE_NORMAL SetRoleType = iota
	SET_ROLE_TYPE_DEFAULT
	SET_ROLE_TYPE_NONE
	SET_ROLE_TYPE_ALL
	SET_ROLE_TYPE_ALL_EXCEPT
)

type SetRole struct {
	statementImpl
	SecondaryRole     bool
	SecondaryRoleType SecondaryRoleType
	Role              *Role
}

func (node *SetRole) Format(ctx *FmtCtx) {
	ctx.WriteString("set")
	if !node.SecondaryRole {
		if node.Role != nil {
			ctx.WriteString(" role ")
			node.Role.Format(ctx)
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

type SetPassword struct {
	statementImpl
	User     *User
	Password string
}

func (node *SetPassword) Format(ctx *FmtCtx) {
	ctx.WriteString("set password")
	if node.User != nil {
		ctx.WriteString(" for ")
		node.User.Format(ctx)
	}
	ctx.WriteString(" = ")
	ctx.WriteString(node.Password)
}

func NewSetPassword(u *User, p string) *SetPassword {
	return &SetPassword{
		User:     u,
		Password: p,
	}
}
