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

func NewSetVar(a []*VarAssignmentExpr)*SetVar{
	return &SetVar{
		Assignments:   a,
	}
}

//for variable = expr
type VarAssignmentExpr struct {
	NodePrinter
	System bool
	Global bool
	Name string
	Value Expr
	Reserved Expr
}

func NewVarAssignmentExpr(s bool, g bool, n string, v Expr, r Expr) *VarAssignmentExpr {
	return &VarAssignmentExpr{
		System:      s,
		Global:      g,
		Name:        n,
		Value:       v,
		Reserved:    r,
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
	Type SetDefaultRoleType
	Roles []*Role
	Users []*User
}

func NewSetDefaultRole(t SetDefaultRoleType,r []*Role,u []*User) *SetDefaultRole {
	return &SetDefaultRole{
		Type:          t,
		Roles:         r,
		Users:         u,
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
	Type SetRoleType
	Roles []*Role
}

func NewSetRole(t SetRoleType,r []*Role) *SetRole{
	return &SetRole{
		Type:          t,
		Roles:         r,
	}
}

type SetPassword struct {
	statementImpl
	User *User
	Password string
}

func NewSetPassword(u *User,p string)*SetPassword{
	return &SetPassword{
		User:u,
		Password: p,
	}
}