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

type GrantType int

const (
	GrantTypePrivilege GrantType = iota
	GrantTypeRole
	GrantTypeProxy
)

type Grant struct {
	statementImpl
	Typ            GrantType
	GrantPrivilege GrantPrivilege
	GrantRole      GrantRole
	GrantProxy     GrantProxy
}

func (node *Grant) Format(ctx *FmtCtx) {
	switch node.Typ {
	case GrantTypePrivilege:
		node.GrantPrivilege.Format(ctx)
	case GrantTypeRole:
		node.GrantRole.Format(ctx)
	case GrantTypeProxy:
		node.GrantProxy.Format(ctx)
	}
}

func NewGrant() *Grant {
	return &Grant{}
}

type GrantPrivilege struct {
	statementImpl
	Privileges []*Privilege
	//grant privileges
	ObjType ObjectType
	//grant privileges
	Level       *PrivilegeLevel
	Roles       []*Role
	GrantOption bool
}

func (node *GrantPrivilege) Format(ctx *FmtCtx) {
	ctx.WriteString("grant")
	if node.Privileges != nil {
		prefix := " "
		for _, p := range node.Privileges {
			ctx.WriteString(prefix)
			p.Format(ctx)
			prefix = ", "
		}
	}
	ctx.WriteString(" on")
	if node.ObjType != OBJECT_TYPE_NONE {
		ctx.WriteByte(' ')
		ctx.WriteString(node.ObjType.String())
	}
	if node.Level != nil {
		ctx.WriteByte(' ')
		node.Level.Format(ctx)
	}

	if node.Roles != nil {
		ctx.WriteString(" to")
		prefix := " "
		for _, r := range node.Roles {
			ctx.WriteString(prefix)
			r.Format(ctx)
			prefix = ", "
		}
	}
	if node.GrantOption {
		ctx.WriteString(" with grant option")
	}
}

type GrantRole struct {
	statementImpl
	Roles       []*Role
	Users       []*User
	GrantOption bool
}

func (node *GrantRole) Format(ctx *FmtCtx) {
	ctx.WriteString("grant")
	if node.Roles != nil {
		prefix := " "
		for _, r := range node.Roles {
			ctx.WriteString(prefix)
			r.Format(ctx)
			prefix = ", "
		}
	}
	if node.Users != nil {
		ctx.WriteString(" to")
		prefix := " "
		for _, r := range node.Users {
			ctx.WriteString(prefix)
			r.Format(ctx)
			prefix = ", "
		}
	}
	if node.GrantOption {
		ctx.WriteString(" with grant option")
	}
}

type GrantProxy struct {
	statementImpl
	ProxyUser   *User
	Users       []*User
	GrantOption bool
}

func (node *GrantProxy) Format(ctx *FmtCtx) {
	ctx.WriteString("grant")
	if node.ProxyUser != nil {
		ctx.WriteString(" proxy on ")
		node.ProxyUser.Format(ctx)
	}
	if node.Users != nil {
		ctx.WriteString(" to")
		prefix := " "
		for _, r := range node.Users {
			ctx.WriteString(prefix)
			r.Format(ctx)
			prefix = ", "
		}
	}
	if node.GrantOption {
		ctx.WriteString(" with grant option")
	}
}
