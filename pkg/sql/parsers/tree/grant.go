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

type Grant struct {
	statementImpl
	Privileges []*Privilege

	IsGrantRole      bool
	RolesInGrantRole []*Role

	IsProxy   bool
	ProxyUser *User

	ObjType     ObjectType
	Level       *PrivilegeLevel
	Users       []*User
	Roles       []*Role
	GrantOption bool
}

func (node *Grant) Format(ctx *FmtCtx) {
	ctx.WriteString("grant")
	if node.IsGrantRole {
		prefix := " "
		for _, r := range node.RolesInGrantRole {
			ctx.WriteString(prefix)
			r.Format(ctx)
			prefix = ", "
		}
		goto common
	}
	if node.IsProxy {
		ctx.WriteString(" proxy on ")
		node.ProxyUser.Format(ctx)
		goto common
	}

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
		ctx.WriteString(node.ObjType.ToString())
	}
	if node.Level != nil {
		ctx.WriteByte(' ')
		node.Level.Format(ctx)
	}

common:
	if node.Users != nil {
		ctx.WriteString(" to")
		prefix := " "
		for _, u := range node.Users {
			ctx.WriteString(prefix)
			u.Format(ctx)
			prefix = ", "
		}
	}
	if node.GrantOption {
		ctx.WriteString(" with grant option")
	}
}

func NewGrant(igr bool, ip bool, rigr []*Role, p []*Privilege, t ObjectType, l *PrivilegeLevel, pu *User, u []*User, r []*Role, gopt bool) *Grant {
	return &Grant{
		IsGrantRole:      igr,
		IsProxy:          ip,
		RolesInGrantRole: rigr,
		Privileges:       p,
		ObjType:          t,
		Level:            l,
		ProxyUser:        pu,
		Users:            u,
		Roles:            r,
		GrantOption:      gopt,
	}
}
