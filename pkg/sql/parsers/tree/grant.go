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

type GrantType int

func init() {
	reuse.CreatePool[Grant](
		func() *Grant { return &Grant{} },
		func(g *Grant) { g.reset() },
		reuse.DefaultOptions[Grant](),
	)

	reuse.CreatePool[GrantPrivilege](
		func() *GrantPrivilege { return &GrantPrivilege{} },
		func(g *GrantPrivilege) { g.reset() },
		reuse.DefaultOptions[GrantPrivilege](),
	)

	reuse.CreatePool[GrantRole](
		func() *GrantRole { return &GrantRole{} },
		func(g *GrantRole) { g.reset() },
		reuse.DefaultOptions[GrantRole](),
	)

	reuse.CreatePool[GrantProxy](
		func() *GrantProxy { return &GrantProxy{} },
		func(g *GrantProxy) { g.reset() },
		reuse.DefaultOptions[GrantProxy](),
	)
}

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

func (node *Grant) GetStatementType() string { return "Grant" }

func (node *Grant) GetQueryType() string { return QueryTypeDCL }

func (node Grant) TypeName() string { return "tree.Grant" }

func (node *Grant) reset() {
	switch node.Typ {
	case GrantTypePrivilege:
		node.GrantPrivilege.Free()
	case GrantTypeRole:
		node.GrantRole.Free()
	case GrantTypeProxy:
		node.GrantProxy.Free()
	}
	*node = Grant{}
}

func (node *Grant) Free() {
	reuse.Free[Grant](node, nil)
}

func NewGrant(t GrantType) *Grant {
	g := reuse.Alloc[Grant](nil)
	g.Typ = t
	return g
}

type GrantPrivilege struct {
	statementImpl
	Privileges []*Privilege
	// grant privileges
	ObjType ObjectType
	// grant privileges
	Level       *PrivilegeLevel
	Roles       []*Role
	GrantOption bool
}

func NewGrantPrivilege(ps []*Privilege, o ObjectType, pl *PrivilegeLevel, rs []*Role, ot bool) *GrantPrivilege {
	gp := reuse.Alloc[GrantPrivilege](nil)
	gp.Privileges = ps
	gp.ObjType = o
	gp.Level = pl
	gp.Roles = rs
	gp.GrantOption = ot
	return gp
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

func (node *GrantPrivilege) reset() {
	if node.Privileges != nil {
		for _, item := range node.Privileges {
			item.Free()
		}
	}
	if node.Level != nil {
		node.Level.Free()
	}
	if node.Roles != nil {
		for _, item := range node.Roles {
			item.Free()
		}
	}
	*node = GrantPrivilege{}
}

func (node *GrantPrivilege) GetStatementType() string { return "Grant Privilege" }

func (node *GrantPrivilege) GetQueryType() string { return QueryTypeDCL }

func (node GrantPrivilege) TypeName() string { return "tree.GrantPrivilege" }

func (node *GrantPrivilege) Free() {
	reuse.Free[GrantPrivilege](node, nil)
}

type GrantRole struct {
	statementImpl
	Roles       []*Role
	Users       []*User
	GrantOption bool
}

func NewGrantRole(rs []*Role, us []*User, gopt bool) *GrantRole {
	gr := reuse.Alloc[GrantRole](nil)
	gr.Roles = rs
	gr.Users = us
	gr.GrantOption = gopt
	return gr
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

func (node *GrantRole) reset() {
	if node.Roles != nil {
		for _, item := range node.Roles {
			item.Free()
		}
	}
	if node.Users != nil {
		for _, item := range node.Users {
			item.Free()
		}
	}
	*node = GrantRole{}
}

func (node *GrantRole) GetStatementType() string { return "Grant Role" }

func (node *GrantRole) GetQueryType() string { return QueryTypeDCL }

func (node GrantRole) TypeName() string { return "tree.GrantRole" }

func (node *GrantRole) Free() {
	reuse.Free[GrantRole](node, nil)
}

type GrantProxy struct {
	statementImpl
	ProxyUser   *User
	Users       []*User
	GrantOption bool
}

func NewGrantProxy(pu *User, us []*User, gopt bool) *GrantProxy {
	gp := reuse.Alloc[GrantProxy](nil)
	gp.ProxyUser = pu
	gp.Users = us
	gp.GrantOption = gopt
	return gp
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

func (node *GrantProxy) reset() {
	if node.ProxyUser != nil {
		node.ProxyUser.Free()
	}
	if node.Users != nil {
		for _, item := range node.Users {
			item.Free()
		}
	}
	*node = GrantProxy{}
}

func (node *GrantProxy) Free() {
	reuse.Free[GrantProxy](node, nil)
}

func (node *GrantProxy) GetStatementType() string { return "Grant Proxy" }

func (node *GrantProxy) GetQueryType() string { return QueryTypeDCL }

func (node GrantProxy) TypeName() string { return "tree.GrantProxy" }
