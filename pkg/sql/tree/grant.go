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
	IsGrantRole bool
	IsProxy bool
	RolesInGrantRole []*Role
	Privileges []*Privilege
	ObjType ObjectType
	Level *PrivilegeLevel
	ProxyUser *User
	Users []*User
	Roles []*Role
	GrantOption bool
}

func NewGrant(igr bool, ip bool, rigr []*Role, p []*Privilege, t ObjectType, l *PrivilegeLevel, pu *User, u []*User, r []*Role, gopt bool) *Grant {
	return &Grant{
		IsGrantRole: igr,
		IsProxy: ip,
		RolesInGrantRole: rigr,
		Privileges:    p,
		ObjType:       t,
		Level:         l,
		ProxyUser:		pu,
		Users:         u,
		Roles:         r,
		GrantOption: gopt,
	}
}