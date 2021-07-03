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