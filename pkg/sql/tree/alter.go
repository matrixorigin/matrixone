package tree

type AlterUser struct {
	statementImpl
	IfExists bool
	IsUserFunc bool
	UserFunc *User
	Users []*User
	Roles []*Role
	TlsOpts []TlsOption
	ResOpts []ResourceOption
	MiscOpts []UserMiscOption
}

func NewAlterUser(ife bool,iuf bool,uf *User,u []*User,r []*Role,t []TlsOption,res []ResourceOption,m []UserMiscOption) *AlterUser{
	return &AlterUser{
		IfExists:      ife,
		IsUserFunc: iuf,
		UserFunc: uf,
		Users:         u,
		Roles:         r,
		TlsOpts:       t,
		ResOpts:       res,
		MiscOpts:      m,
	}
}