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