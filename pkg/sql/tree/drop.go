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

//DROP Database statement
type DropDatabase struct {
	statementImpl
	Name Identifier
	IfExists bool
}

func NewDropDatabase(n Identifier,i bool)*DropDatabase{
	return &DropDatabase{
		Name:          n,
		IfExists:      i,
	}
}

//DROP Table statement
type DropTable struct {
	statementImpl
	IfExists bool
	Names TableNames
}

func NewDropTable(i bool,n TableNames) *DropTable {
	return &DropTable{
		IfExists:      i,
		Names:         n,
	}
}

type DropIndex struct {
	statementImpl
	Name Identifier
	TableName TableName
	IfExists bool
	MiscOption []MiscOption
}

func NewDropIndex(i Identifier,t TableName,ife bool,m []MiscOption) *DropIndex {
	return &DropIndex{
		Name:          i,
		TableName: t,
		IfExists:      ife,
		MiscOption:    m,
	}
}

type DropRole struct {
	statementImpl
	IfExists bool
	Roles []*Role
}

func NewDropRole(ife bool, r []*Role) *DropRole {
	return &DropRole{
		IfExists:      ife,
		Roles:         r,
	}
}

type DropUser struct {
	statementImpl
	IfExists bool
	Users []*User
}

func NewDropUser(ife bool,u []*User) *DropUser{
	return &DropUser{
		IfExists:      ife,
		Users:         u,
	}
}