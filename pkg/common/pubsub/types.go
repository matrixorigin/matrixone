// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pubsub

import (
	"strings"
)

const (
	// AccountAll is the value of account_list column in table mo_pubs
	AccountAll = "all"
	// AccountAllOutput is the value of sub_account column of command `show publication`
	AccountAllOutput = "*"
	// TableAll is the value of table_list column in table mo_pubs
	TableAll = "*"
	Sep      = ","
	// PubAllAccounts is the accounts which have privilege to publish to-all publications, separated by comma
	PubAllAccounts = "*"
)

type SubStatus int

const (
	SubStatusNormal SubStatus = iota
	SubStatusNotAuthorized
	SubStatusDeleted
)

type AccountInfo struct {
	Id            int32
	Name          string
	Status        string
	Version       uint64
	SuspendedTime string
}

type PubInfo struct {
	PubAccountId   uint32
	PubAccountName string
	PubName        string
	DbName         string
	DbId           uint64
	TablesStr      string
	SubAccountsStr string
	CreateTime     string
	UpdateTime     string
	Owner          uint32
	Creator        uint32
	Comment        string
}

func (pubInfo *PubInfo) GetSubAccountNames() (names []string) {
	return SplitAccounts(pubInfo.SubAccountsStr)
}

func (pubInfo *PubInfo) InSubAccounts(accountName string) bool {
	if strings.ToLower(pubInfo.SubAccountsStr) == AccountAll {
		return true
	}

	for _, name := range strings.Split(pubInfo.SubAccountsStr, Sep) {
		if name == accountName {
			return true
		}
	}
	return false
}

func (pubInfo *PubInfo) GetCreateSql() string {
	sql := "CREATE PUBLICATION " + pubInfo.PubName + " DATABASE " + pubInfo.DbName
	if pubInfo.TablesStr != TableAll {
		sql += " TABLE " + pubInfo.TablesStr
	}
	sql += " ACCOUNT " + pubInfo.SubAccountsStr
	if len(pubInfo.Comment) > 0 {
		sql += " COMMENT '" + pubInfo.Comment + "'"
	}
	return sql
}

type SubInfo struct {
	SubAccountId   int32
	SubName        string
	SubTime        string
	PubAccountName string
	PubName        string
	PubDbName      string
	PubTables      string
	PubTime        string
	PubComment     string
	Status         SubStatus
}
