package pubsub

import (
	"strings"
)

const (
	AccountAll       = "all"
	AccountAllOutput = "*"
	TableAll         = "*"
	Sep              = ","
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
