package pubsub

import (
	"strings"
)

const (
	AccountAll = "all"
	TableAll   = "*"
	Sep        = ","
)

type SubStatus int

const (
	SubStatusNormal SubStatus = iota
	SubStatusNotAuthorized
	SubStatusDeleted
)

type AccountInfo struct {
	Id   int32
	Name string
}

type PubInfo struct {
	PubName        string
	DbName         string
	DbId           uint64
	TablesStr      string
	SubAccountsStr string
	CreateTime     string
	UpdateTime     string
	Comment        string
}

func (pubInfo *PubInfo) GetSubAccountNames() (names []string) {
	for _, name := range strings.Split(pubInfo.SubAccountsStr, Sep) {
		names = append(names, name)
	}
	return names
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
