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
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func InSubMetaTables(m *plan.SubscriptionMeta, tableName string) bool {
	if strings.ToLower(m.Tables) == TableAll {
		return true
	}

	for _, name := range strings.Split(m.Tables, Sep) {
		if name == tableName {
			return true
		}
	}
	return false
}

func AddSingleQuotesJoin(s []string) string {
	if len(s) == 0 {
		return ""
	}
	return "'" + strings.Join(s, "','") + "'"
}

func SplitAccounts(s string) (accounts []string) {
	if len(s) == 0 {
		return
	}
	for _, split := range strings.Split(s, Sep) {
		accounts = append(accounts, strings.TrimSpace(split))
	}
	return
}

func JoinAccounts(accountMap map[int32]*AccountInfo) string {
	accountNames := make([]string, 0, len(accountMap))
	for _, acc := range accountMap {
		accountNames = append(accountNames, acc.Name)
	}
	slices.Sort(accountNames)
	return strings.Join(accountNames, Sep)
}

func CanPubToAll(accountName, pubAllAccounts string) bool {
	if pubAllAccounts == PubAllAccounts {
		return true
	}
	return slices.Contains(SplitAccounts(pubAllAccounts), accountName)
}

func RemoveTable(oldTableListStr, tblName string) string {
	if oldTableListStr == TableAll {
		return TableAll
	}

	tableList := strings.Split(oldTableListStr, Sep)
	newTableList := make([]string, 0, len(tableList))
	for _, name := range tableList {
		if name != tblName {
			newTableList = append(newTableList, name)
		}
	}
	slices.Sort(newTableList)
	return strings.Join(newTableList, Sep)
}
