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
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func InSubMetaTables(m *plan.SubscriptionMeta, tableName string) bool {
	if strings.ToLower(m.Tables) == TableAll {
		return true
	}

	for name := range strings.SplitSeq(m.Tables, Sep) {
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
	accounts = make([]string, 0, strings.Count(s, Sep)+1)
	for split := range strings.SplitSeq(s, Sep) {
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

func JoinAccountIds(accIds []int32) string {
	var result strings.Builder
	var scratch [12]byte
	for i, id := range accIds {
		if i != 0 {
			result.WriteByte(',')
		}
		result.Write(strconv.AppendInt(scratch[:0], int64(id), 10))
	}
	return result.String()
}

func CanPubToAll(accountName, pubAllAccounts string) bool {
	if pubAllAccounts == PubAllAccounts {
		return true
	}
	if pubAllAccounts == "" {
		return false
	}
	for account := range strings.SplitSeq(pubAllAccounts, Sep) {
		if strings.TrimSpace(account) == accountName {
			return true
		}
	}
	return false
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

// GetAccounts returns
// nameInfoMap: map[accName] -> AccountInfo
// idInfoMap: map[accId] -> AccountInfo
var GetAccounts = func(txn executor.TxnExecutor) (nameInfoMap map[string]*AccountInfo, idInfoMap map[int32]*AccountInfo, err error) {
	sql := "select account_id, account_name, status, version, suspended_time from mo_catalog.mo_account where 1=1"

	res, err := txn.Exec(sql, executor.StatementOption{}.WithAccountID(0))
	if err != nil {
		return
	}
	defer res.Close()

	nameInfoMap = make(map[string]*AccountInfo)
	idInfoMap = make(map[int32]*AccountInfo)
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			var accountInfo AccountInfo
			accountInfo.Id = vector.GetFixedAtWithTypeCheck[int32](cols[0], i)
			accountInfo.Name = cols[1].GetStringAt(i)
			accountInfo.Status = cols[2].GetStringAt(i)
			accountInfo.Version = vector.GetFixedAtWithTypeCheck[uint64](cols[3], i)
			if !cols[4].IsNull(uint64(i)) {
				accountInfo.SuspendedTime = vector.GetFixedAtWithTypeCheck[types.Timestamp](cols[4], i).String2(time.Local, cols[4].GetType().Scale)
			} else {
				accountInfo.SuspendedTime = ""
			}
			nameInfoMap[accountInfo.Name] = &accountInfo
			idInfoMap[accountInfo.Id] = &accountInfo
		}
		return true
	})
	return
}
