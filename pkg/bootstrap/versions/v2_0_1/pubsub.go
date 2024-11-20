// Copyright 2024 Matrix Origin
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

package v2_0_1

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var insertIntoNewMoPubsFormat = `insert into mo_catalog.mo_pubs (account_id, pub_name, database_name, database_id, all_table, table_list, account_list, created_time, update_time, owner, creator, comment) values (%d, '%s', '%s', %d, %t,'%s', '%s', '%s', '%s', %d, %d, '%s');`

var getSubbedAccNames = func(
	txn executor.TxnExecutor,
	pubAccountName, pubName string,
	accIdInfoMap map[int32]*pubsub.AccountInfo,
) ([]string, error) {
	sqlFormat := "select sub_account_id from mo_catalog.mo_subs where pub_account_name = '%s' and pub_name = '%s' and sub_name is NOT NULL"
	res, err := txn.Exec(fmt.Sprintf(sqlFormat, pubAccountName, pubName), executor.StatementOption{}.WithAccountID(0))
	if err != nil {
		return nil, err
	}
	defer res.Close()

	accountNames := make([]string, 0)
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			accId := vector.GetFixedAtWithTypeCheck[int32](cols[0], i)
			if accInfo, ok := accIdInfoMap[accId]; ok {
				accountNames = append(accountNames, accInfo.Name)
			}
		}
		return true
	})
	return accountNames, nil
}

// migrateMoPubs migrate each non-sys account's mo_pubs tables to sys account's, attach with account_id
func migrateMoPubs(txn executor.TxnExecutor) (err error) {
	accNameInfoMap, accIdInfoMap, err := pubsub.GetAccounts(txn)
	if err != nil {
		return
	}

	// allPubInfos: pubAccountName#pubName -> pubInfo
	allPubInfos, err := versions.GetAllPubInfos(txn, accNameInfoMap)
	if err != nil {
		return
	}

	var subbedAccNames []string
	for _, info := range allPubInfos {
		if info.PubAccountName == "sys" {
			continue
		}

		// modifies the to-all publications' pub scope to subscribed accounts
		if info.SubAccountsStr == pubsub.AccountAll {
			if subbedAccNames, err = getSubbedAccNames(txn, info.PubAccountName, info.PubName, accIdInfoMap); err != nil {
				return
			}
			info.SubAccountsStr = strings.Join(subbedAccNames, pubsub.Sep)
		}

		insertSql := fmt.Sprintf(insertIntoNewMoPubsFormat,
			// account_id
			accNameInfoMap[info.PubAccountName].Id,
			// pub_name
			info.PubName,
			// database_name
			info.DbName,
			// database_id
			info.DbId,
			// all_table
			true,
			// table_list
			info.TablesStr,
			// account_list
			info.SubAccountsStr,
			// created_time
			info.CreateTime,
			// update_time
			info.UpdateTime,
			// owner
			info.Owner,
			// creator
			info.Creator,
			// comment
			info.Comment,
		)
		if _, err = txn.Exec(insertSql, executor.StatementOption{}.WithAccountID(0)); err != nil {
			return
		}
	}
	return
}
