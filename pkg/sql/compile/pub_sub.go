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

package compile

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const sysAccountId = 0

func createSubscription(ctx context.Context, c *Compile, dbName string, subOption *plan.SubscriptionOption) error {
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	// check existence
	sql := fmt.Sprintf("select count(1) from mo_catalog.mo_subs where pub_account_name = '%s' and pub_name = '%s' and sub_account_id = %d and sub_name is not null", subOption.From, subOption.Publication, accountId)
	rs, err := c.runSqlWithResult(sql, sysAccountId)
	if err != nil {
		return err
	}
	defer rs.Close()

	var totalCnt int64
	rs.ReadRows(func(_ int, cols []*vector.Vector) bool {
		totalCnt = executor.GetFixedRows[int64](cols[0])[0]
		return false
	})
	if totalCnt > 0 {
		return moerr.NewInternalError(ctx, "publication %s can only be subscribed once", subOption.Publication)
	}

	sql = fmt.Sprintf("update mo_catalog.mo_subs set sub_name='%s', sub_time=now() where pub_account_name = '%s' and pub_name = '%s' and sub_account_id = %d", dbName, subOption.From, subOption.Publication, accountId)
	return c.runSqlWithAccountId(sql, sysAccountId)
}

func dropSubscription(ctx context.Context, c *Compile, dbName string) error {
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	// update SubStatusNormal records
	sql := fmt.Sprintf("update mo_catalog.mo_subs set sub_name=null, sub_time=null where sub_account_id = %d and sub_name = '%s' and status = %d", accountId, dbName, pubsub.SubStatusNormal)
	if err = c.runSqlWithAccountId(sql, sysAccountId); err != nil {
		return err
	}

	// delete SubStatusDeleted && SubStatusNotAuthorized records
	sql = fmt.Sprintf("delete from mo_catalog.mo_subs where sub_account_id = %d and sub_name = '%s' and status != %d", accountId, dbName, pubsub.SubStatusNormal)
	return c.runSqlWithAccountId(sql, sysAccountId)
}

func updatePubTableList(ctx context.Context, c *Compile, dbName, dropTblName string) error {
	accountName, err := func() (string, error) {
		accountId, err := defines.GetAccountId(ctx)
		if err != nil {
			return "", err
		}

		sql := fmt.Sprintf("select account_name from mo_catalog.mo_account where account_id = %d", accountId)
		rs, err := c.runSqlWithResult(sql, sysAccountId)
		if err != nil {
			return "", err
		}
		defer rs.Close()

		var accountName string
		rs.ReadRows(func(rows int, cols []*vector.Vector) bool {
			for i := 0; i < rows; {
				accountName = cols[0].GetStringAt(i)
				break
			}
			return false
		})
		return accountName, nil
	}()
	if err != nil {
		return err
	}

	// get pub
	sql := fmt.Sprintf("select pub_name, table_list from mo_catalog.mo_pubs where database_name = '%s'", dbName)
	rs, err := c.runSqlWithResult(sql, NoAccountId)
	if err != nil {
		return err
	}
	defer rs.Close()

	pubNameTableListMap := make(map[string]string)
	rs.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			pubNameTableListMap[cols[0].GetStringAt(i)] = cols[1].GetStringAt(i)
		}
		return true
	})

	for pubName, tableListStr := range pubNameTableListMap {
		if tableListStr == pubsub.TableAll {
			continue
		}

		newTableListStr := pubsub.RemoveTable(tableListStr, dropTblName)
		// update pub
		sql = fmt.Sprintf("update mo_catalog.mo_pubs set table_list='%s' where pub_name = '%s'", newTableListStr, pubName)
		if err = c.runSqlWithAccountId(sql, NoAccountId); err != nil {
			return err
		}

		// update sub
		sql = fmt.Sprintf("update mo_catalog.mo_subs set pub_tables='%s' where pub_account_name = '%s' and pub_name = '%s'", newTableListStr, accountName, pubName)
		if err = c.runSqlWithAccountId(sql, sysAccountId); err != nil {
			return err
		}
	}
	return nil
}
