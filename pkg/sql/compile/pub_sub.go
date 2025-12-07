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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const sysAccountId = 0

var sysDatabases = []string{
	"mo_catalog",
	"information_schema",
	"system",
	"system_metrics",
	"mysql",
	"mo_task",
}

func createSubscription(ctx context.Context, c *Compile, dbName string, subOption *plan.SubscriptionOption) error {
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	// check existence
	sql := fmt.Sprintf(`
		SELECT count(1)
		FROM mo_catalog.mo_subs
		WHERE pub_account_name = '%s' AND pub_name = '%s' AND sub_account_id = %d AND sub_name is not null
	`, subOption.From, subOption.Publication, accountId)
	rs, err := c.runSqlWithResultAndOptions(
		sql,
		sysAccountId,
		executor.StatementOption{}.WithDisableLog(),
	)
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
		return moerr.NewInternalErrorf(ctx, "publication %s can only be subscribed once", subOption.Publication)
	}

	sql = fmt.Sprintf(`
		UPDATE mo_catalog.mo_subs
		SET sub_name='%s', sub_time=now()
		WHERE pub_account_name = '%s' AND pub_name = '%s' AND sub_account_id = %d
	`, dbName, subOption.From, subOption.Publication, accountId)
	return c.runSqlWithAccountIdAndOptions(
		sql,
		sysAccountId,
		executor.StatementOption{}.WithDisableLog(),
	)
}

func dropSubscription(ctx context.Context, c *Compile, dbName string) error {
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	// update SubStatusNormal records
	sql := fmt.Sprintf(`
		UPDATE mo_catalog.mo_subs
		SET sub_name=null, sub_time=null
		WHERE sub_account_id = %d AND sub_name = '%s' AND status = %d
	`, accountId, dbName, pubsub.SubStatusNormal)
	if err = c.runSqlWithAccountIdAndOptions(
		sql,
		sysAccountId,
		executor.StatementOption{}.WithDisableLog(),
	); err != nil {
		return err
	}

	// delete SubStatusDeleted && SubStatusNotAuthorized records
	sql = fmt.Sprintf(`
		DELETE FROM mo_catalog.mo_subs
		WHERE sub_account_id = %d AND sub_name = '%s' AND status != %d
	`, accountId, dbName, pubsub.SubStatusNormal)
	return c.runSqlWithAccountIdAndOptions(sql, sysAccountId, executor.StatementOption{}.WithDisableLog())
}

func updatePubTableList(ctx context.Context, c *Compile, dbName, dropTblName string) error {
	// skip system databases
	if slices.Contains(sysDatabases, dbName) {
		return nil
	}

	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	accountName, err := func() (string, error) {
		sql := fmt.Sprintf(`
			SELECT account_name
			FROM mo_catalog.mo_account
			WHERE account_id = %d
		`, accountId)
		rs, err := c.runSqlWithResultAndOptions(
			sql,
			sysAccountId,
			executor.StatementOption{}.WithDisableLog(),
		)
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
	sql := fmt.Sprintf(`
		SELECT pub_name, table_list
		FROM mo_catalog.mo_pubs
		WHERE account_id = %d AND database_name = '%s'
	`, accountId, dbName)
	rs, err := c.runSqlWithResultAndOptions(
		sql,
		sysAccountId,
		executor.StatementOption{}.WithDisableLog(),
	)
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
		sql = fmt.Sprintf(`
			UPDATE mo_catalog.mo_pubs
			SET table_list='%s'
			WHERE account_id = %d AND pub_name = '%s'
		`, newTableListStr, accountId, pubName)
		if err = c.runSqlWithAccountIdAndOptions(
			sql,
			sysAccountId,
			executor.StatementOption{}.WithDisableLog(),
		); err != nil {
			return err
		}

		// update sub
		sql = fmt.Sprintf(`
			UPDATE mo_catalog.mo_subs
			SET pub_tables='%s'
			WHERE pub_account_name = '%s' AND pub_name = '%s'
		`, newTableListStr, accountName, pubName)
		if err = c.runSqlWithAccountIdAndOptions(
			sql,
			sysAccountId,
			executor.StatementOption{}.WithDisableLog(),
		); err != nil {
			return err
		}
	}
	return nil
}
