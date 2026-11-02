// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func publicationStatementForReplay(pub *pubsub.PubInfo) *tree.CreatePublication {
	// Catalog names are already canonical. Reparsing SQL with a different
	// lower_case_table_names mode could select a case-distinct private table.
	// Build the internal AST directly, preserving both names and grants. This
	// also supports empty account/table lists that CREATE's grammar cannot spell.
	// The caller carries the separate all-tables bit: an empty AST table list
	// alone cannot distinguish zero selected tables from an omitted TABLE clause.
	var tables tree.TableNames
	if pub.TablesStr != pubsub.TableAll && pub.TablesStr != "" {
		names := strings.Split(pub.TablesStr, pubsub.Sep)
		tables = make(tree.TableNames, 0, len(names))
		for _, name := range names {
			tables = append(tables, tree.NewTableName(tree.Identifier(name), tree.ObjectNamePrefix{}, nil))
		}
	}
	allAccounts := strings.EqualFold(pub.SubAccountsStr, pubsub.AccountAll)
	var accounts tree.IdentifierList
	if !allAccounts {
		for _, name := range pub.GetSubAccountNames() {
			accounts = append(accounts, tree.Identifier(name))
		}
	}
	return tree.NewCreatePublication(false, tree.Identifier(pub.PubName), tree.Identifier(pub.DbName), tables,
		tree.NewAccountsSetOption(allAccounts, accounts, nil, nil), pub.Comment)
}

// createPublicationFromRestoreInfo keeps replay on the ordinary creation path,
// including current database identity, historical principals and subscriber state.
func createPublicationFromRestoreInfo(ctx context.Context, bh BackgroundExec, pub *pubsub.PubInfo, ts int64, omittedViews map[restoreObjectName]struct{}) error {
	replay, err := publicationInfoForBulkRestore(ctx, bh, pub, ts, omittedViews)
	if err != nil {
		return err
	}
	statement := publicationStatementForReplay(replay)
	defer statement.Free()
	var lowerCaseTableNames int64
	if publicationCreateValidatesTableScope(statement) {
		lowerCaseTableNames, err = publicationBackgroundLowerCaseTableNames(ctx, bh)
		if err != nil {
			return err
		}
	}
	return createPublicationWithTableSelection(ctx, bh, statement, replay.TablesStr == pubsub.TableAll, lowerCaseTableNames)
}

// publicationOmittedViews records only omissions confirmed by this restore's
// view owner. Database-qualified keys cannot hide same-named objects elsewhere.
func publicationOmittedViews(views map[string]*tableInfo) map[restoreObjectName]struct{} {
	var omitted map[restoreObjectName]struct{}
	for _, info := range views {
		if info != nil && info.typ == view && info.unservable {
			if omitted == nil {
				omitted = make(map[restoreObjectName]struct{})
			}
			omitted[restoreObjectName{info.dbName, info.tblName}] = struct{}{}
		}
	}
	return omitted
}

// Bulk restore deliberately omits external tables and known unservable views.
// Remove only those historical objects from an explicit selection, not arbitrary missing tables:
// ordinary creation validation must still catch an incomplete restore. Use the
// source tenant and timestamp, since current metadata has already been replaced.
func publicationInfoForBulkRestore(ctx context.Context, bh BackgroundExec, pub *pubsub.PubInfo, ts int64, omittedViews map[restoreObjectName]struct{}) (*pubsub.PubInfo, error) {
	if pub.TablesStr == pubsub.TableAll || pub.TablesStr == "" {
		return pub, nil
	}
	target, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	tables, err := showFullTablesFromTS(ctx, "", bh, pub.DbName, "", ts, pub.PubAccountId, target)
	if err != nil {
		return nil, err
	}
	omitted := make(map[string]struct{})
	for _, table := range tables {
		_, skippedView := omittedViews[restoreObjectName{table.dbName, table.tblName}]
		if shouldSkipRestoreTableInBulk(table) || table.typ == view && skippedView {
			omitted[table.tblName] = struct{}{}
		}
	}
	var selected []string
	for _, name := range strings.Split(pub.TablesStr, pubsub.Sep) {
		if _, skip := omitted[name]; !skip {
			selected = append(selected, name)
		}
	}
	// Never mutate snapshot metadata or turn an emptied explicit list into ALL.
	replay := *pub
	replay.TablesStr = strings.Join(selected, pubsub.Sep)
	return &replay, nil
}
