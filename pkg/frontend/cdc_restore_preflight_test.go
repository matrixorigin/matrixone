// Copyright 2026 Matrix Origin
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

package frontend

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestHistoricalCDCAccountRestorePreflight(t *testing.T) {
	const query = "select attname from mo_catalog.mo_columns {MO_TS = 123} where account_id = 0 and att_database = 'mo_catalog' and att_relname = 'mo_cdc_watermark' and attname in ('source_table_id','owner_generation','pending_source_table_id','target_identity')"
	columns := []string{"source_table_id", "owner_generation", "pending_source_table_id", "target_identity"}
	for _, tc := range []struct {
		name      string
		account   uint32
		missing   string
		queryFail bool
		want      string
	}{
		{"current schema", 0, "", false, ""},
		{"old schema", 0, "pending_source_table_id", false, "incompatible schema"},
		{"metadata read failure", 0, "", true, "metadata read failed"},
		{"tenant does not own catalog", 42, "", false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			var rows [][]interface{}
			for _, col := range columns {
				if col != tc.missing {
					rows = append(rows, []interface{}{col})
				}
			}
			bh.sql2result[query] = newMrsForRestoreStringRows([]string{"attname"}, rows)
			if tc.queryFail {
				bh.sql2err[query] = moerr.NewInternalErrorNoCtx("metadata read failed")
			}
			err := preflightHistoricalCDCAccountRestore(context.Background(), bh, 123, tc.account)
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
			if tc.account != 0 {
				require.Empty(t, bh.executedSQLs)
			}
		})
	}
}

func TestClusterRestoreRejectsLegacyCDCBeforeAccountSideEffects(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	bh := &backgroundExecTest{}
	bh.init()
	const ts = int64(123)
	current := getCurrentExistsAccountsFmt
	past := fmt.Sprintf(getPastAccountsFmt, ts)
	legacyColumns := "select attname from mo_catalog.mo_columns {MO_TS = 123} where account_id = 0 and att_database = 'mo_catalog' and att_relname = 'mo_cdc_watermark' and attname in ('source_table_id','owner_generation','pending_source_table_id','target_identity')"
	bh.sql2result[current] = newMrsForRestoreStringRows([]string{"account_id", "account_name"}, [][]interface{}{{"0", "sys"}, {"42", "must_not_be_dropped"}})
	bh.sql2result[past] = newMrsForRestoreStringRows([]string{"account_id", "account_name", "admin_name", "comments"}, [][]interface{}{{"0", "sys", "root", ""}})
	bh.sql2result[legacyColumns] = newMrsForRestoreStringRows([]string{"attname"}, nil)
	var retired []uint32
	err := restoreToCluster(defines.AttachAccountId(context.Background(), 0), ses, bh, "old", ts, &retired)
	require.ErrorContains(t, err, "incompatible schema")
	require.Empty(t, retired)
	require.Equal(t, []string{current, past, legacyColumns}, bh.executedSQLs)
}

func TestPitrRestoreRejectsLegacyCDCBeforeSideEffects(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	const query = "select attname from mo_catalog.mo_columns {MO_TS = 123} where account_id = 0 and att_database = 'mo_catalog' and att_relname = 'mo_cdc_watermark' and attname in ('source_table_id','owner_generation','pending_source_table_id','target_identity')"
	bh.sql2result[query] = newMrsForRestoreStringRows([]string{"attname"}, nil)
	err := preflightRestorePitrEntry(context.Background(), "", bh, "old", 123, tree.RESTORELEVELACCOUNT, "", 0)
	require.ErrorContains(t, err, "incompatible schema")
	require.Equal(t, []string{query}, bh.executedSQLs)
}
