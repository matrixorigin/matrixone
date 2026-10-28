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

package bootstrap

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const testDecisionHeader = `{"format":"matrixone-catalog-snapshot-decisions-v1","target":"4.0.7"}`
const testDecision = `{"catalog_account_id":"0","snapshot_id":"01a17d34-05f1-752c-acbd-52c7a8fe04fe","name":"__mo_branch_42","timestamp":"123","level":"table","account_name":"sys","database_name":"db","table_name":"t","object_id":"9","prior_kind":"user","edge":{"table_id":"42","parent_id":"9","clone_ts":"123","creator":"0","level":"table","table_deleted":false},"decision":"branch"}`

func TestSnapshotDecisionReader(t *testing.T) {
	reader, err := newSnapshotDecisionReader(strings.NewReader(testDecisionHeader + "\n" + testDecision + "\n"))
	require.NoError(t, err)
	d, err := reader.next()
	require.NoError(t, err)
	require.Equal(t, "__mo_branch_42", d.Name)
	require.Equal(t, "branch", d.Decision)
	require.NotNil(t, d.Edge)
	next, err := reader.next()
	require.NoError(t, err)
	require.Nil(t, next)
}

func TestSnapshotDecisionRejectsInvalidInput(t *testing.T) {
	for _, tc := range []struct{ name, header, record string }{
		{name: "wrong target", header: `{"format":"matrixone-catalog-snapshot-decisions-v1","target":"4.2"}`, record: testDecision},
		{name: "case alias in header", header: `{"format":"matrixone-catalog-snapshot-decisions-v1","target":"4.0.7","Target":"4.0.6"}`, record: testDecision},
		{name: "unknown field", record: strings.Replace(testDecision, `"decision":"branch"`, `"decision":"branch","extra":1`, 1)},
		{name: "duplicate decision", record: strings.Replace(testDecision, `"decision":"branch"`, `"decision":"branch","decision":"user"`, 1)},
		{name: "escaped duplicate decision", record: strings.Replace(testDecision, `"decision":"branch"`, `"decision":"branch","deci\u0073ion":"user"`, 1)},
		{name: "case alias after decision", record: strings.Replace(testDecision, `"decision":"branch"`, `"decision":"branch","Decision":"user"`, 1)},
		{name: "case alias before decision", record: strings.Replace(testDecision, `"decision":"branch"`, `"Decision":"user","decision":"branch"`, 1)},
		{name: "duplicate nested field", record: strings.Replace(testDecision, `"table_deleted":false`, `"table_deleted":false,"table_deleted":true`, 1)},
		{name: "case alias in edge", record: strings.Replace(testDecision, `"table_deleted":false`, `"table_deleted":false,"Table_Deleted":true`, 1)},
		{name: "unknown decision", record: strings.Replace(testDecision, `"decision":"branch"`, `"decision":"ignore"`, 1)},
		{name: "missing edge field", record: strings.Replace(testDecision, `,"table_deleted":false`, "", 1)},
		{name: "missing nullable field", record: strings.Replace(testDecision, `,"database_name":"db"`, "", 1)},
		{name: "invalid UTF-8", record: strings.Replace(testDecision, `"account_name":"sys"`, "\"account_name\":\"\xff\"", 1)},
		{name: "wrong child id", record: strings.Replace(testDecision, `"table_id":"42"`, `"table_id":"43"`, 1)},
		{name: "noncanonical name", record: strings.Replace(testDecision, `__mo_branch_42`, `__mo_branch_042`, 1)},
		{name: "noncanonical uuid", record: strings.Replace(testDecision, `01a17d34-05f1-752c-acbd-52c7a8fe04fe`, `invalid`, 1)},
		{name: "noncanonical timestamp", record: strings.Replace(testDecision, `"timestamp":"123"`, `"timestamp":"0123"`, 1)},
		{name: "non system account", record: strings.Replace(testDecision, `"catalog_account_id":"0"`, `"catalog_account_id":"3"`, 1)},
		{name: "wrong kind", record: strings.Replace(testDecision, `"prior_kind":"user"`, `"prior_kind":"branch"`, 1)},
		{name: "trailing value", record: testDecision + ` {}`},
		{name: "oversized record", record: fmt.Sprintf(`{"padding":"%s"}`, strings.Repeat("x", snapshotDecisionRecordLimit))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			header := tc.header
			if header == "" {
				header = testDecisionHeader
			}
			reader, err := newSnapshotDecisionReader(strings.NewReader(header + "\n" + tc.record + "\n"))
			if err == nil {
				_, err = reader.next()
			}
			require.Error(t, err)
		})
	}
	reader, err := newSnapshotDecisionReader(strings.NewReader(testDecisionHeader + "\n" + testDecision + "\n" + testDecision + "\n"))
	require.NoError(t, err)
	_, err = reader.next()
	require.NoError(t, err)
	_, err = reader.next()
	require.ErrorContains(t, err, "strictly ordered")
}

func TestSnapshotDecisionUsesExactPreimageAndIsMonotone(t *testing.T) {
	reader, err := newSnapshotDecisionReader(strings.NewReader(testDecisionHeader + "\n" + testDecision))
	require.NoError(t, err)
	d, err := reader.next()
	require.NoError(t, err)
	actual := d.snapshotDecisionPreimage
	require.NoError(t, validateSnapshotDecision(*d, actual, "user"))
	require.NoError(t, validateSnapshotDecision(*d, actual, "branch"))
	user := *d
	user.Decision = "user"
	require.ErrorContains(t, validateSnapshotDecision(user, actual, "branch"), "reverse")
	stale := actual
	stale.AccountName = nil
	require.ErrorContains(t, validateSnapshotDecision(*d, stale, "user"), "preimage")
	edge := *actual.Edge
	changed := "10"
	edge.ParentID = &changed
	stale = actual
	stale.Edge = &edge
	require.ErrorContains(t, validateSnapshotDecision(*d, stale, "user"), "preimage")
	require.Error(t, validateSnapshotDecision(*d, actual, "unexpected"))
}
