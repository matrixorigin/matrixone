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

package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// iscpLogRow is a row of mo_iscp_log as SHOW INDEX STATUS reads it.
type iscpLogRow struct {
	tableID   uint64
	job       string
	jobID     uint64
	state     int8
	watermark string
	errCode   int64
	errMsg    string
}

// iscpLogExecutor answers the read of mo_iscp_log with rows, and records how it was asked.
type iscpLogExecutor struct {
	t       *testing.T
	rows    []iscpLogRow
	sqls    []string
	tenants []uint32
}

func (e *iscpLogExecutor) Exec(_ context.Context, sql string, opts executor.Options) (executor.Result, error) {
	e.sqls = append(e.sqls, sql)
	e.tenants = append(e.tenants, opts.AccountID())
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(7)
	bat.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_int8.ToType())
	bat.Vecs[4] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[5] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[6] = vector.NewVec(types.T_varchar.ToType())
	for _, r := range e.rows {
		require.NoError(e.t, vector.AppendFixed(bat.Vecs[0], r.tableID, false, mp))
		require.NoError(e.t, vector.AppendBytes(bat.Vecs[1], []byte(r.job), false, mp))
		require.NoError(e.t, vector.AppendFixed(bat.Vecs[2], r.jobID, false, mp))
		require.NoError(e.t, vector.AppendFixed(bat.Vecs[3], r.state, false, mp))
		require.NoError(e.t, vector.AppendBytes(bat.Vecs[4], []byte(r.watermark), false, mp))
		require.NoError(e.t, vector.AppendFixed(bat.Vecs[5], r.errCode, false, mp))
		require.NoError(e.t, vector.AppendBytes(bat.Vecs[6], []byte(r.errMsg), r.errMsg == "", mp))
	}
	bat.SetRowCount(len(e.rows))
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
}

func (e *iscpLogExecutor) ExecTxn(context.Context, func(executor.TxnExecutor) error, executor.Options) error {
	return nil
}

// iscpLogCompilerContext is a compiler context whose internal SQL reads iscpLogExecutor.
type iscpLogCompilerContext struct {
	*MockCompilerContext
	exec *iscpLogExecutor
}

func (c *iscpLogCompilerContext) getInternalSQLExecutor(*process.Process) (executor.SQLExecutor, bool) {
	return c.exec, true
}

const testWatermark = "1730000000000000000-0"

func newShowIndexStatusTestContext(t *testing.T) *iscpLogCompilerContext {
	cc := newShowIndexIgnoredTestContext(t)
	cc.SetContext(defines.AttachAccountId(cc.GetContext(), 7))
	for _, idx := range cc.tables["vt"].Indexes {
		if idx.IndexName == "i" {
			idx.IndexAlgoParams = `{"max_bad_records":"3"}`
		}
	}
	return &iscpLogCompilerContext{MockCompilerContext: cc, exec: &iscpLogExecutor{t: t, rows: []iscpLogRow{
		{tableID: 9001, job: "index_i", jobID: 1, state: catalog.MoISCPJobStateError, errMsg: "dropped"},
		{tableID: 9001, job: "index_i", jobID: 2, state: 2, watermark: testWatermark},
		{tableID: 9001, job: "index_j", jobID: 1, state: catalog.MoISCPJobStateError, errCode: 10000,
			errMsg: "the index cannot store more than max_bad_records = 0 rows; row 2: it's out"},
		{tableID: 9001, job: "index_old", jobID: 1, state: 3, errCode: 1, errMsg: "lost connection"},
	}}}
}

func TestShowIndexStatus(t *testing.T) {
	cc := newShowIndexStatusTestContext(t)
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "show index status from vt where State = 'stopped'", 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(cc, stmt, false)
	require.NoError(t, err)
	query := p.GetQuery()
	require.Equal(t, []string{"Key_name", "Table", "State", "Error_message", "Ignored_rows", "Max_bad_records", "Synced_at"}, query.Headings)
	// the ignored tables are counted on behalf of vt
	require.ElementsMatch(t, []string{"__mo_index_i_ignored", "__mo_index_j_ignored"}, showIndexIgnoredScans(query))
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_TABLE_SCAN {
			require.Equal(t, "vt", node.GetParentObjRef().GetObjName())
		}
	}
	// the jobs of the caller's account, read as the system account that keeps them
	require.Len(t, cc.exec.sqls, 1)
	require.Contains(t, cc.exec.sqls[0], "FROM mo_catalog.mo_iscp_log WHERE account_id = 7 AND table_id IN (9001)")
	require.Equal(t, []uint32{catalog.System_Account}, cc.exec.tenants)

	// a table whose indexes keep no job and no ignored rows has an empty result of the same columns
	cc.exec.rows = nil
	stmt2, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "show index status from nation", 1)
	require.NoError(t, err)
	defer stmt2.Free()
	p, err = BuildPlan(cc, stmt2, false)
	require.NoError(t, err)
	require.Empty(t, showIndexIgnoredScans(p.GetQuery()))
	require.Len(t, p.GetQuery().Headings, 7)
}

func TestShowIndexStatusSql(t *testing.T) {
	cc := newShowIndexStatusTestContext(t)
	defs := []*TableDef{cc.tables["vt"]}
	jobs, err := loadIndexJobs(cc, defs)
	require.NoError(t, err)
	// the latest job of an index is its job
	require.Equal(t, uint64(2), jobs[indexJobKey{9001, "index_i"}].id)

	sql, ignored, err := showIndexStatusSql("tpch", defs, jobs)
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{"__mo_index_i_ignored": {}, "__mo_index_j_ignored": {}}, ignored)
	rows := strings.Split(sql, " UNION ALL ")
	require.Len(t, rows, 3)
	// i: running, synced, with its limit
	require.Contains(t, rows[0], "CAST('i' AS VARCHAR) AS `Key_name`, CAST('vt' AS VARCHAR) AS `Table`, 'running' AS `State`, CAST(NULL AS VARCHAR) AS `Error_message`")
	require.Contains(t, rows[0], "(SELECT COUNT(*) FROM `tpch`.`__mo_index_i_ignored`) AS `Ignored_rows`, CAST(3 AS BIGINT) AS `Max_bad_records`, FROM_UNIXTIME(1730000000000000000 DIV 1000000000) AS `Synced_at`")
	// j: stopped by the limit, never synced, limit 0 when not given
	require.Contains(t, rows[1], "'stopped' AS `State`, 'the index cannot store more than max_bad_records = 0 rows; row 2: it''s out' AS `Error_message`")
	require.Contains(t, rows[1], "CAST(0 AS BIGINT) AS `Max_bad_records`, FROM_UNIXTIME(NULL) AS `Synced_at`")
	// old: no ignored table; its job retries
	require.Contains(t, rows[2], "CAST('old' AS VARCHAR) AS `Key_name`")
	require.Contains(t, rows[2], "'retrying' AS `State`, 'lost connection' AS `Error_message`, CAST(NULL AS BIGINT) AS `Ignored_rows`, CAST(NULL AS BIGINT) AS `Max_bad_records`")

	// an index without a job or an ignored table is not listed
	sql, _, err = showIndexStatusSql("tpch", defs, nil)
	require.NoError(t, err)
	require.Len(t, strings.Split(sql, " UNION ALL "), 2)
	require.NotContains(t, sql, "'old'")

	// an invalid limit is an error
	for _, idx := range defs[0].Indexes {
		if idx.IndexName == "i" {
			idx.IndexAlgoParams = `{"max_bad_records":"x"}`
		}
	}
	_, _, err = showIndexStatusSql("tpch", defs, jobs)
	require.Error(t, err)

	for w, want := range map[string]int64{testWatermark: 1730000000000000000, "": 0, "0-0": 0, "x-1": 0, "12": 0, types.MaxTs().ToString(): 0} {
		require.Equal(t, want, watermarkPhysical(w), w)
	}
}
