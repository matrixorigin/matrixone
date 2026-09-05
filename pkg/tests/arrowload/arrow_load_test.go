// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package arrowload

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

// TestArrowLoadBVT is the embedded public-path BVT suite for `LOAD DATA ...
// format='arrow'` (issue #23684, design doc sections 14 and 18). It runs every
// subtest against one dedicated 1-CN cluster with S3 explicitly enabled and uses
// the real MySQL protocol rather than the internal executor. Local File and Stream
// remain available by default; S3/stage cases prove the explicit opt-in surface.
// It proves type-matrix correctness, option/DDL rejection, multi-object
// atomicity, explicit transactions, cross-session visibility, and local gate
// behavior. Standard distributed CI, mixed binaries, and real cloud providers
// remain separate release evidence.
func TestArrowLoadBVT(t *testing.T) {
	c := startArrowLoadCluster(t, 1, true, true, false)
	db := openArrowLoadDB(t, c, 0)
	mustExec(t, db, "create database if not exists arrow_bvt")
	mustExec(t, db, "use arrow_bvt")

	t.Run("TypeMatrixNumeric", func(t *testing.T) { testArrowTypeMatrixNumeric(t, db) })
	t.Run("TypeMatrixTimestampDict", func(t *testing.T) { testArrowTypeMatrixTimestampDict(t, db) })
	t.Run("TypeMatrixLongBinary", func(t *testing.T) { testArrowTypeMatrixLongBinary(t, db) })
	t.Run("ExplicitColumnOrder", func(t *testing.T) { testArrowExplicitColumnOrder(t, db) })
	t.Run("ArrowContainerSemantics", func(t *testing.T) { testArrowContainerSemantics(t, db) })
	t.Run("NegativeOptionValidation", func(t *testing.T) { testArrowNegativeOptions(t, db) })
	t.Run("DDLRejects", func(t *testing.T) { testArrowDDLRejects(t, db) })
	t.Run("LocalStage", func(t *testing.T) { testArrowLocalStage(t, db) })
	t.Run("MultiObjectSchemaMismatch", func(t *testing.T) { testArrowSchemaMismatchRollback(t, db) })
	t.Run("ConstraintViolationRollback", func(t *testing.T) { testArrowConstraintViolationRollback(t, db) })
	t.Run("CorruptInputRollback", func(t *testing.T) { testArrowCorruptInputRollback(t, db) })
	t.Run("ExplicitTransactionVisibility", func(t *testing.T) { testArrowExplicitTransaction(t, c) })
	t.Run("TwoSessionIsolation", func(t *testing.T) { testArrowTwoSessionIsolation(t, c) })
	t.Run("DifferentialVsInsert", func(t *testing.T) { testArrowDifferentialVsInsert(t, db) })
	t.Run("CommitPhaseFailureRollback", func(t *testing.T) { testArrowCommitPhaseFailureRollback(t, db) })
	t.Run("LocalMinIO", func(t *testing.T) { testArrowLoadLocalMinIO(t, db) })

	// Restart is deliberately last: it destroys every existing SQL connection
	// while preserving the cluster data directory. No later subtest may depend on
	// the original CN generation.
	t.Run("ClusterRestartPersistence", func(t *testing.T) { testArrowClusterRestart(t, c, db) })
}

func testArrowExplicitColumnOrder(t *testing.T, db *sql.DB) {
	mustExec(t, db, "drop table if exists explicit_column_order")
	mustExec(t, db, "create table explicit_column_order(a bigint, b bigint)")
	for _, container := range []string{containerFile, containerStream} {
		t.Run(container, func(t *testing.T) {
			mustExec(t, db, "truncate table explicit_column_order")
			path := fixtureInt64Pair(
				t, t.TempDir(), "explicit_"+container+".arrow", container,
				[]int64{11, 12}, []int64{21, 22},
			)
			mustExec(t, db, fmt.Sprintf(
				"load data infile {'filepath'='%s','format'='arrow'} into table explicit_column_order (b,a)",
				path,
			))
			require.Equal(t, int64(2), queryCount(t, db,
				"select count(*) from explicit_column_order where (a=21 and b=11) or (a=22 and b=12)"))
		})
	}
}

// testArrowCommitPhaseFailureRollback injects the transaction failure after
// workspace batches have been dumped but before commit becomes visible. That is
// later than reader/conversion failures, so this test closes the statement-level
// atomicity contract at the actual commit boundary and then proves the same
// source can be retried after the injection is removed.
func testArrowCommitPhaseFailureRollback(t *testing.T, db *sql.DB) {
	path := fixtureIDName(t, t.TempDir(), "commit-failure.arrow", containerFile,
		[][]idNameRow{{{id: 1, name: "loaded"}}})
	mustExec(t, db, "drop table if exists commit_failure_rollback")
	mustExec(t, db, "create table commit_failure_rollback(id bigint not null, name varchar(50))")
	mustExec(t, db, "insert into commit_failure_rollback values (0, 'seed')")

	fault.Enable()
	defer fault.Disable()
	removeFailure, err := objectio.SimpleInject(objectio.FJ_CNCommitAfterWorkspaceDumpFailed)
	require.NoError(t, err)
	defer removeFailure()

	_, err = db.Exec(fmt.Sprintf(
		"load data infile {'filepath'='%s','format'='arrow'} into table commit_failure_rollback", path))
	require.ErrorContains(t, err, "injected commit failure after workspace dump")
	removeFailure()
	require.Equal(t, int64(1), queryCount(t, db, "select count(*) from commit_failure_rollback"),
		"a failed commit must not expose any Arrow rows")

	mustExec(t, db, fmt.Sprintf(
		"load data infile {'filepath'='%s','format'='arrow'} into table commit_failure_rollback", path))
	require.Equal(t, int64(2), queryCount(t, db, "select count(*) from commit_failure_rollback"))
}

// TestArrowLoadGateDisabled proves the explicit rollback switch fails closed:
// with `cn.frontend.arrow-load.enabled=false`, any Arrow LOAD must be rejected
// before touching the file at all. This checks the client-visible opt-out
// contract, but it does not replace a true mixed-binary-version rehearsal.
func TestArrowLoadGateDisabled(t *testing.T) {
	c := startArrowLoadCluster(t, 1, false, false, false)
	db := openArrowLoadDB(t, c, 0)
	mustExec(t, db, "create database if not exists arrow_gate_off")
	mustExec(t, db, "use arrow_gate_off")
	mustExec(t, db, "create table t(id bigint not null, amount decimal(18,2), score double, flag bool)")
	path := filepath.Join(t.TempDir(), "missing-before-gate.arrow")

	_, err := db.Exec(fmt.Sprintf("load data infile {'filepath'='%s','format'='arrow'} into table t", path))
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "disabled by configuration")
	require.Equal(t, int64(0), queryCount(t, db, "select count(*) from t"))
}

// TestArrowLoadGateS3Disabled proves the default S3 sub-gate fails closed before
// any network I/O. Dummy, unreachable credentials are sufficient proof of the
// ordering: the statement must be rejected by configuration rather than
// attempting HeadObject.
func TestArrowLoadGateS3Disabled(t *testing.T) {
	c := startArrowLoadClusterWithDefaults(t, 1)
	db := openArrowLoadDB(t, c, 0)
	mustExec(t, db, "create database if not exists arrow_s3_gate_off")
	mustExec(t, db, "use arrow_s3_gate_off")
	mustExec(t, db, "create table t(id bigint not null, amount decimal(18,2), score double, flag bool)")

	_, err := db.Exec(
		"load data url s3option " +
			"{'endpoint'='http://127.0.0.1:1','access_key_id'='dummy','secret_access_key'='dummy'," +
			"'bucket'='no-such-bucket','region'='us-east-1'," +
			"'filepath'='does-not-matter.arrow','format'='arrow'} into table t")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "disabled by configuration")
	require.Equal(t, int64(0), queryCount(t, db, "select count(*) from t"))
}

// TestArrowLoadGateDistributedDisabledSoftFallback proves DistributedEnabled=false
// is a soft fallback (silently serialize), not a hard rejection.
func TestArrowLoadGateDistributedDisabledSoftFallback(t *testing.T) {
	c := startArrowLoadCluster(t, 1, true /*enabled*/, true /*s3Enabled*/, false /*distributedEnabled*/)
	db := openArrowLoadDB(t, c, 0)
	mustExec(t, db, "create database if not exists arrow_distributed_off")
	mustExec(t, db, "use arrow_distributed_off")
	mustExec(t, db, "create table t(id bigint not null, name varchar(50))")

	dir := t.TempDir()
	fixtureIDName(t, dir, "part1.arrow", containerFile, [][]idNameRow{{{id: 1, name: "a"}}})
	fixtureIDName(t, dir, "part2.arrow", containerFile, [][]idNameRow{{{id: 2, name: "b"}}})

	mustExec(t, db, fmt.Sprintf(
		"load data infile {'filepath'='%s','format'='arrow'} into table t parallel 'true'",
		filepath.Join(dir, "part*.arrow")))
	require.Equal(t, int64(2), queryCount(t, db, "select count(*) from t"))
}

func testArrowTypeMatrixNumeric(t *testing.T, db *sql.DB) {
	mustExec(t, db, "drop table if exists numeric_matrix")
	mustExec(t, db, "create table numeric_matrix(id bigint not null, amount decimal(18,2), score double, flag bool)")
	rows := []numericRow{
		{id: 1, amount: 100, score: 1.5, flag: true},
		{id: 2, amountNull: true, score: 2.5, flag: false},
		{id: 3, amount: -50000, scoreNull: true, flag: true},
		{id: 4, amount: 0, score: 4.5, flagNull: true},
	}
	for _, container := range []string{containerFile, containerStream} {
		t.Run(container, func(t *testing.T) {
			mustExec(t, db, "truncate table numeric_matrix")
			path := fixtureNumeric(t, t.TempDir(), "numeric_"+container+".arrow", container, rows, 2)
			mustExec(t, db, fmt.Sprintf(
				"load data infile {'filepath'='%s','format'='arrow'} into table numeric_matrix", path))
			require.Equal(t, int64(4), queryCount(t, db, "select count(*) from numeric_matrix"))
			require.Equal(t, int64(1), queryCount(t, db, "select count(*) from numeric_matrix where amount is null"))
			require.Equal(t, int64(1), queryCount(t, db, "select count(*) from numeric_matrix where score is null"))
			require.Equal(t, int64(1), queryCount(t, db, "select count(*) from numeric_matrix where flag is null"))
			require.Equal(t, int64(1), queryCount(t, db,
				"select count(*) from numeric_matrix where id=3 and amount=-500.00"))
			require.Equal(t, int64(1), queryCount(t, db,
				"select count(*) from numeric_matrix where id=1 and amount=1.00 and flag=true"))
		})
	}
}

func testArrowTypeMatrixTimestampDict(t *testing.T, db *sql.DB) {
	mustExec(t, db, "drop table if exists ts_matrix")
	mustExec(t, db, "create table ts_matrix(id bigint not null, ts datetime(6), d date, name varchar(50))")
	loc := time.UTC
	rows := []timestampRow{
		{id: 1, ts: time.Date(2026, 1, 2, 3, 4, 5, 123456000, loc), date: time.Date(2026, 1, 2, 0, 0, 0, 0, loc), name: "alice"},
		{id: 2, tsNull: true, date: time.Date(2026, 1, 3, 0, 0, 0, 0, loc), name: "bob"},
		{id: 3, ts: time.Date(2026, 1, 4, 6, 7, 8, 0, loc), dateNull: true, nameNull: true},
	}
	for _, container := range []string{containerFile, containerStream} {
		t.Run(container, func(t *testing.T) {
			mustExec(t, db, "truncate table ts_matrix")
			path := fixtureTimestampDict(t, t.TempDir(), "ts_"+container+".arrow", container, rows, []string{"alice", "bob"})
			mustExec(t, db, fmt.Sprintf(
				"load data infile {'filepath'='%s','format'='arrow'} into table ts_matrix", path))
			require.Equal(t, int64(3), queryCount(t, db, "select count(*) from ts_matrix"))
			require.Equal(t, int64(1), queryCount(t, db,
				"select count(*) from ts_matrix where id=1 and ts='2026-01-02 03:04:05.123456' and d='2026-01-02' and name='alice'"))
			require.Equal(t, int64(1), queryCount(t, db,
				"select count(*) from ts_matrix where id=2 and ts is null and name='bob'"))
			require.Equal(t, int64(1), queryCount(t, db,
				"select count(*) from ts_matrix where id=3 and d is null and name is null"))
		})
	}
}

func testArrowTypeMatrixLongBinary(t *testing.T, db *sql.DB) {
	mustExec(t, db, "drop table if exists bin_matrix")
	mustExec(t, db, "drop table if exists fixed_bin_matrix")
	mustExec(t, db, "create table bin_matrix(id bigint not null, payload varbinary(200))")
	mustExec(t, db, "create table fixed_bin_matrix(id bigint not null, payload binary(200))")
	longA := strings.Repeat("A", 40)
	longB := strings.Repeat("B", 64)
	rows := []binaryRow{
		{id: 1, payload: []byte(longA)},
		{id: 2, payload: []byte(longB)},
	}
	for _, container := range []string{containerFile, containerStream} {
		t.Run(container, func(t *testing.T) {
			mustExec(t, db, "truncate table bin_matrix")
			mustExec(t, db, "truncate table fixed_bin_matrix")
			path := fixtureLongBinary(t, t.TempDir(), "bin_"+container+".arrow", container, rows)
			mustExec(t, db, fmt.Sprintf(
				"load data infile {'filepath'='%s','format'='arrow'} into table bin_matrix", path))
			mustExec(t, db, fmt.Sprintf(
				"load data infile {'filepath'='%s','format'='arrow'} into table fixed_bin_matrix", path))
			require.Equal(t, int64(2), queryCount(t, db, "select count(*) from bin_matrix"))
			require.Equal(t, int64(1), queryCount(t, db,
				fmt.Sprintf("select count(*) from bin_matrix where id=1 and payload='%s'", longA)))
			require.Equal(t, int64(1), queryCount(t, db,
				fmt.Sprintf("select count(*) from bin_matrix where id=2 and payload='%s'", longB)))
			for _, row := range rows {
				var stored []byte
				require.NoError(t, db.QueryRow(
					"select payload from fixed_bin_matrix where id = ?", row.id,
				).Scan(&stored))
				require.Len(t, stored, 200)
				require.Equal(t, row.payload, stored[:len(row.payload)])
				require.Equal(t, make([]byte, 200-len(row.payload)), stored[len(row.payload):])
			}
		})
	}
}

func testArrowContainerSemantics(t *testing.T, db *sql.DB) {
	mustExec(t, db, "drop table if exists container_semantics")
	mustExec(t, db, "create table container_semantics(id bigint not null, amount decimal(18,2), score double, flag bool)")
	row := []numericRow{{id: 1, amount: 100, score: 1.5, flag: true}}
	filePath := fixtureNumeric(t, t.TempDir(), "container_file.arrow", containerFile, row, 10)
	streamPath := fixtureNumeric(t, t.TempDir(), "container_stream.arrow", containerStream, row, 10)

	for _, arrowContainer := range []string{"auto", "file"} {
		t.Run("file_as_"+arrowContainer, func(t *testing.T) {
			mustExec(t, db, "truncate table container_semantics")
			mustExec(t, db, fmt.Sprintf(
				"load data infile {'filepath'='%s','format'='arrow','arrow_container'='%s'} into table container_semantics",
				filePath, arrowContainer))
			require.Equal(t, int64(1), queryCount(t, db, "select count(*) from container_semantics"))
		})
	}
	for _, arrowContainer := range []string{"auto", "stream"} {
		t.Run("stream_as_"+arrowContainer, func(t *testing.T) {
			mustExec(t, db, "truncate table container_semantics")
			mustExec(t, db, fmt.Sprintf(
				"load data infile {'filepath'='%s','format'='arrow','arrow_container'='%s'} into table container_semantics",
				streamPath, arrowContainer))
			require.Equal(t, int64(1), queryCount(t, db, "select count(*) from container_semantics"))
		})
	}
	t.Run("file_as_stream_rejected", func(t *testing.T) {
		_, err := db.Exec(fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow','arrow_container'='stream'} into table container_semantics",
			filePath))
		require.Error(t, err)
	})
	t.Run("stream_as_file_rejected", func(t *testing.T) {
		_, err := db.Exec(fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow','arrow_container'='file'} into table container_semantics",
			streamPath))
		require.Error(t, err)
	})
	t.Run("invalid_container_value_rejected", func(t *testing.T) {
		_, err := db.Exec(fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow','arrow_container'='flight'} into table container_semantics",
			filePath))
		require.Error(t, err)
	})
}

func testArrowNegativeOptions(t *testing.T, db *sql.DB) {
	mustExec(t, db, "drop table if exists arrow_option_reject")
	mustExec(t, db, "create table arrow_option_reject(id bigint, name varchar(50))")
	path := fixtureIDName(t, t.TempDir(), "option_reject.arrow", containerFile,
		[][]idNameRow{{{id: 1, name: "a"}}})

	cases := []struct {
		name string
		sql  string
	}{
		{"compression", fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow','compression'='gzip'} into table arrow_option_reject", path)},
		{"jsondata", fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow','jsondata'='object'} into table arrow_option_reject", path)},
		{"hive_partitioning", fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow','hive_partitioning'='true'} into table arrow_option_reject", path)},
		{"fields_terminated", fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow'} into table arrow_option_reject fields terminated by ','", path)},
		{"lines_terminated", fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow'} into table arrow_option_reject lines terminated by '\\n'", path)},
		{"ignore_lines", fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow'} into table arrow_option_reject ignore 1 lines", path)},
		{"local_infile", fmt.Sprintf(
			"load data local infile {'filepath'='%s','format'='arrow'} into table arrow_option_reject", path)},
		{"at_variable_column", fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow'} into table arrow_option_reject (id, @name)", path)},
		{"set_assignment", fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow'} into table arrow_option_reject set name=nullif(name,'x')", path)},
		{"invalid_arrow_container", fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow','arrow_container'='flight'} into table arrow_option_reject", path)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Exec(tc.sql)
			require.Error(t, err, tc.sql)
		})
	}
}

func testArrowDDLRejects(t *testing.T, db *sql.DB) {
	arrowPath := fixtureIDName(t, t.TempDir(), "ddl_reject_source.arrow", containerFile,
		[][]idNameRow{{{id: 1, name: "a"}}})

	mustExec(t, db, "drop table if exists arrow_ext_reject")
	_, err := db.Exec(fmt.Sprintf(
		"create external table arrow_ext_reject(id bigint, name varchar(50)) infile{'filepath'='%s','format'='arrow'}",
		arrowPath))
	require.Error(t, err)

	csvDir := t.TempDir()
	csvPath := filepath.Join(csvDir, "placeholder.csv")
	require.NoError(t, os.WriteFile(csvPath, []byte("1,a\n"), 0o600))
	mustExec(t, db, "drop table if exists arrow_into_external")
	mustExec(t, db, fmt.Sprintf(
		"create external table arrow_into_external(id bigint, name varchar(50)) infile{'filepath'='%s','format'='csv'} fields terminated by ','",
		csvPath))
	_, err = db.Exec(fmt.Sprintf(
		"load data infile {'filepath'='%s','format'='arrow'} into table arrow_into_external", arrowPath))
	require.Error(t, err)
}

func testArrowLocalStage(t *testing.T, db *sql.DB) {
	dir := t.TempDir()
	path := fixtureNumeric(t, dir, "stage_source.arrow", containerFile,
		[]numericRow{{id: 1, amount: 100, score: 1.5, flag: true}}, 10)

	mustExec(t, db, "drop stage if exists arrow_local_stage")
	mustExec(t, db, fmt.Sprintf("create stage arrow_local_stage URL='file://%s/'", dir))
	mustExec(t, db, "drop table if exists stage_target")
	mustExec(t, db, "create table stage_target(id bigint not null, amount decimal(18,2), score double, flag bool)")
	mustExec(t, db, fmt.Sprintf(
		"load data infile {'filepath'='stage://arrow_local_stage/%s','format'='arrow'} into table stage_target",
		filepath.Base(path)))
	require.Equal(t, int64(1), queryCount(t, db, "select count(*) from stage_target"))
	mustExec(t, db, "drop stage arrow_local_stage")
}

// testArrowSchemaMismatchRollback proves design invariant I2/I6: two objects in one
// LOAD statement whose Arrow schemas disagree (here, `id` is int64 in one file and
// float64 in the other) must fail the whole statement, leaving only the pre-seeded
// row behind, mirroring load_data_parquet.sql's multi-file negative pattern.
func testArrowSchemaMismatchRollback(t *testing.T, db *sql.DB) {
	dir := t.TempDir()
	fixtureIDName(t, dir, "mismatch_part1.arrow", containerFile, [][]idNameRow{{{id: 1, name: "a"}}})
	fixtureIDNameMismatchedIDType(t, dir, "mismatch_part2.arrow", containerFile, []float64{2}, []string{"b"})

	mustExec(t, db, "drop table if exists schema_mismatch")
	mustExec(t, db, "create table schema_mismatch(id bigint, name varchar(50))")
	mustExec(t, db, "insert into schema_mismatch values (0, 'seed')")

	_, err := db.Exec(fmt.Sprintf(
		"load data infile {'filepath'='%s','format'='arrow'} into table schema_mismatch parallel 'true'",
		filepath.Join(dir, "mismatch_part*.arrow")))
	require.Error(t, err)
	require.Equal(t, int64(1), queryCount(t, db, "select count(*) from schema_mismatch"))
}

// testArrowConstraintViolationRollback proves the same all-or-nothing autocommit
// behavior for a MO-side constraint failure discovered only after the Arrow bridge
// has already converted some rows: a NOT NULL violation split across two same-schema
// files, and a separate INT-range overflow, each leave only the pre-seeded row.
func testArrowConstraintViolationRollback(t *testing.T, db *sql.DB) {
	t.Run("not_null_violation", func(t *testing.T) {
		dir := t.TempDir()
		fixtureIDName(t, dir, "notnull_part1.arrow", containerFile, [][]idNameRow{{{id: 1, name: "a"}}})
		fixtureIDName(t, dir, "notnull_part2.arrow", containerFile, [][]idNameRow{{{idNull: true, name: "b"}}})

		mustExec(t, db, "drop table if exists notnull_violation")
		mustExec(t, db, "create table notnull_violation(id bigint not null, name varchar(50))")
		mustExec(t, db, "insert into notnull_violation values (0, 'seed')")

		_, err := db.Exec(fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow'} into table notnull_violation parallel 'true'",
			filepath.Join(dir, "notnull_part*.arrow")))
		require.Error(t, err)
		require.Equal(t, int64(1), queryCount(t, db, "select count(*) from notnull_violation"))
	})

	t.Run("integer_range_overflow", func(t *testing.T) {
		dir := t.TempDir()
		fixtureInt64Overflow(t, dir, "overflow.arrow", containerFile, []int64{1, 1 << 40})

		mustExec(t, db, "drop table if exists overflow_violation")
		mustExec(t, db, "create table overflow_violation(v int not null)")
		mustExec(t, db, "insert into overflow_violation values (0)")

		_, err := db.Exec(fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow'} into table overflow_violation",
			filepath.Join(dir, "overflow.arrow")))
		require.Error(t, err)
		require.Equal(t, int64(1), queryCount(t, db, "select count(*) from overflow_violation"))
	})
}

// testArrowCorruptInputRollback reaches malformed File and Stream containers
// through the SQL/frontend/transaction path. Each failure must preserve the
// committed seed row, and a subsequent valid LOAD on the same cluster must
// succeed, proving failed reader generations do not poison later statements.
func testArrowCorruptInputRollback(t *testing.T, db *sql.DB) {
	t.Run("file_in_multi_object_load", func(t *testing.T) {
		dir := t.TempDir()
		validPath := fixtureIDName(t, dir, "01-valid.arrow", containerFile,
			[][]idNameRow{{{id: 1, name: "valid"}}})
		require.NoError(t, os.WriteFile(filepath.Join(dir, "02-corrupt.arrow"), []byte("not an Arrow IPC file"), 0o600))

		mustExec(t, db, "drop table if exists corrupt_file_rollback")
		mustExec(t, db, "create table corrupt_file_rollback(id bigint not null, name varchar(50))")
		mustExec(t, db, "insert into corrupt_file_rollback values (0, 'seed')")
		_, err := db.Exec(fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow'} into table corrupt_file_rollback parallel 'true'",
			filepath.Join(dir, "*.arrow")))
		require.Error(t, err)
		require.Equal(t, int64(1), queryCount(t, db, "select count(*) from corrupt_file_rollback"))

		mustExec(t, db, fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow'} into table corrupt_file_rollback", validPath))
		require.Equal(t, int64(2), queryCount(t, db, "select count(*) from corrupt_file_rollback"))
	})

	t.Run("truncated_stream", func(t *testing.T) {
		dir := t.TempDir()
		validPath := fixtureIDName(t, dir, "valid.stream", containerStream,
			[][]idNameRow{{{id: 1, name: "valid"}}})
		payload, err := os.ReadFile(validPath)
		require.NoError(t, err)
		require.Greater(t, len(payload), 16)
		corruptPath := filepath.Join(dir, "truncated.stream")
		require.NoError(t, os.WriteFile(corruptPath, payload[:len(payload)/2], 0o600))

		mustExec(t, db, "drop table if exists corrupt_stream_rollback")
		mustExec(t, db, "create table corrupt_stream_rollback(id bigint not null, name varchar(50))")
		mustExec(t, db, "insert into corrupt_stream_rollback values (0, 'seed')")
		_, err = db.Exec(fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow','arrow_container'='stream'} into table corrupt_stream_rollback",
			corruptPath))
		require.Error(t, err)
		require.Equal(t, int64(1), queryCount(t, db, "select count(*) from corrupt_stream_rollback"))

		mustExec(t, db, fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow','arrow_container'='stream'} into table corrupt_stream_rollback",
			validPath))
		require.Equal(t, int64(2), queryCount(t, db, "select count(*) from corrupt_stream_rollback"))
	})
}

// testArrowExplicitTransaction adapts the begin/rollback and start-transaction/commit
// shape from test/distributed/cases/optimistic/atomicity_1.sql to Arrow LOAD, run
// directly as a Go test (no mo-tester, no @bvt:issue wrapper) against a dedicated
// connection so the transaction stays open across statements.
func testArrowExplicitTransaction(t *testing.T, c embed.Cluster) {
	db := openArrowLoadDB(t, c, 0)
	mustExec(t, db, "use arrow_bvt")
	mustExec(t, db, "drop table if exists txn_visibility")
	mustExec(t, db, "create table txn_visibility(id bigint not null, name varchar(50))")
	path := fixtureIDName(t, t.TempDir(), "txn.arrow", containerFile, [][]idNameRow{{{id: 1, name: "a"}, {id: 2, name: "b"}}})

	mustExec(t, db, "begin")
	mustExec(t, db, fmt.Sprintf(
		"load data infile {'filepath'='%s','format'='arrow'} into table txn_visibility", path))
	require.Equal(t, int64(2), queryCount(t, db, "select count(*) from txn_visibility"))
	mustExec(t, db, "rollback")
	require.Equal(t, int64(0), queryCount(t, db, "select count(*) from txn_visibility"))

	mustExec(t, db, "start transaction")
	mustExec(t, db, fmt.Sprintf(
		"load data infile {'filepath'='%s','format'='arrow'} into table txn_visibility", path))
	mustExec(t, db, "commit")
	require.Equal(t, int64(2), queryCount(t, db, "select count(*) from txn_visibility"))
}

// testArrowTwoSessionIsolation proves an uncommitted Arrow LOAD is invisible to a
// concurrent session until commit, using a real second connection and a channel
// (not a sleep) to sequence "A has loaded but not committed" before B observes.
func testArrowTwoSessionIsolation(t *testing.T, c embed.Cluster) {
	sessionA := openArrowLoadDB(t, c, 0)
	sessionB := openArrowLoadDB(t, c, 0)
	mustExec(t, sessionA, "use arrow_bvt")
	mustExec(t, sessionB, "use arrow_bvt")
	mustExec(t, sessionA, "drop table if exists two_session_isolation")
	mustExec(t, sessionA, "create table two_session_isolation(id bigint not null, name varchar(50))")
	path := fixtureIDName(t, t.TempDir(), "isolation.arrow", containerFile, [][]idNameRow{{{id: 1, name: "a"}}})

	loaded := make(chan struct{})
	committed := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		mustExec(t, sessionA, "begin")
		mustExec(t, sessionA, fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow'} into table two_session_isolation", path))
		close(loaded)
		<-committed
		mustExec(t, sessionA, "commit")
	}()

	<-loaded
	require.Equal(t, int64(0), queryCount(t, sessionB, "select count(*) from two_session_isolation"),
		"uncommitted Arrow LOAD rows must not be visible to another session")
	close(committed)
	wg.Wait()
	require.Equal(t, int64(1), queryCount(t, sessionB, "select count(*) from two_session_isolation"),
		"committed Arrow LOAD rows must become visible to another session")
}

// testArrowDifferentialVsInsert satisfies design section 14.2's differential-testing
// requirement: the same logical rows loaded through the Arrow bridge and through a
// literal INSERT must be indistinguishable.
func testArrowDifferentialVsInsert(t *testing.T, db *sql.DB) {
	mustExec(t, db, "drop table if exists differential_arrow")
	mustExec(t, db, "drop table if exists differential_insert")
	mustExec(t, db, "create table differential_arrow(id bigint not null, amount decimal(18,2), score double, flag bool)")
	mustExec(t, db, "create table differential_insert(id bigint not null, amount decimal(18,2), score double, flag bool)")

	rows := []numericRow{
		{id: 1, amount: 100, score: 1.5, flag: true},
		{id: 2, amountNull: true, score: 2.5, flag: false},
		{id: 3, amount: -50000, scoreNull: true, flag: true},
	}
	path := fixtureNumeric(t, t.TempDir(), "differential.arrow", containerFile, rows, 10)
	mustExec(t, db, fmt.Sprintf(
		"load data infile {'filepath'='%s','format'='arrow'} into table differential_arrow", path))
	mustExec(t, db,
		"insert into differential_insert values (1,1.00,1.5,true), (2,null,2.5,false), (3,-500.00,null,true)")

	require.Equal(t, int64(0), queryCount(t, db,
		"select count(*) from (select * from differential_arrow except select * from differential_insert) x"))
	require.Equal(t, int64(0), queryCount(t, db,
		"select count(*) from (select * from differential_insert except select * from differential_arrow) x"))
}

// testArrowClusterRestart proves both sides of the restart boundary: rows loaded
// before a full embedded-cluster stop/start remain committed, and the restarted
// CN generation can build a fresh Arrow reader and LOAD the same disk object.
func testArrowClusterRestart(t *testing.T, c embed.Cluster, db *sql.DB) {
	path := fixtureIDName(t, t.TempDir(), "restart.arrow", containerFile,
		[][]idNameRow{{{id: 1, name: "before-restart"}}})
	mustExec(t, db, "use arrow_bvt")
	mustExec(t, db, "drop table if exists restart_persistence")
	mustExec(t, db, "create table restart_persistence(id bigint not null, name varchar(50))")
	mustExec(t, db, fmt.Sprintf(
		"load data infile {'filepath'='%s','format'='arrow'} into table restart_persistence", path))
	require.Equal(t, int64(1), queryCount(t, db, "select count(*) from restart_persistence"))

	require.NoError(t, db.Close())
	require.NoError(t, c.Close())
	require.NoError(t, c.Start())

	restartedDB := openArrowLoadDB(t, c, 0)
	mustExec(t, restartedDB, "use arrow_bvt")
	require.Equal(t, int64(1), queryCount(t, restartedDB, "select count(*) from restart_persistence"),
		"a committed Arrow LOAD must survive a complete local cluster restart")
	mustExec(t, restartedDB, "truncate table restart_persistence")
	mustExec(t, restartedDB, fmt.Sprintf(
		"load data infile {'filepath'='%s','format'='arrow'} into table restart_persistence", path))
	require.Equal(t, int64(1), queryCount(t, restartedDB, "select count(*) from restart_persistence"),
		"the restarted CN generation must accept a new Arrow LOAD")
}
