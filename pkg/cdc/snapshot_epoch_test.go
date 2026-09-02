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

package cdc

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/stretchr/testify/require"
)

type snapshotEpochTestExecutor struct {
	tableID uint64
	epoch   string
}

var (
	snapshotEpochSelectID = regexp.MustCompile("source_table_id = ([0-9]+)$")
	snapshotEpochInsert   = regexp.MustCompile(
		"VALUES \\([0-9]+, '[^']*', '[^']*', '[^']*', ([0-9]+), '([^']+)'\\)(?: ON DUPLICATE KEY UPDATE.*)?$")
)

func (e *snapshotEpochTestExecutor) Exec(
	_ context.Context,
	sql string,
	_ ie.SessionOverrideOptions,
) error {
	switch {
	case strings.HasPrefix(sql, "DELETE FROM") && strings.Contains(sql, "mo_cdc_snapshot"):
		// The production cleanup excludes the current generation.
		if strings.Contains(sql, "source_table_id <>") {
			return nil
		}
		e.tableID, e.epoch = 0, ""
		return nil
	case strings.HasPrefix(sql, "INSERT INTO") && strings.Contains(sql, "mo_cdc_snapshot"):
		match := snapshotEpochInsert.FindStringSubmatch(sql)
		if len(match) != 3 {
			return strconv.ErrSyntax
		}
		tableID, err := strconv.ParseUint(match[1], 10, 64)
		if err != nil {
			return err
		}
		e.tableID = tableID
		e.epoch = match[2]
		return nil
	default:
		return strconv.ErrSyntax
	}
}

func (e *snapshotEpochTestExecutor) Query(
	_ context.Context,
	sql string,
	_ ie.SessionOverrideOptions,
) ie.InternalExecResult {
	match := snapshotEpochSelectID.FindStringSubmatch(sql)
	if len(match) != 2 {
		return &InternalExecResultForTest{err: strconv.ErrSyntax}
	}
	tableID, err := strconv.ParseUint(match[1], 10, 64)
	if err != nil {
		return &InternalExecResultForTest{err: err}
	}
	data := [][]interface{}{}
	if tableID == e.tableID && e.epoch != "" {
		data = append(data, []interface{}{e.epoch})
	}
	return &InternalExecResultForTest{resultSet: &MysqlResultSetForTest{Data: data}}
}

func (e *snapshotEpochTestExecutor) ApplySessionOverride(ie.SessionOverrideOptions) {}

func TestInitialSnapshotEpochPersistsPerTableGeneration(t *testing.T) {
	executor := &snapshotEpochTestExecutor{}
	key := &WatermarkKey{
		AccountId: 7,
		TaskId:    "task",
		DBName:    "db",
		TableName: "tbl",
	}
	first := types.BuildTS(100, 3)
	later := types.BuildTS(900, 8)

	updater := NewCDCWatermarkUpdater(t.Name(), executor)
	epoch, err := updater.GetOrCreateInitialSnapshotEpoch(
		context.Background(), key, 11, first)
	require.NoError(t, err)
	require.Equal(t, first, epoch)
	require.Equal(t, uint64(11), executor.tableID)
	require.Equal(t, first.ToString(), executor.epoch)

	// A restart after an intermediate target commit must not select the newer
	// candidate. A new updater models the new executor process with no cache.
	restarted := NewCDCWatermarkUpdater(t.Name()+"-restart", executor)
	epoch, err = restarted.GetOrCreateInitialSnapshotEpoch(
		context.Background(), key, 11, later)
	require.NoError(t, err)
	require.Equal(t, first, epoch)
	require.Equal(t, uint64(11), executor.tableID)
	require.Equal(t, first.ToString(), executor.epoch)

	// Recreating or truncating the table changes its source ID. The retired
	// generation is removed and the new generation receives a fresh epoch.
	epoch, err = restarted.GetOrCreateInitialSnapshotEpoch(
		context.Background(), key, 12, later)
	require.NoError(t, err)
	require.Equal(t, later, epoch)
	require.Equal(t, uint64(12), executor.tableID)
	require.Equal(t, later.ToString(), executor.epoch)
}

func TestInitialSnapshotEpochRejectsInvalidCandidate(t *testing.T) {
	updater := NewCDCWatermarkUpdater(t.Name(), &snapshotEpochTestExecutor{})
	key := &WatermarkKey{AccountId: 1, TaskId: "task", DBName: "db", TableName: "tbl"}

	_, err := updater.GetOrCreateInitialSnapshotEpoch(
		context.Background(), key, 0, types.BuildTS(1, 0))
	require.Error(t, err)
	_, err = updater.GetOrCreateInitialSnapshotEpoch(
		context.Background(), key, 1, types.TS{})
	require.Error(t, err)
}

func TestSnapshotEpochSQLUsesEscapedKeys(t *testing.T) {
	key := &WatermarkKey{AccountId: 1, TaskId: "t'ask", DBName: "d'b", TableName: "t'bl"}
	sql := CDCSQLBuilder.GetSnapshotEpochSQL(key, 9)
	require.Contains(t, sql, "task_id = 't''ask'")
	require.Contains(t, sql, "db_name = 'd''b'")
	require.Contains(t, sql, "table_name = 't''bl'")
	require.Contains(t, CDCSQLBuilder.InsertSnapshotEpochSQL(key, 9, types.BuildTS(10, 1)), "ON DUPLICATE KEY UPDATE snapshot_epoch = snapshot_epoch")
	require.Contains(t, CDCSQLBuilder.DeleteRetiredSnapshotEpochsSQL(key, 9), "source_table_id <> 9")
	require.Contains(t, CDCSQLBuilder.DeleteOrphanSnapshotEpochSQL(), "LEFT JOIN `mo_catalog`.`mo_cdc_task`")
}
