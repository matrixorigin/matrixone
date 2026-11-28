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

package starcount

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

func TestStarCountCombinesSealedBlocksAndReader(t *testing.T) {
	dataDir := t.TempDir()
	// TN flush 会写 transfer page。为此隔离包的本次测试创建新的临时文件服务。
	tmpFS, err := fileservice.NewTestTmpFileService(defines.TmpFileServiceName,
		filepath.Join(dataDir, "tmp-fs"), fileservice.TmpFileGCInterval)
	require.NoError(t, err)
	t.Cleanup(func() { tmpFS.Close(context.Background()) })

	cluster, err := embed.StartTestCluster(embed.WithCNCount(1), embed.WithDataPath(dataDir))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	defer db.Close()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	schema := strings.ToLower(testutils.GetDatabaseName(t))
	table := schema + ".t"
	exec := func(statement string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	exec("create database " + schema)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, err := conn.ExecContext(cleanup, "drop database "+schema)
		require.NoError(t, err)
	}()
	exec("create table " + table + "(id int primary key)")
	flush := func() {
		t.Helper()
		exec("select mo_ctl('dn','flush','" + table + "')")
		service := cn.RawService().(cnservice.Service)
		frontier, _ := service.GetClock().Now()
		_, err := service.GetTxnClient().WaitLogTailAppliedAt(ctx, frontier)
		require.NoError(t, err)
	}
	exec("insert into " + table + " values (1),(2),(3),(4)")
	flush()
	exec("delete from " + table + " where id = 2")
	flush()

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	// 工作区写入在本次查询前不会被 TN 自动封存。
	_, err = tx.ExecContext(ctx, "insert into "+table+" values (11),(12),(13),(14)")
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "delete from "+table+" where id in (3,12)")
	require.NoError(t, err)

	columnScan := "select id from " + table + " order by id"
	visible := func() []int {
		rows, err := tx.QueryContext(ctx, columnScan)
		require.NoError(t, err)
		defer rows.Close()
		var ids []int
		for rows.Next() {
			var id int
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())
		return ids
	}()
	require.Equal(t, []int{1, 4, 11, 13, 14}, visible)
	// 两组可见行数各不相同，缺少任何一组都会改变最终结果。
	sealedRows, readerRows := 0, 0
	for _, id := range visible {
		if id < 10 {
			sealedRows++
		} else {
			readerRows++
		}
	}
	require.Equal(t, 2, sealedRows)
	require.Equal(t, 3, readerRows)

	scanPattern := regexp.MustCompile("Table Scan on " + regexp.QuoteMeta(table) + `[^\n]*\n[^\n]*Analyze:[^\n]*\binputRows=(\d+)`)
	explainScanRows := func(query string) int {
		t.Helper()
		explain, err := testutils.QueryText(ctx, tx, "explain analyze "+query)
		require.NoError(t, err)
		match := scanPattern.FindStringSubmatch(explain)
		require.Len(t, match, 2, explain)
		n, err := strconv.Atoi(match[1])
		require.NoError(t, err)
		return n
	}
	require.Equal(t, len(visible), explainScanRows(columnScan))
	countSQL := "select count(*) from " + table
	require.Zero(t, explainScanRows(countSQL), "小表缺省使用 StarCount 快路径")

	if fault.EnableDomain(fault.DomainTest) {
		defer fault.DisableDomain(fault.DomainTest)
	}
	point := "starcount_tombstone_estimate:" + table
	require.NoError(t, fault.AddFaultPointInDomain(ctx, fault.DomainTest, point, ":::", "echo", 5_000_001, "", false))
	defer func() {
		_, err := fault.RemoveFaultPointFromDomain(context.Background(), fault.DomainTest, point)
		require.NoError(t, err)
	}()

	var actual int
	require.NoError(t, tx.QueryRowContext(ctx, countSQL).Scan(&actual))
	require.Equal(t, len(visible), actual)

	// 封存块中有真实可见的删除，通用聚合回退也会把它交给扫描。
	// 因此扫描行数恰为 readerRows 可区分封存块优化、通用回退及 StarCount。
	scanned := explainScanRows(countSQL)
	require.Equal(t, readerRows, scanned)
	t.Logf("普通列扫描可见 %d 行，重 COUNT 返回 %d 行，其中封存块 %d 行，普通 reader %d 行", len(visible), actual, sealedRows, scanned)
}
