// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multicn

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

// Both requests pass the destination precheck and copy real source objects
// before either publishes. Different source markers make a mixed archive
// observable through LOAD, even when both schemas are identical.
func TestTableDumpConcurrentPublication(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	dbs := make([]*sql.DB, 2)
	for i := range dbs {
		cn, err := cluster.GetCNService(i)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		dbs[i] = db
	}
	exec := func(db *sql.DB, query string) {
		t.Helper()
		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err, query)
	}
	waitCrossCN := func() {
		t.Helper()
		writer, err := cluster.GetCNService(0)
		require.NoError(t, err)
		reader, err := cluster.GetCNService(1)
		require.NoError(t, err)
		frontier := writer.RawService().(cnservice.Service).GetTxnClient().GetLatestCommitTS()
		require.False(t, frontier.IsEmpty(), "writer must publish a commit timestamp")
		waitCtx, stop := context.WithTimeout(ctx, 10*time.Second)
		defer stop()
		snapshot, err := reader.RawService().(cnservice.Service).GetTxnClient().WaitLogTailAppliedAt(waitCtx, frontier)
		require.NoError(t, err)
		require.True(t, frontier.Less(snapshot), "reader snapshot must advance past the fixture commit")
	}
	const schema = "dump_publication"
	exec(dbs[0], "create database "+schema)
	for i := 0; i < 2; i++ {
		table := fmt.Sprintf("%s.src%d", schema, i)
		exec(dbs[0], "create table "+table+" (id int primary key, marker int)")
		exec(dbs[0], fmt.Sprintf("insert into %s values (1,%d),(2,%d)", table, i, i))
		exec(dbs[0], "select mo_ctl('dn','flush','"+table+"')")
	}
	root := t.TempDir()
	alias := root + "-alias"
	require.NoError(t, os.Symlink(root, alias))
	t.Cleanup(func() { require.NoError(t, os.Remove(alias)) })
	for i, path := range []string{root, alias} {
		exec(dbs[0], fmt.Sprintf("create stage dump_pub_%d url='file://%s/'", i, path))
	}
	// SQL completion on CN0 does not imply CN1 has applied its catalog/data.
	// Establish fixture visibility before either publisher reaches the barrier.
	waitCrossCN()
	if fault.Enable() {
		t.Cleanup(func() { fault.Disable() })
	}
	barrier := func(name string) (wait func(int64), release func()) {
		t.Helper()
		require.NoError(t, fault.AddFaultPoint(ctx, name, ":::", "wait", 0, "", false))
		t.Cleanup(func() {
			for _, point := range []string{name, name + "_count", name + "_release"} {
				_, err := fault.RemoveFaultPoint(context.Background(), point)
				require.NoError(t, err)
			}
		})
		require.NoError(t, fault.AddFaultPoint(ctx, name+"_count", ":::", "getwaiters", 0, name, false))
		require.NoError(t, fault.AddFaultPoint(ctx, name+"_release", ":::", "notifyall", 0, name, false))
		release = func() { fault.TriggerFault(name + "_release") }
		t.Cleanup(release)
		wait = func(n int64) {
			t.Helper()
			require.Eventually(t, func() bool {
				count, _, ok := fault.TriggerFault(name + "_count")
				return ok && count == n
			}, 30*time.Second, 10*time.Millisecond)
		}
		return
	}
	wait, release := barrier("table_dump_before_manifest")
	type result struct {
		source int
		err    error
	}
	done := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			_, err := dbs[i].ExecContext(ctx, fmt.Sprintf("dump table %s.src%d to 'stage://dump_pub_%d/race'", schema, i, i))
			done <- result{i, err}
		}(i)
	}
	// The aliases resolve to the same filesystem directory on both CNs.
	wait(2)
	release()
	winner := -1
	for i := 0; i < 2; i++ {
		select {
		case r := <-done:
			if r.err == nil {
				require.Equal(t, -1, winner, "both publishers succeeded")
				winner = r.source
			} else {
				require.Contains(t, r.err.Error(), "already exists")
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	require.NotEqual(t, -1, winner)
	manifestBytes, err := os.ReadFile(filepath.Join(root, "race", "manifest.json"))
	require.NoError(t, err)
	var manifest struct {
		SourceTable string `json:"source_table"`
	}
	require.NoError(t, json.Unmarshal(manifestBytes, &manifest))
	require.Equal(t, fmt.Sprintf("src%d", winner), manifest.SourceTable)
	ready, err := os.ReadFile(filepath.Join(root, "race", "READY"))
	require.NoError(t, err)
	require.Equal(t, "1", string(ready))
	exec(dbs[0], "create table "+schema+".dst like "+schema+".src0")
	exec(dbs[0], "load table "+schema+".dst from 'stage://dump_pub_0/race'")
	var count, sum, minMarker, maxMarker int
	require.NoError(t, dbs[0].QueryRowContext(ctx, "select count(*),sum(id),min(marker),max(marker) from "+schema+".dst").Scan(&count, &sum, &minMarker, &maxMarker))
	require.Equal(t, 2, count)
	require.Equal(t, 3, sum)
	require.Equal(t, winner, minMarker)
	require.Equal(t, winner, maxMarker)
	_, err = fault.RemoveFaultPoint(ctx, "table_dump_before_manifest")
	require.NoError(t, err)

	// Cancellation after manifest creation must leave an incomplete archive;
	// neither LOAD nor a competing publisher may treat it as committed.
	waitAfter, _ := barrier("table_dump_after_manifest")
	conn, err := dbs[0].Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	var connectionID uint64
	require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
	cancelled := make(chan error, 1)
	go func() {
		_, err := conn.ExecContext(ctx, "dump table "+schema+".src0 to 'stage://dump_pub_0/cancelled'")
		cancelled <- err
	}()
	waitAfter(1)
	exec(dbs[0], fmt.Sprintf("kill query %d", connectionID))
	select {
	case err := <-cancelled:
		require.Error(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitAfter(0)
	_, err = os.Stat(filepath.Join(root, "cancelled", "READY"))
	require.True(t, os.IsNotExist(err), "%v", err)
	exec(dbs[0], "create table "+schema+".incomplete like "+schema+".src0")
	// LOAD must reach the archive check, rather than race table visibility.
	waitCrossCN()
	_, err = dbs[1].ExecContext(ctx, "load table "+schema+".incomplete from 'stage://dump_pub_1/cancelled'")
	require.ErrorContains(t, err, "READY marker is missing")
	_, err = dbs[1].ExecContext(ctx, "dump table "+schema+".src1 to 'stage://dump_pub_1/cancelled'")
	require.ErrorContains(t, err, "already exists")
	_, err = fault.RemoveFaultPoint(ctx, "table_dump_after_manifest")
	require.NoError(t, err)
	// Reuse the cancelled connection immediately on a new destination.
	_, err = conn.ExecContext(ctx, "dump table "+schema+".src0 to 'stage://dump_pub_0/retry'")
	require.NoError(t, err)
	exec(dbs[1], "load table "+schema+".incomplete from 'stage://dump_pub_1/retry'")
	require.NoError(t, dbs[1].QueryRowContext(ctx, "select count(*),sum(id),min(marker),max(marker) from "+schema+".incomplete").Scan(&count, &sum, &minMarker, &maxMarker))
	require.Equal(t, 2, count)
	require.Equal(t, 3, sum)
	require.Zero(t, minMarker)
	require.Zero(t, maxMarker)
}
