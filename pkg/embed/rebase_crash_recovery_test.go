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

package embed

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

type rebaseCrashIdentity struct {
	PortBase               uint64
	BaseTS                 int64
	Committed, Uncommitted [2]uint64
}

// The parent owns no cluster. It kills precisely the ready child, then starts
// a fresh process over the same service identities and volume, never Stop/Start.
func TestRebaseCrashRecovery(t *testing.T) {
	if os.Getenv("MO_REBASE_CRASH_CHILD") != "" {
		t.Fatal("parent selected in child")
	}
	root, err := os.MkdirTemp("", "mo-rebase-crash-")
	require.NoError(t, err)
	t.Cleanup(func() {
		if !t.Failed() {
			require.NoError(t, os.RemoveAll(root))
		} else {
			t.Logf("retained crash fixture: %s", root)
		}
	})
	binary, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	runChild := func(phase string, ready *os.File) (*exec.Cmd, <-chan error) {
		t.Helper()
		log, err := os.OpenFile(filepath.Join(root, phase+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		require.NoError(t, err)
		cmd := exec.CommandContext(ctx, binary, "-test.v", "-test.timeout=210s", "-test.run=^TestRebaseCrashHelper$")
		cmd.Env = append(os.Environ(), "MO_REBASE_CRASH_CHILD="+phase, "MO_REBASE_CRASH_ROOT="+root)
		cmd.Stdout, cmd.Stderr = log, log
		if ready != nil {
			cmd.ExtraFiles = []*os.File{ready}
		}
		err = cmd.Start()
		closeErr := log.Close()
		require.NoError(t, err)
		done := make(chan error, 1)
		reaped := make(chan struct{})
		go func() { err := cmd.Wait(); done <- err; close(reaped) }()
		t.Cleanup(func() {
			select {
			case <-reaped:
				return
			default:
			}
			_ = cmd.Process.Kill()
			select {
			case <-reaped:
			case <-time.After(30 * time.Second):
				t.Error("crash child not reaped")
			}
		})
		require.NoError(t, closeErr)
		return cmd, done
	}
	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer r.Close()
	defer w.Close()
	cmd, done := runChild("prepare", w)
	require.NoError(t, w.Close())
	ready := make(chan error, 1)
	go func() {
		var identity rebaseCrashIdentity
		err := json.NewDecoder(r).Decode(&identity)
		if err == nil && identity.BaseTS <= 0 {
			err = fmt.Errorf("missing committed base")
		}
		ready <- err
	}()
	select {
	case err := <-ready:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("child never reached pre-commit crash barrier")
	}
	require.NoError(t, cmd.Process.Kill())
	select {
	case err = <-done:
	case <-ctx.Done():
		t.Fatal("killed child was not reaped")
	}
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	require.Equal(t, syscall.SIGKILL, exit.Sys().(syscall.WaitStatus).Signal())
	_, verified := runChild("verify", nil)
	select {
	case err := <-verified:
		require.NoError(t, err, "fresh-process recovery failed; see retained verify log")
	case <-ctx.Done():
		t.Fatal("recovery child did not finish")
	}
}

func TestRebaseCrashHelper(t *testing.T) {
	phase := os.Getenv("MO_REBASE_CRASH_CHILD")
	if phase == "" {
		t.Skip("selected only by the crash recovery parent")
	}
	require.Contains(t, []string{"prepare", "verify"}, phase)
	root := os.Getenv("MO_REBASE_CRASH_ROOT")
	require.NotEmpty(t, root)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "data"), 0700))
	var identity rebaseCrashIdentity
	if phase == "verify" {
		data, err := os.ReadFile(filepath.Join(root, "identity.json"))
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(data, &identity))
	}
	c, err := NewCluster(WithTesting(), WithCNCount(1), WithTestAdmissionContext(t.Context()), func(c *cluster) { c.id = 12345; c.options.dataPath = filepath.Join(root, "data") }, WithPreStart(func(svc ServiceOperator) {
		adjustBasicClusterService(svc)
		if svc.ServiceType() == metadata.ServiceType_CN {
			svc.Adjust(func(cfg *ServiceConfig) {
				// A killed generation cannot acknowledge withdrawal. Allow the
				// normal 60s HAKeeper expiry instead of timing out discovery at
				// 30s; do not weaken expiry or admission fencing to pass recovery.
				cfg.CN.HAKeeper.DiscoveryTimeout.Duration = 2 * time.Minute
			})
		}
		if svc.ServiceType() == metadata.ServiceType_LOG {
			svc.Adjust(func(cfg *ServiceConfig) {
				cfg.LogService.UUID = "e84f6390-146b-4d3d-b5c2-c7060829ae11"
				cfg.LogService.BootstrapConfig.InitHAKeeperMembers = []string{"131072:" + cfg.LogService.UUID}
			})
			// Adjust changes config but not the operator's cached identity. This
			// private fixture must restore both before initRuntime, otherwise it
			// registers a runtime under a different UUID from the recovered LOG.
			op := svc.(*operator)
			op.sid = op.cfg.mustGetServiceUUID()
		}
	}))
	if c != nil {
		t.Cleanup(func() { require.NoError(t, c.Close()) })
	}
	require.NoError(t, err)
	if phase == "verify" {
		require.Equal(t, identity.PortBase, c.(*cluster).portLeaseBase, "do not change the recovered service addresses")
	} else {
		identity.PortBase = c.(*cluster).portLeaseBase
	}
	require.NoError(t, c.Start())
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	db := recoverySQLClient(t, cn)
	defer func() { require.NoError(t, db.Close()) }()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	execute := func(q string) { t.Helper(); _, err := db.ExecContext(ctx, q); require.NoError(t, err, q) }
	count := func(q string) int {
		t.Helper()
		var n int
		require.NoError(t, db.QueryRowContext(ctx, q).Scan(&n), q)
		return n
	}
	if phase == "verify" {
		require.Equal(t, 11, count("select v from rebase_crash_committed.a where id=1"))
		require.Equal(t, 1, count("select count(*) from rebase_crash_committed.b"))
		require.Zero(t, count("select count(*) from mo_catalog.mo_database where datname='rebase_crash_uncommitted'"))
		for i, table := range []string{"a", "b"} {
			require.Equal(t, 30, count(fmt.Sprintf("select sum(v) from rebase_crash_source.%s{MO_TS=%d}", table, identity.BaseTS)))
			require.Equal(t, 1, count(fmt.Sprintf("select count(*) from mo_catalog.mo_branch_metadata where table_id=%d and clone_ts=%d", identity.Committed[i], identity.BaseTS)))
			require.Equal(t, 1, count(fmt.Sprintf("select count(*) from mo_catalog.mo_snapshots where kind='branch' and sname='__mo_branch_%d'", identity.Committed[i])))
			require.Zero(t, count(fmt.Sprintf("select count(*) from mo_catalog.mo_branch_metadata where table_id=%d", identity.Uncommitted[i])))
			require.Zero(t, count(fmt.Sprintf("select count(*) from mo_catalog.mo_snapshots where kind='branch' and sname='__mo_branch_%d'", identity.Uncommitted[i])))
		}
		return
	}
	execute("create database rebase_crash_source")
	for _, table := range []string{"a", "b"} {
		execute("create table rebase_crash_source." + table + "(id int primary key,v int)")
		execute("insert into rebase_crash_source." + table + " values(1,10),(2,20)")
	}
	execute("create snapshot rebase_crash_u for database rebase_crash_source")
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	for i, name := range []string{"rebase_crash_committed", "rebase_crash_uncommitted"} {
		for _, q := range []string{"set session transaction isolation level read committed", "begin", "data branch create database " + name + " from rebase_crash_source{snapshot='rebase_crash_u'}", "update " + name + ".a set v=11 where id=1", "delete from " + name + ".b where id=2"} {
			_, err := conn.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		for j, table := range []string{"a", "b"} {
			var id uint64
			var ts int64
			query := "select b.table_id,b.clone_ts from mo_catalog.mo_branch_metadata b join mo_catalog.mo_tables t on t.rel_id=b.table_id where t.reldatabase=? and t.relname=?"
			require.NoError(t, conn.QueryRowContext(ctx, query, name, table).Scan(&id, &ts))
			if i == 0 {
				identity.Committed[j] = id
				if j == 0 {
					identity.BaseTS = ts
				}
				require.Equal(t, identity.BaseTS, ts)
			} else {
				identity.Uncommitted[j] = id
				require.Equal(t, identity.BaseTS, ts)
			}
		}
		if i == 0 {
			_, err := conn.ExecContext(ctx, "commit")
			require.NoError(t, err)
		}
	}
	data, err := json.Marshal(identity)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "identity.json"), data, 0600))
	ready := os.NewFile(3, "ready")
	require.NotNil(t, ready)
	require.NoError(t, json.NewEncoder(ready).Encode(identity))
	require.NoError(t, ready.Close())
	// The parent SIGKILLs at this barrier; no deferred Close/rollback may run.
	<-ctx.Done()
	t.Fatal("parent did not kill the prepared child")
}
