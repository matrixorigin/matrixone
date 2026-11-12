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
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/testutil/portlease"
	"github.com/stretchr/testify/require"
)

type rebaseCrashIdentity struct {
	PortBase               uint64
	BaseTS                 int64
	Committed, Uncommitted [2]uint64
	EdgeCount, PinCount    int
	Inventory              *rebaseInventoryRecoveryIdentity `json:",omitempty"`
	SourceDependency       *rebaseSourceRecoveryIdentity    `json:",omitempty"`
}

var nativeRebaseCrashPhase func(*testing.T, string, string, *sql.DB, context.Context, rebaseCrashIdentity)

type rebaseCrashPorts struct {
	shared  *portlease.Lease
	blocked []*portlease.Lease
	private string
}

func reserveRebaseCrashPorts(allocator portlease.Allocator, root string) (_ *rebaseCrashPorts, err error) {
	shared, err := allocator.Acquire()
	if err != nil {
		return nil, err
	}
	result := &rebaseCrashPorts{shared: shared, private: filepath.Join(root, "private-port-leases")}
	defer func() {
		if err != nil {
			err = errors.Join(err, result.close())
		}
	}()
	private := portlease.Allocator{Directory: result.private}
	for base := portlease.MinPort; base < shared.Base(); base += portlease.Span {
		var blocked *portlease.Lease
		var acquired bool
		blocked, acquired, err = private.TryAcquire(base)
		if err != nil {
			return nil, err
		}
		if !acquired {
			return nil, fmt.Errorf("private recovery port range %d unexpectedly held", base)
		}
		result.blocked = append(result.blocked, blocked)
	}
	return result, nil
}

func (p *rebaseCrashPorts) close() error {
	var err error
	for _, lease := range p.blocked {
		err = errors.Join(err, lease.Close())
	}
	return errors.Join(err, p.shared.Close())
}

func (p *rebaseCrashPorts) closeWhenReaped(children []<-chan struct{}) <-chan error {
	done := make(chan error, 1)
	go func() {
		for _, child := range children {
			<-child
		}
		done <- p.close()
	}()
	return done
}

func TestRebaseCrashPortReservation(t *testing.T) {
	root := t.TempDir()
	public := portlease.Allocator{Directory: filepath.Join(root, "shared")}
	first, err := public.Acquire()
	require.NoError(t, err)
	defer first.Close()
	ports, err := reserveRebaseCrashPorts(public, root)
	require.NoError(t, err)
	held := true
	t.Cleanup(func() {
		if held {
			require.NoError(t, ports.close())
		}
	})
	require.Equal(t, first.Base()+portlease.Span, ports.shared.Base())
	other, acquired, err := public.TryAcquire(ports.shared.Base())
	require.NoError(t, err)
	require.False(t, acquired)
	require.Nil(t, other)
	private := portlease.Allocator{Directory: ports.private}
	for range 2 {
		child, err := private.Acquire()
		require.NoError(t, err)
		require.Equal(t, ports.shared.Base(), child.Base())
		require.NoError(t, child.Close())
	}
	require.NoError(t, ports.close())
	held = false
	reused, acquired, err := public.TryAcquire(ports.shared.Base())
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, reused.Close())
}

func TestRebaseCrashPortRetainedUntilChildReaped(t *testing.T) {
	root := t.TempDir()
	allocator := portlease.Allocator{Directory: filepath.Join(root, "shared")}
	ports, err := reserveRebaseCrashPorts(allocator, root)
	require.NoError(t, err)
	reaped := make(chan struct{})
	defer func() {
		select {
		case <-reaped:
		default:
			close(reaped)
		}
	}()
	closed := ports.closeWhenReaped([]<-chan struct{}{reaped})
	other, acquired, err := allocator.TryAcquire(ports.shared.Base())
	if other != nil {
		defer other.Close()
	}
	require.NoError(t, err)
	require.False(t, acquired)
	require.Nil(t, other)
	close(reaped)
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("port lease was not released after the child was reaped")
	}
	reused, acquired, err := allocator.TryAcquire(ports.shared.Base())
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, reused.Close())
}

// The parent owns no cluster. It kills precisely the ready child, then starts
// a fresh process over the same service identities and volume, never Stop/Start.
func TestRebaseCrashRecovery(t *testing.T) {
	runRebaseCrashRecovery(t, false)
}

func TestNativeRebaseCrashRecovery(t *testing.T) {
	runRebaseCrashRecovery(t, true)
}

func runRebaseCrashRecovery(t *testing.T, native bool) {
	runRebaseCrashRecoveryScenario(t, native, "")
}

func runRebaseCrashRecoveryScenario(t *testing.T, native bool, inventoryMode string) {
	t.Helper()
	if os.Getenv("MO_REBASE_CRASH_CHILD") != "" {
		t.Fatal("parent selected in child")
	}
	require.NoError(t, CloseSingleCNBaseClusterTests())
	root, err := os.MkdirTemp("", "mo-rebase-crash-")
	require.NoError(t, err)
	t.Cleanup(func() {
		if !t.Failed() {
			require.NoError(t, os.RemoveAll(root))
		} else {
			t.Logf("retained crash fixture: %s", root)
		}
	})
	ports, err := reserveRebaseCrashPorts(clusterPortAllocator, root)
	require.NoError(t, err)
	var children []<-chan struct{}
	t.Cleanup(func() {
		for _, child := range children {
			select {
			case <-child:
			default:
				t.Error("crash child not reaped; retaining its port lease")
				ports.closeWhenReaped(children)
				return
			}
		}
		require.NoError(t, ports.close())
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
		cmd.Env = append(os.Environ(), "MO_REBASE_CRASH_CHILD="+phase, "MO_REBASE_CRASH_ROOT="+root,
			"MO_REBASE_CRASH_PORT_BASE="+strconv.FormatUint(ports.shared.Base(), 10),
			"MO_REBASE_CRASH_PORT_DIR="+ports.private)
		if native {
			cmd.Env = append(cmd.Env, "MO_REBASE_NATIVE_CRASH=1")
		}
		if inventoryMode != "" {
			cmd.Env = append(cmd.Env, "MO_REBASE_INVENTORY_RECOVERY="+inventoryMode)
		}
		cmd.Stdout, cmd.Stderr = log, log
		if ready != nil {
			cmd.ExtraFiles = []*os.File{ready}
		}
		err = cmd.Start()
		closeErr := log.Close()
		require.NoError(t, err)
		done := make(chan error, 1)
		reaped := make(chan struct{})
		children = append(children, reaped)
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
		if err == nil {
			if inventoryMode == "" && identity.BaseTS <= 0 {
				err = fmt.Errorf("missing committed base")
			} else if (inventoryMode == "dependency-wal" || inventoryMode == "dependency-checkpoint") &&
				(identity.SourceDependency == nil || identity.SourceDependency.ChangedID == 0) {
				err = fmt.Errorf("missing committed source dependency identity")
			} else if inventoryMode != "" && inventoryMode != "dependency-wal" && inventoryMode != "dependency-checkpoint" &&
				(identity.Inventory == nil || identity.Inventory.SourceID == 0) {
				err = fmt.Errorf("missing committed inventory identity")
			}
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
		stage := "unavailable"
		if data, readErr := os.ReadFile(filepath.Join(root, "verify.stage")); readErr == nil {
			switch string(data) {
			case "identity", "new_cluster", "start_cluster", "sql_client", "inventory", "native", "verify_data", "verify_history":
				stage = string(data)
			}
		}
		require.NoError(t, err, "fresh-process recovery failed at %s; see retained verify log", stage)
	case <-ctx.Done():
		t.Fatal("recovery child did not finish")
	}
	if inventoryMode != "" {
		data, err := os.ReadFile(filepath.Join(root, "identity.json"))
		require.NoError(t, err)
		var identity rebaseCrashIdentity
		require.NoError(t, json.Unmarshal(data, &identity))
		if identity.SourceDependency != nil {
			t.Logf("source dependency recovery mode=%s SIGKILL and fresh-process verification passed", inventoryMode)
			return
		}
		t.Logf("inventory recovery mode=%s source=%d retired=%d replacement=%d checkpoint=%t SIGKILL and fresh-process verification passed",
			inventoryMode, identity.Inventory.SourceID, identity.Inventory.RetiredID,
			identity.Inventory.ReplacementID, identity.Inventory.CheckpointCompleted)
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
	portDir := os.Getenv("MO_REBASE_CRASH_PORT_DIR")
	require.Equal(t, filepath.Join(root, "private-port-leases"), portDir)
	portBase, err := strconv.ParseUint(os.Getenv("MO_REBASE_CRASH_PORT_BASE"), 10, 64)
	require.NoError(t, err)
	previousAllocator := clusterPortAllocator
	clusterPortAllocator = portlease.Allocator{Directory: portDir}
	t.Cleanup(func() { clusterPortAllocator = previousAllocator })
	inventoryMode := os.Getenv("MO_REBASE_INVENTORY_RECOVERY")
	if inventoryMode != "" {
		require.Contains(t, []string{"wal", "checkpoint", "dependency-wal", "dependency-checkpoint"}, inventoryMode)
	}
	require.NoError(t, os.MkdirAll(filepath.Join(root, "data"), 0700))
	markStage := func(stage string) {
		t.Helper()
		require.NoError(t, os.WriteFile(filepath.Join(root, phase+".stage"), []byte(stage), 0600))
	}
	markStage("identity")
	var identity rebaseCrashIdentity
	if phase == "verify" {
		data, err := os.ReadFile(filepath.Join(root, "identity.json"))
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(data, &identity))
	}
	markStage("new_cluster")
	c, err := NewCluster(WithTesting(), WithCNCount(1), WithTestAdmissionContext(t.Context()), func(c *cluster) { c.id = 12345; c.options.dataPath = filepath.Join(root, "data") }, WithPreStart(func(svc ServiceOperator) {
		adjustBasicClusterService(svc)
		if inventoryMode != "" && svc.ServiceType() == metadata.ServiceType_TN {
			svc.Adjust(func(cfg *ServiceConfig) {
				tn := cfg.TN_please_use_getTNServiceConfig
				if tn == nil {
					tn = cfg.TNCompatible
				}
				if tn != nil {
					tn.Txn.EnableRebaseDataHistory = inventoryMode == "dependency-wal" || inventoryMode == "dependency-checkpoint"
					// Separate forced checkpoint recovery from WAL recovery; no
					// scheduled checkpoint should race either crash barrier.
					tn.Ckp.ScanInterval.Duration = time.Hour
					tn.Ckp.FlushInterval.Duration = time.Hour
					tn.Ckp.IncrementalInterval.Duration = time.Hour
				}
			})
		}
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
	require.Equal(t, portBase, c.(*cluster).portLeaseBase, "child must use its parent-reserved port range")
	if phase == "verify" {
		require.Equal(t, identity.PortBase, c.(*cluster).portLeaseBase, "do not change the recovered service addresses")
	} else {
		identity.PortBase = c.(*cluster).portLeaseBase
	}
	markStage("start_cluster")
	require.NoError(t, c.Start())
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	markStage("sql_client")
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
	if inventoryMode != "" {
		markStage("inventory")
		runRebaseInventoryRecoveryPhase(t, phase, inventoryMode, root, cn, db, ctx, identity)
		return
	}
	if os.Getenv("MO_REBASE_NATIVE_CRASH") == "1" {
		markStage("native")
		if nativeRebaseCrashPhase == nil {
			t.Fatal("native Rebase crash fixture is unavailable")
		}
		nativeRebaseCrashPhase(t, phase, root, db, ctx, identity)
		return
	}
	if phase == "verify" {
		markStage("verify_data")
		require.Equal(t, 11, count("select v from rebase_crash_committed.a where id=1"))
		require.Equal(t, 1, count("select count(*) from rebase_crash_committed.b"))
		require.Zero(t, count("select count(*) from mo_catalog.mo_database where datname='rebase_crash_uncommitted'"))
		markStage("verify_history")
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
