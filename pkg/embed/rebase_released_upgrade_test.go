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
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil/clusteradmission"
	"github.com/stretchr/testify/require"
)

type releasedInventoryIdentity struct {
	SQLVersion       string
	SourceID         uint64
	TenantID         uint32
	TenantDatabaseID uint64
}

// The wrapper provides a digest-pinned official binary. No download, synthetic
// version edits, production switch or benchmark is performed by this test.
func TestRebaseReleasedBinaryUpgrade(t *testing.T) {
	binary, root := os.Getenv("MO_REBASE_RELEASE_BINARY"), os.Getenv("MO_REBASE_RELEASE_BRIDGE")
	if binary == "" || root == "" {
		t.Skip("requires the frozen official-release qualification wrapper")
	}
	require.True(t, filepath.IsAbs(binary) && filepath.IsAbs(root))
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	require.NoError(t, os.Mkdir(root, 0700))
	require.NoError(t, os.Mkdir(filepath.Join(root, "data"), 0700))
	ctx, cancel := context.WithTimeout(t.Context(), 9*time.Minute)
	defer cancel()
	c, err := newReleasedUpgradeCluster(ctx, root)
	if c != nil {
		defer func() { require.NoError(t, c.Close()) }()
	}
	require.NoError(t, err)
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	port := cn.GetServiceConfig().CN.Frontend.Port
	identity := prepareReleasedInventory(t, ctx, c.(*cluster), binary, root, port)
	dataPath, backup := filepath.Join(root, "data"), filepath.Join(root, "old-cold-backup")
	before := releasedVolumeHashes(t, dataPath)
	require.NotEmpty(t, before)
	copyReleasedVolume(t, dataPath, backup)
	require.Equal(t, before, releasedVolumeHashes(t, backup), "closed release backup must be byte-identical")
	backupManifest, err := json.Marshal(before)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "cold-backup-manifest.json"), backupManifest, 0600))
	first := verifyReleasedInventoryUpgrade(t, ctx, c, port, identity)
	require.NoError(t, c.Stop())
	// This is a same-candidate restart, not a migration interruption claim.
	require.NoError(t, c.Start())
	db := releasedSQLClient(t, port, false)
	defer db.Close()
	require.Equal(t, first, releasedScalar(t, ctx, db,
		fmt.Sprintf("select epoch from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", identity.SourceID)))
	require.EqualValues(t, 1, releasedScalar(t, ctx, db, "select count(*) from nr_release_source.added where id=9 and v=90"))
	require.NoError(t, db.Close())
	require.NoError(t, c.Stop())
	// Never downgrade upgraded data. Keep it separately and restore only the
	// original closed release bytes before opening them with the candidate.
	require.NoError(t, os.Rename(dataPath, filepath.Join(root, "upgraded-retained")))
	copyReleasedVolume(t, backup, dataPath)
	require.Equal(t, before, releasedVolumeHashes(t, dataPath))
	restored := verifyReleasedInventoryUpgrade(t, ctx, c, port, identity)
	require.Equal(t, first, restored, "restored old data must initialize, not inherit candidate in-memory epoch")
	require.NoError(t, c.Stop())
	result, err := json.Marshal(map[string]any{
		"status": "PASS_RELEASED_DATA_AND_COLD_BACKUP_ONLY", "old_identity": identity,
		"target": v4_0_7.Handler.Metadata(), "epoch": first, "backup_files": len(before),
		"same_candidate_restart": true, "cold_backup_restored": true, "cleanup_complete": true,
		"migration_interruption_verified": false, "production_R2_enabled": false,
	})
	require.NoError(t, err)
	require.NoError(t, c.Close())
	require.NoError(t, os.WriteFile(filepath.Join(root, "result.json"), result, 0600))
}

func newReleasedUpgradeCluster(ctx context.Context, root string) (Cluster, error) {
	return NewCluster(WithTesting(), WithCNCount(1), WithTestAdmissionContext(ctx), func(c *cluster) {
		c.id = 12346
		c.options.dataPath = filepath.Join(root, "data")
	}, WithPreStart(func(op ServiceOperator) {
		adjustBasicClusterService(op)
		op.Adjust(func(cfg *ServiceConfig) {
			cfg.IsStandalone = true
			if op.ServiceType() == metadata.ServiceType_CN {
				cfg.CN.AutomaticUpgrade = true
				cfg.CN.Txn.Mode = pbtxn.TxnMode_Pessimistic.String()
				cfg.CN.Txn.Isolation = pbtxn.TxnIsolation_RC.String()
				cfg.CN.Frontend.UnixSocketAddress = ""
				cfg.CN.HAKeeper.DiscoveryTimeout.Duration = 2 * time.Minute
			}
			if op.ServiceType() == metadata.ServiceType_LOG {
				cfg.LogService.UUID = "a64d950f-f349-45c7-a1ed-5650c5e3059f"
				cfg.LogService.BootstrapConfig.InitHAKeeperMembers = []string{"131072:" + cfg.LogService.UUID}
			}
		})
		if op.ServiceType() == metadata.ServiceType_LOG {
			operator := op.(*operator)
			operator.sid = operator.cfg.mustGetServiceUUID()
		}
		if op.ServiceType() == metadata.ServiceType_CN {
			op.(*operator).testingCNOptions = []cnservice.Option{cnservice.WithBootstrapOptions(
				bootstrap.WithCheckUpgradeDuration(100*time.Millisecond),
				bootstrap.WithCheckUpgradeTenantDuration(100*time.Millisecond),
				bootstrap.WithCheckUpgradeTenantWorkers(1), bootstrap.WithUpgradeTenantBatch(1))}
		}
	}))
}

// The old executable creates real 4.0.6 data. A separate candidate process is
// killed after its ordinary-tenant backfill commit, before tenant readiness.
func TestRebaseReleasedUpgradeCrashRecovery(t *testing.T) {
	binary, bridge := os.Getenv("MO_REBASE_RELEASE_BINARY"), os.Getenv("MO_REBASE_RELEASE_BRIDGE")
	if binary == "" || bridge == "" {
		t.Skip("requires the frozen official-release qualification wrapper")
	}
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	root := bridge + "-crash"
	require.NoError(t, os.Mkdir(root, 0700))
	require.NoError(t, os.Mkdir(filepath.Join(root, "data"), 0700))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	c, err := newReleasedUpgradeCluster(ctx, root)
	if c != nil {
		defer func() { require.NoError(t, c.Close()) }()
	}
	require.NoError(t, err)
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	old := prepareReleasedInventory(t, ctx, c.(*cluster), binary, root, cn.GetServiceConfig().CN.Frontend.Port)
	portBase := c.(*cluster).portLeaseBase
	require.NoError(t, c.Close())
	identity, err := json.Marshal(struct {
		Old      releasedInventoryIdentity
		PortBase uint64
	}{old, portBase})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "crash-identity.json"), identity, 0600))
	executable, err := os.Executable()
	require.NoError(t, err)
	start := func(phase string, ready *os.File) (*exec.Cmd, <-chan error) {
		t.Helper()
		log, err := os.OpenFile(filepath.Join(root, phase+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		require.NoError(t, err)
		command := exec.CommandContext(ctx, executable, "-test.v", "-test.timeout=240s", "-test.run=^TestRebaseReleasedUpgradeCrashHelper$")
		command.Env = append(os.Environ(), "MO_REBASE_RELEASE_CRASH_PHASE="+phase, "MO_REBASE_RELEASE_CRASH_ROOT="+root)
		command.Stdout, command.Stderr = log, log
		if ready != nil {
			command.ExtraFiles = []*os.File{ready}
		}
		err = command.Start()
		closeErr := log.Close()
		require.NoError(t, err)
		done := make(chan error, 1)
		reaped := make(chan struct{})
		go func() { done <- command.Wait(); close(reaped) }()
		t.Cleanup(func() {
			select {
			case <-reaped:
				return
			default:
			}
			_ = command.Process.Kill()
			select {
			case <-reaped:
			case <-time.After(30 * time.Second):
				t.Error("upgrade crash child not reaped")
			}
		})
		require.NoError(t, closeErr)
		return command, done
	}
	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer r.Close()
	defer w.Close()
	child, done := start("prepare", w)
	require.NoError(t, w.Close())
	ready := make(chan error, 1)
	go func() {
		var signal [1]byte
		_, err := io.ReadFull(r, signal[:])
		if err == nil && signal[0] != 1 {
			err = fmt.Errorf("invalid upgrade commit barrier")
		}
		ready <- err
	}()
	select {
	case err := <-ready:
		require.NoError(t, err, "upgrade child exited before confirmed backfill commit")
	case <-ctx.Done():
		t.Fatal("upgrade commit barrier timed out")
	}
	require.NoError(t, child.Process.Kill())
	select {
	case err := <-done:
		var exited *exec.ExitError
		require.ErrorAs(t, err, &exited)
		require.Equal(t, syscall.SIGKILL, exited.Sys().(syscall.WaitStatus).Signal())
	case <-ctx.Done():
		t.Fatal("upgrade child did not terminate")
	}
	_, verify := start("verify", nil)
	select {
	case err := <-verify:
		require.NoError(t, err, "fresh-process upgrade recovery failed; raw child log retained")
	case <-ctx.Done():
		t.Fatal("fresh-process upgrade recovery timed out")
	}
}

func TestRebaseReleasedUpgradeCrashHelper(t *testing.T) {
	phase := os.Getenv("MO_REBASE_RELEASE_CRASH_PHASE")
	if phase == "" {
		t.Skip("selected only by the released-upgrade crash parent")
	}
	require.Contains(t, []string{"prepare", "verify"}, phase)
	root := os.Getenv("MO_REBASE_RELEASE_CRASH_ROOT")
	var identity struct {
		Old      releasedInventoryIdentity
		PortBase uint64
	}
	data, err := os.ReadFile(filepath.Join(root, "crash-identity.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &identity))
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	if phase == "prepare" {
		var signalled atomic.Bool
		restore := versions.SetInventoryUpgradeCommittedHookForTest(func(ctx context.Context, account int32, operation string) {
			if uint32(account) != identity.Old.TenantID || operation != "backfill database inventory" || !signalled.CompareAndSwap(false, true) {
				return
			}
			receipt, err := json.Marshal(map[string]any{"account_id": account, "operation": operation, "owner_commit_confirmed": true})
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(root, "committed-barrier.json"), receipt, 0600))
			pipe := os.NewFile(3, "upgrade-commit-ready")
			_, err = pipe.Write([]byte{1})
			require.NoError(t, err)
			require.NoError(t, pipe.Close())
			<-ctx.Done()
		})
		defer restore()
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	c, err := newReleasedUpgradeCluster(ctx, root)
	if c != nil {
		defer func() { require.NoError(t, c.Close()) }()
	}
	require.NoError(t, err)
	require.Equal(t, identity.PortBase, c.(*cluster).portLeaseBase)
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	if phase == "prepare" {
		_ = c.Start()
		t.Fatal("upgrade commit barrier was not reached")
	}
	epoch := verifyReleasedInventoryUpgrade(t, ctx, c, cn.GetServiceConfig().CN.Frontend.Port, identity.Old)
	require.NoError(t, c.Close())
	receipt, err := json.Marshal(map[string]any{"status": "PASS_RELEASED_MIGRATION_SIGKILL_RECOVERY", "epoch": epoch, "cleanup_complete": true})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "result.json"), receipt, 0600))
}

func releasedSQLClient(t *testing.T, port int64, tenant bool) *sql.DB {
	t.Helper()
	user := "dump"
	if tenant {
		user = "nr_release_tenant#root#accountadmin"
	}
	db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/?timeout=3s&readTimeout=10s&writeTimeout=10s", user, port))
	require.NoError(t, err)
	return db
}

func releasedScalar(t *testing.T, ctx context.Context, db *sql.DB, query string) uint64 {
	t.Helper()
	var value uint64
	require.NoError(t, db.QueryRowContext(ctx, query).Scan(&value), query)
	return value
}

func prepareReleasedInventory(t *testing.T, ctx context.Context, c *cluster, binary, root string, port int64) releasedInventoryIdentity {
	t.Helper()
	admission, err := clusteradmission.Acquire(ctx, clusteradmission.Exclusive)
	require.NoError(t, err)
	released := true
	defer func() {
		if released {
			require.NoError(t, admission.Release())
		}
	}()
	configs := map[string][]string{}
	for i, op := range c.services {
		path := filepath.Join(root, fmt.Sprintf("release-service-%d.toml", i))
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		require.NoError(t, err)
		encodeErr := toml.NewEncoder(file).Encode(releasedServiceConfiguration(op.cfg))
		closeErr := file.Close()
		require.NoError(t, encodeErr)
		require.NoError(t, closeErr)
		key := map[metadata.ServiceType]string{metadata.ServiceType_LOG: "logservices", metadata.ServiceType_TN: "tnservices", metadata.ServiceType_CN: "cnservices"}[op.serviceType]
		require.NotEmpty(t, key)
		configs[key] = append(configs[key], path)
	}
	launch := filepath.Join(root, "release-launch.toml")
	file, err := os.OpenFile(launch, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	require.NoError(t, err)
	encodeErr := toml.NewEncoder(file).Encode(configs)
	closeErr := file.Close()
	require.NoError(t, encodeErr)
	require.NoError(t, closeErr)
	log, err := os.OpenFile(filepath.Join(root, "release-service.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	require.NoError(t, err)
	defer log.Close()
	command := exec.CommandContext(ctx, binary, "-launch", launch)
	command.Dir = root
	command.Stdout, command.Stderr = log, log
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "LD_LIBRARY_PATH=") {
			command.Env = append(command.Env, value)
		}
	}
	command.Env = append(command.Env, "LD_LIBRARY_PATH="+filepath.Join(filepath.Dir(binary), "lib"))
	require.NoError(t, command.Start())
	released = false
	exited := make(chan struct{})
	var waitErr error
	go func() { waitErr = command.Wait(); close(exited) }()
	defer func() {
		select {
		case <-exited:
			released = true
		default:
			_ = command.Process.Kill()
			select {
			case <-exited:
				released = true
			case <-time.After(30 * time.Second):
				t.Error("official release child did not terminate; admission retained")
			}
		}
	}()
	db := releasedSQLClient(t, port, false)
	defer db.Close()
	ready, stopReady := context.WithTimeout(ctx, 90*time.Second)
	defer stopReady()
	for {
		ping, cancel := context.WithTimeout(ready, time.Second)
		err = db.PingContext(ping)
		cancel()
		if err == nil {
			break
		}
		select {
		case <-exited:
			t.Fatalf("official release exited before SQL readiness: %v", waitErr)
		case <-ready.Done():
			t.Fatal("official release did not reach SQL readiness")
		case <-time.After(100 * time.Millisecond):
		}
	}
	var identity releasedInventoryIdentity
	require.NoError(t, db.QueryRowContext(ctx, "select version()").Scan(&identity.SQLVersion))
	require.Contains(t, identity.SQLVersion, "4.2.5")
	var catalogVersion string
	require.NoError(t, db.QueryRowContext(ctx, "select version from mo_catalog.mo_version where state=2 order by create_at desc limit 1").Scan(&catalogVersion))
	require.Equal(t, "4.0.6", catalogVersion)
	require.Zero(t, releasedScalar(t, ctx, db, "select count(*) from information_schema.tables where table_schema='mo_catalog' and table_name='mo_database_inventory'"))
	for _, query := range []string{"create database nr_release_source", "create table nr_release_source.t(id int primary key,v int)",
		"insert into nr_release_source.t values(1,10),(2,20)", "create account nr_release_tenant admin_name 'root' identified by '111'"} {
		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err)
	}
	identity.SourceID = releasedScalar(t, ctx, db, "select dat_id from mo_catalog.mo_database where account_id=0 and datname='nr_release_source'")
	identity.TenantID = uint32(releasedScalar(t, ctx, db, "select account_id from mo_catalog.mo_account where account_name='nr_release_tenant'"))
	tenant := releasedSQLClient(t, port, true)
	defer tenant.Close()
	for _, query := range []string{"create database nr_release_source", "create table nr_release_source.t(id int primary key,v int)", "insert into nr_release_source.t values(7,70)"} {
		_, err := tenant.ExecContext(ctx, query)
		require.NoError(t, err)
	}
	identity.TenantDatabaseID = releasedScalar(t, ctx, tenant, "select dat_id from mo_catalog.mo_database where account_id=current_account_id() and datname='nr_release_source'")
	require.NoError(t, tenant.Close())
	require.NoError(t, db.Close())
	require.NoError(t, command.Process.Signal(syscall.SIGTERM))
	select {
	case <-exited:
		require.NoError(t, waitErr)
	case <-time.After(time.Minute):
		t.Fatal("official release did not finish normal shutdown")
	}
	identityFile, err := json.Marshal(identity)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "old-identity.json"), identityFile, 0600))
	return identity
}

// Only export stable declarative transport/storage fields. Runtime configuration
// also contains VFS functions and cache callbacks, which are not TOML settings.
func releasedServiceConfiguration(cfg ServiceConfig) map[string]any {
	result := map[string]any{
		"service-type": cfg.ServiceType, "data-dir": cfg.DataDir,
		"log":             map[string]any{"level": "info", "format": "console", "max-size": 64},
		"hakeeper-client": cfg.HAKeeperClient,
		"observability":   map[string]any{"disableMetric": true, "disableTrace": true, "enable-metric-to-prom": false},
	}
	services := make([]map[string]any, 0, len(cfg.FileServices))
	for _, service := range cfg.FileServices {
		item := map[string]any{"name": service.Name, "backend": service.Backend, "data-dir": service.DataDir}
		if service.Backend == "S3" {
			item["s3"] = map[string]any{"endpoint": service.S3.Endpoint, "bucket": service.S3.Bucket}
		}
		cache := map[string]any{}
		if service.Cache.MemoryCapacity != nil {
			cache["memory-capacity"] = service.Cache.MemoryCapacity
		}
		if service.Cache.DiskCapacity != nil {
			cache["disk-capacity"] = service.Cache.DiskCapacity
		}
		if service.Cache.DiskPath != nil {
			cache["disk-path"] = service.Cache.DiskPath
		}
		item["cache"] = cache
		services = append(services, item)
	}
	result["fileservice"] = services
	switch cfg.ServiceType {
	case "LOG":
		log := cfg.LogService
		result["logservice"] = map[string]any{
			"uuid": log.UUID, "deployment-id": log.DeploymentID, "rttmillisecond": log.RTTMillisecond,
			"service-host": log.ServiceHost, "logservice-port": log.LogServicePort, "raft-port": log.RaftPort,
			"logservice-address": log.ServiceAddress, "logservice-listen-address": log.ServiceListenAddress,
			"raft-address": log.RaftAddress, "raft-listen-address": log.RaftListenAddress,
			"gossip-address": log.GossipAddress, "gossip-listen-address": log.GossipListenAddress,
			"gossip-port": log.GossipPort, "gossip-seed-addresses": log.GossipSeedAddresses,
			"gossip-allow-self-as-seed": log.GossipAllowSelfAsSeed, "BootstrapConfig": log.BootstrapConfig,
			"HAKeeperConfig": log.HAKeeperConfig, "hakeeper-check-interval": log.HAKeeperCheckInterval,
			"hakeeper-bootstrap-retry-interval": log.HAKeeperBootstrapRetryInterval,
		}
	case "TN":
		tn := cfg.getTNServiceConfig()
		result["tn"] = map[string]any{"uuid": tn.UUID, "port-base": tn.PortBase, "service-host": tn.ServiceHost,
			"Txn": map[string]any{"Storage": map[string]any{"backend": "TAE", "log-backend": "logservice"}}}
	case "CN":
		result["cn"] = map[string]any{"uuid": cfg.CN.UUID, "port-base": cfg.CN.PortBase, "service-host": cfg.CN.ServiceHost,
			"auto-upgrade": true, "Engine": map[string]any{"type": "distributed-tae"},
			"txn":      map[string]any{"mode": cfg.CN.Txn.Mode, "isolation": cfg.CN.Txn.Isolation},
			"frontend": map[string]any{"port": cfg.CN.Frontend.Port, "unix-socket": cfg.CN.Frontend.UnixSocketAddress}}
	}
	return result
}

func verifyReleasedInventoryUpgrade(t *testing.T, ctx context.Context, c Cluster, port int64, old releasedInventoryIdentity) uint64 {
	t.Helper()
	require.NoError(t, c.Start())
	sys := releasedSQLClient(t, port, false)
	defer sys.Close()
	tenant := releasedSQLClient(t, port, true)
	defer tenant.Close()
	require.NoError(t, tenant.PingContext(ctx))
	require.Eventually(t, func() bool {
		var count int
		err := sys.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_account where create_version='4.0.7'").Scan(&count)
		return err == nil && count == 2
	}, time.Minute, 100*time.Millisecond)
	final := v4_0_7.Handler.Metadata()
	require.EqualValues(t, versions.StateReady, releasedScalar(t, ctx, sys,
		fmt.Sprintf("select state from mo_catalog.mo_version where version='%s' and version_offset=%d", final.Version, final.VersionOffset)))
	for i, db := range []*sql.DB{sys, tenant} {
		id := old.SourceID
		want := [][2]int{{1, 10}, {2, 20}}
		if i == 1 {
			id, want = old.TenantDatabaseID, [][2]int{{7, 70}}
			require.EqualValues(t, old.TenantID, releasedScalar(t, ctx, db, "select current_account_id()"))
		}
		require.Equal(t, id, releasedScalar(t, ctx, db, "select dat_id from mo_catalog.mo_database where account_id=current_account_id() and datname='nr_release_source'"))
		require.EqualValues(t, 1, releasedScalar(t, ctx, db,
			fmt.Sprintf("select count(*) from mo_catalog.mo_database_inventory where account_id=current_account_id() and database_id=%d and epoch=1", id)))
		func() {
			rows, err := db.QueryContext(ctx, "select id,v from nr_release_source.t order by id")
			require.NoError(t, err)
			defer rows.Close()
			got := [][2]int{}
			for rows.Next() {
				var row [2]int
				require.NoError(t, rows.Scan(&row[0], &row[1]))
				got = append(got, row)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, want, got)
		}()
	}
	_, err := sys.ExecContext(ctx, "create table nr_release_source.added(id int primary key,v int)")
	require.NoError(t, err)
	_, err = sys.ExecContext(ctx, "insert into nr_release_source.added values(9,90)")
	require.NoError(t, err)
	epoch := releasedScalar(t, ctx, sys, fmt.Sprintf("select epoch from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", old.SourceID))
	require.Greater(t, epoch, uint64(1))
	return epoch
}

func copyReleasedVolume(t *testing.T, source, target string) {
	t.Helper()
	_, err := os.Lstat(target)
	require.True(t, os.IsNotExist(err), "copy target must be new")
	command := exec.Command("cp", "-a", source, target)
	require.NoError(t, command.Run())
}

func releasedVolumeHashes(t *testing.T, root string) map[string]string {
	t.Helper()
	result := map[string]string{}
	require.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("non-regular closed backup member %s", path)
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		hash := sha256.New()
		_, err = io.Copy(hash, file)
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		relative, err := filepath.Rel(root, path)
		if err == nil {
			result[relative] = hex.EncodeToString(hash.Sum(nil))
		}
		return err
	}))
	return result
}
