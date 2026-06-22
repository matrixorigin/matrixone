// Copyright 2024 Matrix Origin
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

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

func TestParseLogConfigForWizard(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log1.toml")
	data := `
[logservice]
uuid = "00000000-0000-0000-0000-000000000d01"
deployment-id = 8850055262063090202
data-dir = "/tmp/mo-zombie-restore-repro/data/log1/logservice-data"
raft-address = "127.0.0.1:65200"
gossip-address = "127.0.0.1:65202"
logservice-service-address = "127.0.0.1:65201"

[hakeeper-client]
service-addresses = ["127.0.0.1:65201", "127.0.0.1:65301"]
`
	if err := os.WriteFile(path, []byte(data), 0640); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := parseLogConfig(path)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if cfg.UUID != "00000000-0000-0000-0000-000000000d01" {
		t.Fatalf("unexpected uuid: %s", cfg.UUID)
	}
	if cfg.DeploymentID != 8850055262063090202 {
		t.Fatalf("unexpected deployment id: %d", cfg.DeploymentID)
	}
	if cfg.NodeHostDir != "/tmp/mo-zombie-restore-repro/data/log1/logservice-data/00000000-0000-0000-0000-000000000d01" {
		t.Fatalf("unexpected node host dir: %s", cfg.NodeHostDir)
	}
	if cfg.RaftAddress != "127.0.0.1:65200" || cfg.GossipAddress != "127.0.0.1:65202" {
		t.Fatalf("unexpected raft/gossip address: %s %s", cfg.RaftAddress, cfg.GossipAddress)
	}
	if len(cfg.HAKeeperAddrs) != 2 || cfg.HAKeeperAddrs[0] != "127.0.0.1:65201" {
		t.Fatalf("unexpected hakeeper addresses: %v", cfg.HAKeeperAddrs)
	}
}

func TestLocalShardReplicasScansTanNodes(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "host-10-222-1-50", "08850055262063090202", "tandb")
	if err := os.MkdirAll(filepath.Join(root, "node-1-262147"), 0750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "node-1-282826"), 0750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "node-2-123"), 0750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	replicas := localShardReplicas(dir, 8850055262063090202, 1)
	if len(replicas) != 2 || replicas[0] != 262147 || replicas[1] != 282826 {
		t.Fatalf("unexpected replicas: %v", replicas)
	}
}

func TestFilterRunningLocalAddressesDropsStoppedLocalStores(t *testing.T) {
	addresses := []string{"127.0.0.1:65101", "127.0.0.1:65201", "logservice.example:32001"}
	configs := map[string]localLogConfig{
		"127.0.0.1:65101": {
			ConfigPath: "/tmp/not-running-log0.toml",
		},
		"127.0.0.1:65201": {
			ConfigPath: "/tmp/not-running-log1.toml",
		},
	}
	filtered := filterRunningLocalAddresses(addresses, configs)
	if len(filtered) != 1 || filtered[0] != "logservice.example:32001" {
		t.Fatalf("unexpected filtered addresses: %v", filtered)
	}
}

func TestBuildPlanStoreKeepsReportedTargetReplica(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "host-10-222-1-50", "08850055262063090202", "tandb")
	if err := os.MkdirAll(filepath.Join(root, "node-1-262147"), 0750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "node-1-282826"), 0750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	shard := logpb.LogShardInfo{
		ShardID:  1,
		Replicas: map[uint64]string{282826: "store-d01", 262145: "store-d02"},
		LeaderID: 262145,
	}
	cfg := localLogConfig{
		UUID:         "store-d01",
		NodeHostDir:  dir,
		DeploymentID: 8850055262063090202,
	}
	store := buildPlanStore(1, shard, cfg, "store-d02", logpb.LogStoreInfo{
		Replicas: []logpb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 282826}},
	}, true)
	if store.Role != "cleanup" {
		t.Fatalf("unexpected role: %s", store.Role)
	}
	if len(store.CleanupReplicas) != 1 || store.CleanupReplicas[0] != 262147 {
		t.Fatalf("unexpected cleanup replicas: %v", store.CleanupReplicas)
	}
}

func TestBuildPlanStoreRebuildsReportedTargetWithStaleMembership(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "host-10-222-1-50", "08850055262063090202", "tandb")
	if err := os.MkdirAll(filepath.Join(root, "node-1-262149"), 0750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	shard := logpb.LogShardInfo{
		ShardID:  1,
		Replicas: map[uint64]string{262147: "store-d02", 262149: "store-d03", 272586: "store-d01"},
		Epoch:    1799,
		LeaderID: 262147,
	}
	cfg := localLogConfig{
		UUID:         "store-d03",
		NodeHostDir:  dir,
		DeploymentID: 8850055262063090202,
	}
	staleShard := logpb.LogShardInfo{
		ShardID:  1,
		Replicas: map[uint64]string{262145: "store-d00", 262146: "store-d01", 262147: "store-d02"},
		Epoch:    424,
		LeaderID: 262147,
	}
	store := buildPlanStore(1, shard, cfg, "store-d02", logpb.LogStoreInfo{
		Replicas: []logpb.LogReplicaInfo{{LogShardInfo: staleShard, ReplicaID: 262149}},
	}, true)
	if store.Role != "rebuild" {
		t.Fatalf("unexpected role: %s", store.Role)
	}
	if len(store.CleanupReplicas) != 1 || store.CleanupReplicas[0] != 262149 {
		t.Fatalf("unexpected cleanup replicas: %v", store.CleanupReplicas)
	}
	if len(store.Warnings) == 0 {
		t.Fatalf("expected stale membership warning")
	}
}

func TestBuildPlanStoreKeepsReportedTargetWithOnlyEpochLag(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "host-10-222-1-50", "08850055262063090202", "tandb")
	if err := os.MkdirAll(filepath.Join(root, "node-1-272586"), 0750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	shard := logpb.LogShardInfo{
		ShardID:  1,
		Replicas: map[uint64]string{262146: "store-d02", 272586: "store-d03", 282826: "store-d01"},
		Epoch:    602,
		LeaderID: 262146,
	}
	reported := shard
	reported.Epoch = 599
	cfg := localLogConfig{
		UUID:         "store-d03",
		NodeHostDir:  dir,
		DeploymentID: 8850055262063090202,
	}
	store := buildPlanStore(1, shard, cfg, "store-d02", logpb.LogStoreInfo{
		Replicas: []logpb.LogReplicaInfo{{LogShardInfo: reported, ReplicaID: 272586}},
	}, true)
	if store.Role != "target" {
		t.Fatalf("unexpected role: %s", store.Role)
	}
	if len(store.CleanupReplicas) != 0 {
		t.Fatalf("unexpected cleanup replicas: %v", store.CleanupReplicas)
	}
	if len(store.Warnings) == 0 {
		t.Fatalf("expected epoch warning")
	}
}

func TestStableHAKeeperAddressesForApplyRanksCleanupStoresLast(t *testing.T) {
	plan := &repairPlan{
		HAKeeperAddresses: []string{"127.0.0.1:65201", "127.0.0.1:65301", "127.0.0.1:65401"},
		SourceStore:       "d02",
		Stores: []planStore{
			{UUID: "d01", ServiceAddress: "127.0.0.1:65201", CleanupReplicas: []uint64{262146}},
			{UUID: "d02", ServiceAddress: "127.0.0.1:65301", Role: "source"},
			{UUID: "d03", ServiceAddress: "127.0.0.1:65401", Role: "target"},
		},
	}
	got := stableHAKeeperAddressesForApply(plan)
	want := []string{"127.0.0.1:65301", "127.0.0.1:65401", "127.0.0.1:65201"}
	if len(got) != len(want) {
		t.Fatalf("unexpected addresses: %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected addresses: got %v want %v", got, want)
		}
	}
}

func TestValidateNoDuplicateLocalShardsCatchesOtherShardResidue(t *testing.T) {
	dir := t.TempDir()
	if err := writeLogMetadata(filepath.Join(dir, logMetadataFilename), metadata.LogStore{
		Shards: []metadata.LogShard{
			{LogShardRecord: metadata.LogShardRecord{ShardID: 0}, ReplicaID: 131074},
			{LogShardRecord: metadata.LogShardRecord{ShardID: 0}, ReplicaID: 272587},
			{LogShardRecord: metadata.LogShardRecord{ShardID: 1}, ReplicaID: 272588},
		},
	}); err != nil {
		t.Fatalf("write metadata: %v", err)
	}
	store := planStore{
		UUID:            "00000000-0000-0000-0000-000000000d01",
		NodeHostDir:     dir,
		DeploymentID:    8850055262063090202,
		RaftAddress:     "127.0.0.1:65200",
		ListenAddress:   "127.0.0.1:65200",
		GossipAddress:   "127.0.0.1:65202",
		CleanupReplicas: []uint64{272588},
	}
	err := validateNoDuplicateLocalShards([]planStore{store})
	if err == nil {
		t.Fatalf("expected duplicate local shard error")
	}
	msg := err.Error()
	for _, want := range []string{"shard 0", "131074", "272587", "--shard-id 0"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("expected %q in error: %s", want, msg)
		}
	}
}

func TestValidatePlannedLocalCleanupCompletenessCatchesBeforeApply(t *testing.T) {
	dir := t.TempDir()
	if err := writeLogMetadata(filepath.Join(dir, logMetadataFilename), metadata.LogStore{
		Shards: []metadata.LogShard{
			{LogShardRecord: metadata.LogShardRecord{ShardID: 0}, ReplicaID: 131074},
			{LogShardRecord: metadata.LogShardRecord{ShardID: 0}, ReplicaID: 272588},
			{LogShardRecord: metadata.LogShardRecord{ShardID: 1}, ReplicaID: 262146},
			{LogShardRecord: metadata.LogShardRecord{ShardID: 1}, ReplicaID: 272587},
		},
	}); err != nil {
		t.Fatalf("write metadata: %v", err)
	}
	plan := &repairPlan{ShardID: 1}
	store := planStore{
		UUID:            "00000000-0000-0000-0000-000000000d01",
		NodeHostDir:     dir,
		DeploymentID:    8850055262063090202,
		CleanupReplicas: []uint64{272587},
	}
	err := validatePlannedLocalCleanupCompleteness(plan, []planStore{store})
	if err == nil {
		t.Fatalf("expected incomplete cleanup error")
	}
	msg := err.Error()
	for _, want := range []string{"before mutating HAKeeper", "shard 0", "131074", "272588"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("expected %q in error: %s", want, msg)
		}
	}
}

func TestValidateCombinedPlannedLocalCleanupCompletenessAllowsCrossShardCleanup(t *testing.T) {
	dir := t.TempDir()
	if err := writeLogMetadata(filepath.Join(dir, logMetadataFilename), metadata.LogStore{
		Shards: []metadata.LogShard{
			{LogShardRecord: metadata.LogShardRecord{ShardID: 0}, ReplicaID: 131074},
			{LogShardRecord: metadata.LogShardRecord{ShardID: 0}, ReplicaID: 272588},
			{LogShardRecord: metadata.LogShardRecord{ShardID: 1}, ReplicaID: 262146},
			{LogShardRecord: metadata.LogShardRecord{ShardID: 1}, ReplicaID: 272587},
		},
	}); err != nil {
		t.Fatalf("write metadata: %v", err)
	}
	store := planStore{
		UUID:            "00000000-0000-0000-0000-000000000d01",
		NodeHostDir:     dir,
		DeploymentID:    8850055262063090202,
		RaftAddress:     "127.0.0.1:65200",
		ListenAddress:   "127.0.0.1:65200",
		GossipAddress:   "127.0.0.1:65202",
		CleanupReplicas: []uint64{131074},
	}
	shard0 := &repairPlan{
		Version:        repairPlanVersion,
		Mode:           modeLocal,
		ShardID:        0,
		ApplySupported: true,
		RebuildStores:  []string{store.UUID},
		Local:          &localPlanSettings{BackupDir: filepath.Join(dir, "backup")},
		Stores:         []planStore{store},
	}
	store.CleanupReplicas = []uint64{272587}
	shard1 := &repairPlan{
		Version:        repairPlanVersion,
		Mode:           modeLocal,
		ShardID:        1,
		ApplySupported: true,
		RebuildStores:  []string{store.UUID},
		Local:          &localPlanSettings{BackupDir: filepath.Join(dir, "backup")},
		Stores:         []planStore{store},
	}
	if err := validateCombinedPlannedLocalCleanupCompleteness([]*repairPlan{shard0, shard1}); err != nil {
		t.Fatalf("combined cleanup should be complete: %v", err)
	}
	tasks := cleanupTasksForPlans([]*repairPlan{shard0, shard1})
	if len(tasks) != 2 || tasks[0].Plan.ShardID != 0 || tasks[1].Plan.ShardID != 1 {
		t.Fatalf("unexpected cleanup tasks: %+v", tasks)
	}
}

func TestValidateLocalPlanFreshnessCatchesStaleLocalReplicas(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "host-10-222-1-50", "08850055262063090202", "tandb")
	if err := os.MkdirAll(filepath.Join(root, "node-1-262147"), 0750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "node-1-282826"), 0750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	plan := &repairPlan{
		Mode:    modeLocal,
		ShardID: 1,
		Stores: []planStore{{
			UUID:          "00000000-0000-0000-0000-000000000d01",
			NodeHostDir:   dir,
			DeploymentID:  8850055262063090202,
			LocalReplicas: []uint64{262146, 272587},
		}},
	}
	err := validateLocalPlanFreshness([]*repairPlan{plan})
	if err == nil {
		t.Fatalf("expected stale plan error")
	}
	msg := err.Error()
	for _, want := range []string{"repair plan is stale", "262146", "272587", "262147", "282826"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("expected %q in error: %s", want, msg)
		}
	}
}

func TestBuildK8sPlanStoreRebuildsDirtyTarget(t *testing.T) {
	shard := logpb.LogShardInfo{
		ShardID:  1,
		Replicas: map[uint64]string{262146: "store-d02", 282826: "store-d01"},
		Epoch:    602,
		LeaderID: 262146,
	}
	stale := logpb.LogShardInfo{
		ShardID:  1,
		Replicas: map[uint64]string{262146: "store-d02", 262147: "store-d01"},
		Epoch:    308,
		LeaderID: 262146,
	}
	store := buildK8sPlanStore(1, shard, "store-d01", "store-d02", logpb.LogStoreInfo{
		RaftAddress:   "store-d01-raft:32000",
		GossipAddress: "store-d01-gossip:32002",
		Replicas:      []logpb.LogReplicaInfo{{LogShardInfo: stale, ReplicaID: 282826}},
	}, true, wizardOptions{
		pvcDataDir:   "/repair-pvc/logservice-data",
		deploymentID: 8850055262063090202,
	})
	if store.Role != "rebuild" {
		t.Fatalf("unexpected role: %s", store.Role)
	}
	if store.NodeHostDir != "/repair-pvc/logservice-data/store-d01" {
		t.Fatalf("unexpected nodehost dir: %s", store.NodeHostDir)
	}
	if len(store.CleanupReplicas) != 1 || store.CleanupReplicas[0] != 282826 {
		t.Fatalf("unexpected cleanup replicas: %v", store.CleanupReplicas)
	}
}

func TestChooseK8sRepairShardUsesSourceHeartbeatMembership(t *testing.T) {
	hakeeperShard := logpb.LogShardInfo{
		ShardID:  1,
		Replicas: map[uint64]string{262146: "store-d01", 262147: "store-d00", 282825: "store-d03"},
		Epoch:    939,
		LeaderID: 262147,
		Term:     3,
	}
	sourceShard := logpb.LogShardInfo{
		ShardID:  1,
		Replicas: map[uint64]string{262147: "store-d00", 262150: "store-d03", 272586: "store-d01"},
		Epoch:    834,
		Term:     3,
	}
	state := logpb.CheckerState{
		LogState: logpb.LogState{
			Stores: map[string]logpb.LogStoreInfo{
				"store-d00": {
					Replicas: []logpb.LogReplicaInfo{{
						ReplicaID:    262147,
						LogShardInfo: sourceShard,
					}},
				},
			},
		},
	}
	got, sourceStore, warnings := chooseK8sRepairShard(state, 1, hakeeperShard)
	if sourceStore != "store-d00" {
		t.Fatalf("unexpected source store: %s", sourceStore)
	}
	if !sameReplicaMap(got.Replicas, sourceShard.Replicas) {
		t.Fatalf("expected source replicas %s, got %s", formatReplicaMap(sourceShard.Replicas), formatReplicaMap(got.Replicas))
	}
	if got.LeaderID != 262147 {
		t.Fatalf("expected leader 262147, got %d", got.LeaderID)
	}
	if got.Epoch != 940 {
		t.Fatalf("expected bumped epoch 940, got %d", got.Epoch)
	}
	if len(warnings) == 0 || !strings.Contains(warnings[0], "using source heartbeat membership") {
		t.Fatalf("expected source membership warning, got %v", warnings)
	}
}

func TestTargetReplicaHeartbeatProblemsDetectsPendingTarget(t *testing.T) {
	target := planShard{
		ShardID:  1,
		Replicas: map[uint64]string{262146: "store-d01", 262147: "store-d00", 282825: "store-d03"},
		Epoch:    939,
		LeaderID: 262147,
		Term:     3,
	}
	plan := &repairPlan{
		ShardID:     1,
		TargetShard: target,
		Stores: []planStore{
			{UUID: "store-d00", TargetReplicaID: 262147},
			{UUID: "store-d01", TargetReplicaID: 262146},
			{UUID: "store-d03", TargetReplicaID: 282825},
		},
	}
	state := logpb.CheckerState{
		LogState: logpb.LogState{
			Stores: map[string]logpb.LogStoreInfo{
				"store-d00": {
					Replicas: []logpb.LogReplicaInfo{{
						ReplicaID:    262147,
						LogShardInfo: logShardInfoFromPlan(target),
					}},
				},
				"store-d01": {
					Replicas: []logpb.LogReplicaInfo{{
						ReplicaID: 272586,
						LogShardInfo: logpb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{262147: "store-d00", 262150: "store-d03", 272586: "store-d01"},
						},
					}},
				},
				"store-d03": {},
			},
		},
	}
	problems := targetReplicaHeartbeatProblems(state, []*repairPlan{plan})
	joined := strings.Join(problems, "\n")
	for _, want := range []string{"store-d01 target replica 262146 is not reported", "store-d03 target replica 282825 is not reported"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected %q in problems: %s", want, joined)
		}
	}
}

func TestBuildK8sActionsIncludesRepairPodAndCleanCommand(t *testing.T) {
	plan := &repairPlan{
		Mode:              modeK8s,
		Namespace:         "mo-prod",
		ShardID:           1,
		HAKeeperAddresses: []string{"127.0.0.1:32001"},
		TargetShard: planShard{
			ShardID:  1,
			Replicas: map[uint64]string{282826: "store-d01"},
			Epoch:    602,
		},
		InitialBlockedStores:    []string{"store-d01"},
		PersistentBlockedStores: []string{"store-d00"},
		RebuildStores:           []string{"store-d01"},
		Stores: []planStore{{
			UUID:            "store-d01",
			Role:            "rebuild",
			NodeHostDir:     "/repair-pvc/logservice-data/store-d01",
			DeploymentID:    8850055262063090202,
			RaftAddress:     "store-d01-raft:32000",
			ListenAddress:   "store-d01-raft:32000",
			GossipAddress:   "store-d01-gossip:32002",
			CleanupReplicas: []uint64{282826},
		}},
		K8s: &k8sPlanSettings{
			Kubectl:              "kubectl",
			LogSelector:          "component=logservice",
			PVCMountPath:         "/repair-pvc",
			PVCLogServiceDataDir: "/repair-pvc/logservice-data",
			RepairImage:          "matrixorigin/matrixone:test",
			RepairBinary:         "/tmp/mo-logservice-repair",
			DeploymentID:         8850055262063090202,
		},
	}
	actions := buildK8sActions(plan)
	joined := ""
	for _, action := range actions {
		joined += action.Command + "\n"
	}
	if !strings.Contains(joined, "cleanupReplicasByStore") || !strings.Contains(joined, "282826") {
		t.Fatalf("repair payload cleanup missing: %s", joined)
	}
	if !strings.Contains(joined, "delete pod <logservice-pod-for-store-d01>") {
		t.Fatalf("restart pod command missing: %s", joined)
	}
}

func TestSelectedRepairShardIDsKeepsHAKeeperShardZero(t *testing.T) {
	ids, err := selectedRepairShardIDs(wizardOptions{
		shardID:    1,
		shardIDSet: true,
		shards:     "0,1",
	})
	if err != nil {
		t.Fatalf("selected shard ids: %v", err)
	}
	if len(ids) != 2 || ids[0] != 0 || ids[1] != 1 {
		t.Fatalf("unexpected shard ids: %v", ids)
	}
}

func TestRepairPayloadForPlanWithCleanup(t *testing.T) {
	plan := &repairPlan{
		TargetShard: planShard{
			ShardID:  1,
			Replicas: map[uint64]string{10: "store-d01"},
			Epoch:    7,
		},
		Stores: []planStore{{
			UUID:            "store-d01",
			CleanupReplicas: []uint64{11, 12},
		}},
	}
	payload := repairPayloadForPlanWithCleanup(plan, []string{"store-d01"}, "repair", true)
	if got := payload.CleanupReplicasByStore["store-d01"]; len(got) != 2 || got[0] != 11 || got[1] != 12 {
		t.Fatalf("unexpected cleanup payload: %+v", payload.CleanupReplicasByStore)
	}
	if encoded := encodeReason(payload.Reason, payload.CleanupReplicasByStore); !strings.Contains(encoded, repairReasonPrefix) {
		t.Fatalf("cleanup payload was not encoded: %s", encoded)
	}
}
