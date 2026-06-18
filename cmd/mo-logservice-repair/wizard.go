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
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"

	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

const (
	repairPlanVersion = "v1"
	modeLocal         = "local"
	modeK8s           = "k8s"
)

type wizardOptions struct {
	mode           string
	baseDir        string
	confDir        string
	shardID        uint64
	shardIDSet     bool
	addresses      string
	output         string
	planPath       string
	apply          bool
	yes            bool
	timeout        time.Duration
	moServicePath  string
	namespace      string
	kubeContext    string
	k8sLogSelector string
	pvcMountPath   string
	pvcDataDir     string
	repairImage    string
	repairBinary   string
	deploymentID   uint64
}

type repairPlan struct {
	Version                 string             `json:"version"`
	Mode                    string             `json:"mode"`
	CreatedAt               string             `json:"createdAt"`
	BaseDir                 string             `json:"baseDir,omitempty"`
	ConfDir                 string             `json:"confDir,omitempty"`
	Namespace               string             `json:"namespace,omitempty"`
	KubeContext             string             `json:"kubeContext,omitempty"`
	ShardID                 uint64             `json:"shardID"`
	HAKeeperAddresses       []string           `json:"hakeeperAddresses,omitempty"`
	TargetShard             planShard          `json:"targetShard,omitempty"`
	Stores                  []planStore        `json:"stores,omitempty"`
	InitialBlockedStores    []string           `json:"initialBlockedStores,omitempty"`
	PersistentBlockedStores []string           `json:"persistentBlockedStores,omitempty"`
	RebuildStores           []string           `json:"rebuildStores,omitempty"`
	SourceStore             string             `json:"sourceStore,omitempty"`
	Actions                 []planAction       `json:"actions"`
	Warnings                []string           `json:"warnings,omitempty"`
	ApplySupported          bool               `json:"applySupported"`
	Local                   *localPlanSettings `json:"local,omitempty"`
	K8s                     *k8sPlanSettings   `json:"k8s,omitempty"`
}

type localPlanSettings struct {
	MOServicePath string `json:"moServicePath,omitempty"`
	BackupDir     string `json:"backupDir,omitempty"`
	LogDir        string `json:"logDir,omitempty"`
}

type k8sPlanSettings struct {
	Kubectl               string `json:"kubectl"`
	LogSelector           string `json:"logSelector,omitempty"`
	PVCMountPath          string `json:"pvcMountPath"`
	PVCLogServiceDataDir  string `json:"pvcLogServiceDataDir"`
	RepairImage           string `json:"repairImage"`
	RepairBinary          string `json:"repairBinary"`
	DeploymentID          uint64 `json:"deploymentID,omitempty"`
	DeploymentIDRequired  bool   `json:"deploymentIDRequired,omitempty"`
	HAKeeperPortForwarded bool   `json:"hakeeperPortForwarded,omitempty"`
}

type planShard struct {
	ShardID           uint64            `json:"shardID"`
	Replicas          map[uint64]string `json:"replicas,omitempty"`
	NonVotingReplicas map[uint64]string `json:"nonVotingReplicas,omitempty"`
	Epoch             uint64            `json:"epoch"`
	LeaderID          uint64            `json:"leaderID"`
	Term              uint64            `json:"term"`
}

type planStore struct {
	UUID               string   `json:"uuid"`
	Role               string   `json:"role"`
	ConfigPath         string   `json:"configPath,omitempty"`
	DataDir            string   `json:"dataDir,omitempty"`
	NodeHostDir        string   `json:"nodeHostDir,omitempty"`
	DeploymentID       uint64   `json:"deploymentID,omitempty"`
	ServiceAddress     string   `json:"serviceAddress,omitempty"`
	RaftAddress        string   `json:"raftAddress,omitempty"`
	ListenAddress      string   `json:"listenAddress,omitempty"`
	GossipAddress      string   `json:"gossipAddress,omitempty"`
	TargetReplicaID    uint64   `json:"targetReplicaID,omitempty"`
	LocalReplicas      []uint64 `json:"localReplicas,omitempty"`
	CleanupReplicas    []uint64 `json:"cleanupReplicas,omitempty"`
	ProcessIDs         []int    `json:"processIDs,omitempty"`
	Warnings           []string `json:"warnings,omitempty"`
	MOServicePath      string   `json:"moServicePath,omitempty"`
	NeedsStopAndStart  bool     `json:"needsStopAndStart,omitempty"`
	PresentInHAKeeper  bool     `json:"presentInHAKeeper"`
	PresentInLocalData bool     `json:"presentInLocalData"`
}

type planAction struct {
	Type        string            `json:"type"`
	Description string            `json:"description"`
	Command     string            `json:"command,omitempty"`
	Store       string            `json:"store,omitempty"`
	ShardID     uint64            `json:"shardID,omitempty"`
	ReplicaID   uint64            `json:"replicaID,omitempty"`
	Parameters  map[string]string `json:"parameters,omitempty"`
}

type logConfigFile struct {
	LogService struct {
		UUID                     string `toml:"uuid"`
		DeploymentID             uint64 `toml:"deployment-id"`
		DataDir                  string `toml:"data-dir"`
		ServiceHost              string `toml:"service-host"`
		LogServiceAddress        string `toml:"logservice-address"`
		LogServiceServiceAddress string `toml:"logservice-service-address"`
		LogServiceListenAddress  string `toml:"logservice-listen-address"`
		LogServicePort           int    `toml:"logservice-port"`
		RaftAddress              string `toml:"raft-address"`
		RaftListenAddress        string `toml:"raft-listen-address"`
		RaftPort                 int    `toml:"raft-port"`
		GossipAddress            string `toml:"gossip-address"`
		GossipListenAddress      string `toml:"gossip-listen-address"`
		GossipPort               int    `toml:"gossip-port"`
	} `toml:"logservice"`
	HAKeeperClient struct {
		ServiceAddresses []string `toml:"service-addresses"`
		DiscoveryAddress string   `toml:"discovery-address"`
	} `toml:"hakeeper-client"`
}

func runOps(args []string) error {
	if len(args) == 0 {
		return usage()
	}
	switch args[0] {
	case "recover-dirty-log-shard":
		return runWizard(args[1:])
	default:
		return usage()
	}
}

func runWizard(args []string) error {
	opts, err := parseWizardFlags("wizard", args)
	if err != nil {
		return err
	}
	opts, err = promptWizardOptions(opts)
	if err != nil {
		return err
	}
	plan, err := buildRepairPlan(opts)
	if err != nil {
		return err
	}
	printPlanSummary(plan)
	if !opts.yes {
		if err := confirmPlanDetails(plan); err != nil {
			return err
		}
	}
	if opts.output != "" {
		if err := writePlanFile(opts.output, plan); err != nil {
			return err
		}
		fmt.Printf("plan written to %s\n", opts.output)
	}
	if !opts.apply {
		if !plan.ApplySupported {
			return nil
		}
		if opts.yes {
			return nil
		}
		ok, err := askYesNo("Apply this plan now? Type y to continue", false)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
	if !opts.yes {
		confirm, err := askLine("This may stop/restart LogService and edit local replica data. Type APPLY to continue")
		if err != nil {
			return err
		}
		if confirm != "APPLY" {
			return fmt.Errorf("apply cancelled")
		}
	}
	return applyRepairPlan(context.Background(), plan, opts)
}

func runPlan(args []string) error {
	opts, err := parseWizardFlags("plan", args)
	if err != nil {
		return err
	}
	if opts.mode == "" {
		opts.mode = modeLocal
	}
	if opts.mode == modeLocal && opts.baseDir == "" && opts.confDir == "" {
		return fmt.Errorf("--base or --conf-dir is required for local mode")
	}
	if !opts.shardIDSet {
		return fmt.Errorf("--shard is required")
	}
	plan, err := buildRepairPlan(opts)
	if err != nil {
		return err
	}
	if opts.output != "" {
		return writePlanFile(opts.output, plan)
	}
	return printJSON(plan)
}

func runApply(args []string) error {
	opts, err := parseWizardFlags("apply", args)
	if err != nil {
		return err
	}
	if opts.planPath == "" {
		return fmt.Errorf("--plan is required")
	}
	plan, err := readPlanFile(opts.planPath)
	if err != nil {
		return err
	}
	printPlanSummary(plan)
	if !opts.yes {
		if err := confirmPlanDetails(plan); err != nil {
			return err
		}
		confirm, err := askLine("This may stop/restart LogService and edit local replica data. Type APPLY to continue")
		if err != nil {
			return err
		}
		if confirm != "APPLY" {
			return fmt.Errorf("apply cancelled")
		}
	}
	return applyRepairPlan(context.Background(), plan, opts)
}

func parseWizardFlags(name string, args []string) (wizardOptions, error) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	var opts wizardOptions
	fs.StringVar(&opts.mode, "mode", "", "repair mode: local or k8s")
	fs.StringVar(&opts.baseDir, "base", "", "local MatrixOne repair base directory")
	fs.StringVar(&opts.confDir, "conf-dir", "", "local MatrixOne config directory")
	fs.Uint64Var(&opts.shardID, "shard", 0, "log shard id to repair")
	fs.StringVar(&opts.addresses, "addresses", "", "comma-separated HAKeeper service addresses")
	fs.StringVar(&opts.output, "output", "", "write generated plan to this file")
	fs.StringVar(&opts.planPath, "plan", "", "repair plan JSON file")
	fs.BoolVar(&opts.apply, "apply", false, "apply after planning")
	fs.BoolVar(&opts.yes, "yes", false, "skip interactive confirmations")
	fs.DurationVar(&opts.timeout, "timeout", 10*time.Second, "HAKeeper request timeout")
	fs.StringVar(&opts.moServicePath, "mo-service", "", "mo-service binary path used when restarting local stores")
	fs.StringVar(&opts.namespace, "namespace", "", "kubernetes namespace")
	fs.StringVar(&opts.kubeContext, "context", "", "kubernetes context")
	fs.StringVar(&opts.k8sLogSelector, "k8s-log-selector", "app.kubernetes.io/component=logservice", "kubernetes selector used to list LogService pods")
	fs.StringVar(&opts.pvcMountPath, "pvc-mount-path", "/repair-pvc", "mount path used by the temporary k8s repair pod")
	fs.StringVar(&opts.pvcDataDir, "pvc-logservice-data-dir", "", "logservice-data directory inside the temporary k8s repair pod")
	fs.StringVar(&opts.repairImage, "repair-image", "matrixorigin/matrixone:latest", "image used by the temporary k8s repair pod")
	fs.StringVar(&opts.repairBinary, "repair-binary", "/tmp/mo-logservice-repair", "mo-logservice-repair path inside the temporary repair pod")
	fs.Uint64Var(&opts.deploymentID, "deployment-id", 0, "dragonboat deployment id; required for exact k8s PVC clean-replica commands")
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "shard" {
			opts.shardIDSet = true
		}
	})
	return opts, nil
}

func promptWizardOptions(opts wizardOptions) (wizardOptions, error) {
	if opts.yes {
		if opts.mode == "" {
			opts.mode = modeLocal
		}
		if opts.mode == modeLocal && opts.baseDir == "" && opts.confDir == "" {
			return opts, fmt.Errorf("--base or --conf-dir is required")
		}
		if !opts.shardIDSet {
			return opts, fmt.Errorf("--shard is required")
		}
		return opts, nil
	}
	if opts.mode == "" {
		mode, err := askChoice("Select mode", []string{modeLocal, modeK8s}, modeLocal)
		if err != nil {
			return opts, err
		}
		opts.mode = mode
	}
	if opts.mode == modeLocal && opts.baseDir == "" && opts.confDir == "" {
		base, err := askLineDefault("Local repair base directory", ".")
		if err != nil {
			return opts, err
		}
		opts.baseDir = base
	}
	if opts.mode == modeK8s && opts.namespace == "" {
		ns, err := askLine("Kubernetes namespace")
		if err != nil {
			return opts, err
		}
		opts.namespace = ns
	}
	if !opts.shardIDSet {
		shard, err := askLineDefault("Log shard ID to repair", "1")
		if err != nil {
			return opts, err
		}
		id, err := strconv.ParseUint(strings.TrimSpace(shard), 10, 64)
		if err != nil {
			return opts, fmt.Errorf("invalid shard id %q: %w", shard, err)
		}
		opts.shardID = id
		opts.shardIDSet = true
	}
	return opts, nil
}

func buildRepairPlan(opts wizardOptions) (*repairPlan, error) {
	if opts.mode == "" {
		opts.mode = modeLocal
	}
	switch opts.mode {
	case modeLocal:
		return buildLocalRepairPlan(opts)
	case modeK8s:
		return buildK8sRepairPlan(opts)
	default:
		return nil, fmt.Errorf("unsupported mode %q", opts.mode)
	}
}

func buildK8sRepairPlan(opts wizardOptions) (*repairPlan, error) {
	if opts.namespace == "" {
		return nil, fmt.Errorf("--namespace is required for k8s mode")
	}
	kubectl := kubectlCommand(opts.kubeContext)
	if opts.pvcMountPath == "" {
		opts.pvcMountPath = "/repair-pvc"
	}
	if opts.pvcDataDir == "" {
		opts.pvcDataDir = filepath.Join(opts.pvcMountPath, "logservice-data")
	}
	if opts.repairImage == "" {
		opts.repairImage = "matrixorigin/matrixone:latest"
	}
	if opts.repairBinary == "" {
		opts.repairBinary = "/tmp/mo-logservice-repair"
	}
	plan := &repairPlan{
		Version:           repairPlanVersion,
		Mode:              modeK8s,
		CreatedAt:         time.Now().Format(time.RFC3339),
		Namespace:         opts.namespace,
		KubeContext:       opts.kubeContext,
		ShardID:           opts.shardID,
		HAKeeperAddresses: splitAddresses(opts.addresses),
		ApplySupported:    false,
		K8s: &k8sPlanSettings{
			Kubectl:              kubectl,
			LogSelector:          opts.k8sLogSelector,
			PVCMountPath:         opts.pvcMountPath,
			PVCLogServiceDataDir: opts.pvcDataDir,
			RepairImage:          opts.repairImage,
			RepairBinary:         opts.repairBinary,
			DeploymentID:         opts.deploymentID,
			DeploymentIDRequired: opts.deploymentID == 0,
		},
		Warnings: []string{
			"k8s mode is plan-only in this version; it does not edit PVC data directly",
			"cleaning replicas in k8s must be done from a maintenance pod after the target LogService pod is stopped",
		},
	}
	if len(plan.HAKeeperAddresses) == 0 {
		plan.Warnings = append(plan.Warnings, "pass --addresses after port-forwarding HAKeeper to generate membership-aware store cleanup actions")
		plan.Actions = buildK8sActions(plan)
		return plan, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()
	client, addr, err := connect(ctx, plan.HAKeeperAddresses)
	if err != nil {
		return nil, fmt.Errorf("connect hakeeper: %w", err)
	}
	defer client.Close()
	state, err := getClusterState(ctx, client, addr)
	if err != nil {
		return nil, fmt.Errorf("get cluster state: %w", err)
	}
	shard, ok := state.LogState.Shards[opts.shardID]
	if !ok {
		return nil, fmt.Errorf("shard %d not found in HAKeeper state", opts.shardID)
	}
	plan.TargetShard = toPlanShard(shard)
	targetByStore := replicaByStore(shard)
	sourceStore := targetByStore[shard.LeaderID]
	plan.SourceStore = sourceStore
	if sourceStore == "" {
		plan.Warnings = append(plan.Warnings, "HAKeeper has no leader for the target shard; source replica must be reviewed manually")
	}

	storeSet := targetStores(shard)
	for uuid, info := range state.LogState.Stores {
		if storeSet[uuid] {
			continue
		}
		for _, replica := range info.Replicas {
			if replica.ShardID == opts.shardID {
				storeSet[uuid] = true
				break
			}
		}
	}
	storeIDs := make([]string, 0, len(storeSet))
	for uuid := range storeSet {
		storeIDs = append(storeIDs, uuid)
	}
	sort.Strings(storeIDs)
	for _, uuid := range storeIDs {
		storeInfo, hasStoreInfo := state.LogState.Stores[uuid]
		store := buildK8sPlanStore(opts.shardID, shard, uuid, sourceStore, storeInfo, hasStoreInfo, opts)
		if store.NeedsStopAndStart {
			plan.RebuildStores = append(plan.RebuildStores, store.UUID)
			plan.InitialBlockedStores = append(plan.InitialBlockedStores, store.UUID)
		}
		for _, warning := range store.Warnings {
			plan.Warnings = append(plan.Warnings, fmt.Sprintf("%s: %s", store.UUID, warning))
		}
		if store.Role == "stale" {
			plan.PersistentBlockedStores = append(plan.PersistentBlockedStores, store.UUID)
			plan.InitialBlockedStores = append(plan.InitialBlockedStores, store.UUID)
		}
		plan.Stores = append(plan.Stores, store)
	}
	plan.InitialBlockedStores = uniqueStrings(plan.InitialBlockedStores)
	plan.PersistentBlockedStores = uniqueStrings(plan.PersistentBlockedStores)
	plan.RebuildStores = uniqueStrings(plan.RebuildStores)
	plan.Actions = buildK8sActions(plan)
	return plan, nil
}

func buildK8sPlanStore(
	shardID uint64,
	shard logpb.LogShardInfo,
	uuid string,
	sourceStore string,
	storeInfo logpb.LogStoreInfo,
	hasStoreInfo bool,
	opts wizardOptions,
) planStore {
	targetReplicaID := uint64(0)
	for replicaID, store := range shard.Replicas {
		if store == uuid {
			targetReplicaID = replicaID
			break
		}
	}
	if targetReplicaID == 0 {
		for replicaID, store := range shard.NonVotingReplicas {
			if store == uuid {
				targetReplicaID = replicaID
				break
			}
		}
	}
	reportedReplicas := reportedShardReplicas(storeInfo, shardID)
	replicaReported := false
	replicaHealthy := false
	reportWarnings := []string(nil)
	if targetReplicaID != 0 && hasStoreInfo {
		replicaReported, replicaHealthy, reportWarnings = storeReportsHealthyReplica(storeInfo, shard, targetReplicaID)
	}
	role := "unused"
	switch {
	case uuid == sourceStore:
		role = "source"
	case targetReplicaID != 0:
		if replicaHealthy {
			role = "target"
		} else {
			role = "rebuild"
			if replicaReported && len(reportWarnings) > 0 {
				reportWarnings = append(reportWarnings, "target replica is reported by HAKeeper heartbeat, but its local shard membership differs from the target shard; PVC data must be rebuilt")
			}
		}
	case len(reportedReplicas) > 0:
		role = "stale"
	}
	cleanupReplicas := []uint64(nil)
	if role == "rebuild" {
		cleanupReplicas = append(cleanupReplicas, reportedReplicas...)
		if targetReplicaID != 0 {
			cleanupReplicas = append(cleanupReplicas, targetReplicaID)
		}
		cleanupReplicas = uniqueUint64s(cleanupReplicas)
	}
	nodeHostDir := ""
	if opts.pvcDataDir != "" {
		nodeHostDir = filepath.Join(opts.pvcDataDir, uuid)
	}
	return planStore{
		UUID:               uuid,
		Role:               role,
		NodeHostDir:        nodeHostDir,
		DeploymentID:       opts.deploymentID,
		ServiceAddress:     storeInfo.ServiceAddress,
		RaftAddress:        storeInfo.RaftAddress,
		ListenAddress:      storeInfo.RaftAddress,
		GossipAddress:      storeInfo.GossipAddress,
		TargetReplicaID:    targetReplicaID,
		LocalReplicas:      reportedReplicas,
		CleanupReplicas:    cleanupReplicas,
		Warnings:           reportWarnings,
		NeedsStopAndStart:  targetReplicaID != 0 && len(cleanupReplicas) > 0,
		PresentInHAKeeper:  targetReplicaID != 0,
		PresentInLocalData: len(reportedReplicas) > 0,
	}
}

func reportedShardReplicas(storeInfo logpb.LogStoreInfo, shardID uint64) []uint64 {
	seen := make(map[uint64]bool)
	for _, replica := range storeInfo.Replicas {
		if replica.ShardID == shardID {
			seen[replica.ReplicaID] = true
		}
	}
	ret := make([]uint64, 0, len(seen))
	for replicaID := range seen {
		ret = append(ret, replicaID)
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i] < ret[j] })
	return ret
}

func buildK8sActions(plan *repairPlan) []planAction {
	k := plan.K8s
	kubectl := "kubectl"
	if k != nil && k.Kubectl != "" {
		kubectl = k.Kubectl
	}
	namespace := shellQuote(plan.Namespace)
	actions := make([]planAction, 0)
	actions = append(actions, planAction{
		Type:        "discover-k8s",
		Description: "Discover LogService pods, services, configmaps, and PVCs in the namespace.",
		Command:     fmt.Sprintf("%s -n %s get pod,svc,cm,pvc -o wide", kubectl, namespace),
	})
	if k != nil && k.LogSelector != "" {
		actions = append(actions, planAction{
			Type:        "discover-logservice-pods",
			Description: "List LogService pods with labels and mounted PVC names. Confirm each store UUID maps to the expected pod/PVC before cleanup.",
			Command:     fmt.Sprintf("%s -n %s get pod -l %s -o wide --show-labels", kubectl, namespace, shellQuote(k.LogSelector)),
		})
	}
	if len(plan.HAKeeperAddresses) == 0 {
		actions = append(actions, planAction{
			Type:        "port-forward-hakeeper",
			Description: "Port-forward one running LogService/HAKeeper service, then rerun plan with --addresses 127.0.0.1:<local-port>.",
			Command:     fmt.Sprintf("%s -n %s port-forward svc/<logservice-or-hakeeper-service> 32001:<hakeeper-service-port>", kubectl, namespace),
		})
		return actions
	}
	actions = append(actions, planAction{
		Type:        "hakeeper-state",
		Description: "Save HAKeeper state used by this k8s repair plan.",
		Command:     fmt.Sprintf("mo-logservice-repair hakeeper state --addresses %s > /tmp/mo-logservice-hakeeper-state-before.json", strings.Join(plan.HAKeeperAddresses, ",")),
	})
	if len(plan.TargetShard.Replicas) > 0 {
		actions = append(actions, planAction{
			Type:        "hakeeper-repair",
			Description: "Write repair state and block stale/dirty stores before stopping pods and mounting PVCs.",
			Command:     fmt.Sprintf("mo-logservice-repair hakeeper repair --addresses %s --payload '%s'", strings.Join(plan.HAKeeperAddresses, ","), mustJSON(repairPayloadForPlan(plan, plan.InitialBlockedStores, "k8s wizard: block stale/dirty stores before PVC cleanup"))),
			ShardID:     plan.ShardID,
		})
	}
	for _, store := range plan.Stores {
		if len(store.CleanupReplicas) == 0 {
			continue
		}
		repairPod := k8sRepairPodName(store.UUID)
		pvcPlaceholder := k8sStorePVCPlaceholder(store.UUID)
		actions = append(actions,
			planAction{
				Type:        "k8s-stop-logservice-pod",
				Description: "Stop the LogService pod that owns this store UUID and make sure the PVC is no longer mounted by mo-service.",
				Store:       store.UUID,
				Command:     fmt.Sprintf("%s -n %s get pod,pvc -o wide | grep -E %s\n# Stop the owning LogService workload for store %s before mounting PVC %s in a repair pod.", kubectl, namespace, shellQuote(shortStoreID(store.UUID)+"|"+store.UUID), store.UUID, pvcPlaceholder),
			},
			planAction{
				Type:        "k8s-create-repair-pod",
				Description: "Create a temporary repair pod mounting the same PVC at the configured repair mount path.",
				Store:       store.UUID,
				Command:     k8sRepairPodManifestCommand(kubectl, plan.Namespace, repairPod, pvcPlaceholder, k),
			},
			planAction{
				Type:        "k8s-copy-repair-binary",
				Description: "Copy the tested mo-logservice-repair binary into the temporary repair pod.",
				Store:       store.UUID,
				Command:     fmt.Sprintf("%s -n %s cp /tmp/mo-logservice-repair %s:%s && %s -n %s exec %s -- chmod +x %s", kubectl, namespace, shellQuote(repairPod), shellQuote(k.RepairBinary), kubectl, namespace, shellQuote(repairPod), shellQuote(k.RepairBinary)),
			},
			planAction{
				Type:        "k8s-list-pvc-replicas",
				Description: "List shard-local residuals inside the PVC. Clean any extra node-<shard>-<replica> entries that are not already listed in this plan.",
				Store:       store.UUID,
				Command:     fmt.Sprintf("%s -n %s exec %s -- sh -c %s", kubectl, namespace, shellQuote(repairPod), shellQuote(fmt.Sprintf("find %s -path '*/tandb/node-%d-*' -o -path '*/snapshot-part-%d/snapshot-%d-*' | sort", shellQuote(store.NodeHostDir), plan.ShardID, plan.ShardID, plan.ShardID))),
			},
			planAction{
				Type:        "k8s-backup-pvc-nodehost",
				Description: "Back up the store nodehost directory inside the same PVC before cleanup.",
				Store:       store.UUID,
				Command:     fmt.Sprintf("%s -n %s exec %s -- sh -c %s", kubectl, namespace, shellQuote(repairPod), shellQuote(fmt.Sprintf("ts=$(date +%%Y%%m%%d-%%H%%M%%S); cp -a %s %s.repair-backup.$ts", shellQuote(store.NodeHostDir), shellQuote(store.NodeHostDir)))),
			},
		)
		for _, replicaID := range store.CleanupReplicas {
			actions = append(actions, planAction{
				Type:        "k8s-clean-replica",
				Description: "Clean one dirty local replica from the mounted PVC while the owning LogService pod is stopped.",
				Store:       store.UUID,
				ShardID:     plan.ShardID,
				ReplicaID:   replicaID,
				Command:     k8sCleanReplicaCommand(kubectl, plan.Namespace, repairPod, k, store, plan.ShardID, replicaID),
			})
		}
		actions = append(actions, planAction{
			Type:        "k8s-delete-repair-pod",
			Description: "Delete the temporary repair pod after PVC cleanup completes.",
			Store:       store.UUID,
			Command:     fmt.Sprintf("%s -n %s delete pod %s --wait=true", kubectl, namespace, shellQuote(repairPod)),
		})
	}
	for _, storeUUID := range plan.RebuildStores {
		actions = append(actions, planAction{
			Type:        "k8s-restart-logservice-pod",
			Description: "Start or unpause the original LogService pod/workload for this store and wait until it is Ready.",
			Store:       storeUUID,
			Command:     fmt.Sprintf("# Restart the original LogService pod/workload for store %s, then wait for Ready:\n%s -n %s get pod -o wide | grep -E %s", storeUUID, kubectl, namespace, shellQuote(shortStoreID(storeUUID)+"|"+storeUUID)),
		})
		actions = append(actions, planAction{
			Type:        "hakeeper-unblock",
			Description: "Unblock one cleaned store and wait for L/Start/snapshot restore before continuing.",
			Store:       storeUUID,
			ShardID:     plan.ShardID,
			Command:     fmt.Sprintf("mo-logservice-repair hakeeper unblock --addresses %s --payload '{\"shardID\":%d,\"stores\":[\"%s\"],\"reason\":\"k8s wizard: cleaned %s\"}'", strings.Join(plan.HAKeeperAddresses, ","), plan.ShardID, storeUUID, storeUUID),
		})
	}
	if len(plan.TargetShard.Replicas) > 0 {
		actions = append(actions, planAction{
			Type:        "hakeeper-repair-final",
			Description: "Refresh final repair state and keep only persistent stale stores blocked.",
			Command:     fmt.Sprintf("mo-logservice-repair hakeeper repair --addresses %s --payload '%s'", strings.Join(plan.HAKeeperAddresses, ","), mustJSON(repairPayloadForPlan(plan, plan.PersistentBlockedStores, "k8s wizard: repair complete"))),
			ShardID:     plan.ShardID,
		})
	}
	actions = append(actions, planAction{
		Type:        "verify",
		Description: "Verify final HAKeeper state, SQL, and LogService logs after cleaned pods rejoin.",
		Command:     fmt.Sprintf("mo-logservice-repair hakeeper state --addresses %s", strings.Join(plan.HAKeeperAddresses, ",")),
	})
	return actions
}

func buildLocalRepairPlan(opts wizardOptions) (*repairPlan, error) {
	if opts.baseDir == "" && opts.confDir != "" {
		opts.baseDir = filepath.Dir(opts.confDir)
	}
	if opts.confDir == "" {
		opts.confDir = filepath.Join(opts.baseDir, "conf")
	}
	configs, err := discoverLogConfigs(opts.confDir)
	if err != nil {
		return nil, err
	}
	addresses := splitAddresses(opts.addresses)
	if len(addresses) == 0 {
		addresses = discoverHAKeeperAddresses(configs, true)
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("cannot discover HAKeeper addresses; pass --addresses")
	}
	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()
	client, addr, err := connect(ctx, addresses)
	if err != nil {
		return nil, fmt.Errorf("connect hakeeper: %w", err)
	}
	defer client.Close()
	state, err := getClusterState(ctx, client, addr)
	if err != nil {
		return nil, fmt.Errorf("get cluster state: %w", err)
	}
	shard, ok := state.LogState.Shards[opts.shardID]
	if !ok {
		return nil, fmt.Errorf("shard %d not found in HAKeeper state", opts.shardID)
	}
	plan := &repairPlan{
		Version:           repairPlanVersion,
		Mode:              modeLocal,
		CreatedAt:         time.Now().Format(time.RFC3339),
		BaseDir:           opts.baseDir,
		ConfDir:           opts.confDir,
		ShardID:           opts.shardID,
		HAKeeperAddresses: addresses,
		TargetShard:       toPlanShard(shard),
		ApplySupported:    true,
		Local: &localPlanSettings{
			MOServicePath: opts.moServicePath,
			BackupDir:     filepath.Join(opts.baseDir, "repair-backup", time.Now().Format("20060102-150405")),
			LogDir:        filepath.Join(opts.baseDir, "logs"),
		},
	}
	targetByStore := replicaByStore(shard)
	sourceStore := targetByStore[shard.LeaderID]
	plan.SourceStore = sourceStore
	if sourceStore == "" {
		plan.Warnings = append(plan.Warnings, "HAKeeper has no leader for the target shard; source replica must be reviewed manually")
	}

	configByStore := make(map[string]localLogConfig)
	for _, cfg := range configs {
		configByStore[cfg.UUID] = cfg
	}
	seenStores := make(map[string]bool)
	for _, cfg := range configs {
		storeInfo, hasStoreInfo := state.LogState.Stores[cfg.UUID]
		store := buildPlanStore(opts.shardID, shard, cfg, sourceStore, storeInfo, hasStoreInfo)
		if opts.moServicePath != "" && store.MOServicePath == "" {
			store.MOServicePath = opts.moServicePath
		}
		if store.NeedsStopAndStart {
			plan.RebuildStores = append(plan.RebuildStores, store.UUID)
			plan.InitialBlockedStores = append(plan.InitialBlockedStores, store.UUID)
		}
		for _, warning := range store.Warnings {
			plan.Warnings = append(plan.Warnings, fmt.Sprintf("%s: %s", store.UUID, warning))
		}
		if store.Role == "stale" {
			plan.PersistentBlockedStores = append(plan.PersistentBlockedStores, store.UUID)
			plan.InitialBlockedStores = append(plan.InitialBlockedStores, store.UUID)
		}
		plan.Stores = append(plan.Stores, store)
		seenStores[store.UUID] = true
	}
	for store := range targetStores(shard) {
		if !seenStores[store] {
			plan.Warnings = append(plan.Warnings, fmt.Sprintf("target store %s has no local log config; it cannot be stopped or cleaned by local apply", store))
		}
	}
	for store, info := range state.LogState.Stores {
		if seenStores[store] || targetStores(shard)[store] {
			continue
		}
		for _, replica := range info.Replicas {
			if replica.ShardID == opts.shardID {
				plan.PersistentBlockedStores = append(plan.PersistentBlockedStores, store)
				plan.InitialBlockedStores = append(plan.InitialBlockedStores, store)
				plan.Warnings = append(plan.Warnings, fmt.Sprintf("HAKeeper still reports stale store %s for shard %d replica %d", store, opts.shardID, replica.ReplicaID))
			}
		}
	}
	plan.InitialBlockedStores = uniqueStrings(plan.InitialBlockedStores)
	plan.PersistentBlockedStores = uniqueStrings(plan.PersistentBlockedStores)
	plan.RebuildStores = uniqueStrings(plan.RebuildStores)
	plan.Actions = buildLocalActions(plan)
	_ = configByStore
	return plan, nil
}

type localLogConfig struct {
	UUID           string
	ConfigPath     string
	DataDir        string
	NodeHostDir    string
	DeploymentID   uint64
	ServiceAddress string
	RaftAddress    string
	ListenAddress  string
	GossipAddress  string
	HAKeeperAddrs  []string
}

func discoverLogConfigs(confDir string) ([]localLogConfig, error) {
	paths, err := filepath.Glob(filepath.Join(confDir, "log*.toml"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return nil, fmt.Errorf("no log*.toml found under %s", confDir)
	}
	ret := make([]localLogConfig, 0, len(paths))
	for _, path := range paths {
		cfg, err := parseLogConfig(path)
		if err != nil {
			return nil, err
		}
		ret = append(ret, cfg)
	}
	return ret, nil
}

func parseLogConfig(path string) (localLogConfig, error) {
	var raw logConfigFile
	if _, err := toml.DecodeFile(path, &raw); err != nil {
		return localLogConfig{}, err
	}
	if raw.LogService.UUID == "" {
		return localLogConfig{}, fmt.Errorf("%s: missing [logservice].uuid", path)
	}
	if raw.LogService.DeploymentID == 0 {
		return localLogConfig{}, fmt.Errorf("%s: missing [logservice].deployment-id", path)
	}
	if raw.LogService.DataDir == "" {
		return localLogConfig{}, fmt.Errorf("%s: missing [logservice].data-dir", path)
	}
	serviceHost := raw.LogService.ServiceHost
	if serviceHost == "" {
		serviceHost = "127.0.0.1"
	}
	serviceAddress := firstNonEmpty(
		raw.LogService.LogServiceServiceAddress,
		raw.LogService.LogServiceAddress,
		addressFromHostPort(serviceHost, raw.LogService.LogServicePort),
	)
	raftAddress := firstNonEmpty(
		raw.LogService.RaftAddress,
		addressFromHostPort(serviceHost, raw.LogService.RaftPort),
	)
	listenAddress := firstNonEmpty(raw.LogService.RaftListenAddress, raftAddress)
	gossipAddress := firstNonEmpty(
		raw.LogService.GossipAddress,
		raw.LogService.GossipListenAddress,
		addressFromHostPort(serviceHost, raw.LogService.GossipPort),
	)
	return localLogConfig{
		UUID:           raw.LogService.UUID,
		ConfigPath:     path,
		DataDir:        raw.LogService.DataDir,
		NodeHostDir:    filepath.Join(raw.LogService.DataDir, raw.LogService.UUID),
		DeploymentID:   raw.LogService.DeploymentID,
		ServiceAddress: serviceAddress,
		RaftAddress:    raftAddress,
		ListenAddress:  listenAddress,
		GossipAddress:  gossipAddress,
		HAKeeperAddrs:  raw.HAKeeperClient.ServiceAddresses,
	}, nil
}

func buildPlanStore(
	shardID uint64,
	shard logpb.LogShardInfo,
	cfg localLogConfig,
	sourceStore string,
	storeInfo logpb.LogStoreInfo,
	hasStoreInfo bool,
) planStore {
	targetReplicaID := uint64(0)
	for replicaID, store := range shard.Replicas {
		if store == cfg.UUID {
			targetReplicaID = replicaID
			break
		}
	}
	if targetReplicaID == 0 {
		for replicaID, store := range shard.NonVotingReplicas {
			if store == cfg.UUID {
				targetReplicaID = replicaID
				break
			}
		}
	}
	localReplicas := localShardReplicas(cfg.NodeHostDir, cfg.DeploymentID, shardID)
	replicaReported := false
	replicaHealthy := false
	reportWarnings := []string(nil)
	if targetReplicaID != 0 && hasStoreInfo {
		replicaReported, replicaHealthy, reportWarnings = storeReportsHealthyReplica(storeInfo, shard, targetReplicaID)
	}
	role := "unused"
	switch {
	case cfg.UUID == sourceStore:
		role = "source"
	case targetReplicaID != 0:
		if replicaHealthy {
			role = "target"
		} else {
			role = "rebuild"
			if replicaReported && len(reportWarnings) > 0 {
				reportWarnings = append(reportWarnings, "target replica is reported by HAKeeper heartbeat, but its local shard view differs from the target shard; local data must be rebuilt")
			}
		}
	case len(localReplicas) > 0:
		role = "stale"
	}
	cleanupReplicas := []uint64(nil)
	if role == "rebuild" {
		cleanupReplicas = append(cleanupReplicas, localReplicas...)
	} else if role == "target" {
		for _, replicaID := range localReplicas {
			if replicaID != targetReplicaID {
				cleanupReplicas = append(cleanupReplicas, replicaID)
			}
		}
		if len(cleanupReplicas) > 0 {
			role = "cleanup"
		}
	}
	pids, binary := findMOServiceProcesses(cfg.ConfigPath)
	return planStore{
		UUID:               cfg.UUID,
		Role:               role,
		ConfigPath:         cfg.ConfigPath,
		DataDir:            cfg.DataDir,
		NodeHostDir:        cfg.NodeHostDir,
		DeploymentID:       cfg.DeploymentID,
		ServiceAddress:     cfg.ServiceAddress,
		RaftAddress:        cfg.RaftAddress,
		ListenAddress:      cfg.ListenAddress,
		GossipAddress:      cfg.GossipAddress,
		TargetReplicaID:    targetReplicaID,
		LocalReplicas:      localReplicas,
		CleanupReplicas:    cleanupReplicas,
		ProcessIDs:         pids,
		Warnings:           reportWarnings,
		MOServicePath:      binary,
		NeedsStopAndStart:  targetReplicaID != 0 && len(cleanupReplicas) > 0,
		PresentInHAKeeper:  targetReplicaID != 0,
		PresentInLocalData: len(localReplicas) > 0,
	}
}

func storeReportsHealthyReplica(storeInfo logpb.LogStoreInfo, shard logpb.LogShardInfo, replicaID uint64) (bool, bool, []string) {
	for _, replica := range storeInfo.Replicas {
		if replica.ShardID != shard.ShardID || replica.ReplicaID != replicaID {
			continue
		}
		warnings := compareReportedShard(replica.LogShardInfo, shard)
		return true, reportedShardMembershipMatches(replica.LogShardInfo, shard), warnings
	}
	return false, false, nil
}

func compareReportedShard(reported logpb.LogShardInfo, target logpb.LogShardInfo) []string {
	warnings := []string(nil)
	if reported.ShardID != target.ShardID {
		warnings = append(warnings, fmt.Sprintf("reported shard id %d differs from target shard id %d", reported.ShardID, target.ShardID))
	}
	if reported.Epoch != target.Epoch {
		warnings = append(warnings, fmt.Sprintf("reported epoch %d differs from target epoch %d", reported.Epoch, target.Epoch))
	}
	if !sameReplicaMap(reported.Replicas, target.Replicas) {
		warnings = append(warnings, fmt.Sprintf("reported voting replicas %s differ from target %s", formatReplicaMap(reported.Replicas), formatReplicaMap(target.Replicas)))
	}
	if !sameReplicaMap(reported.NonVotingReplicas, target.NonVotingReplicas) {
		warnings = append(warnings, fmt.Sprintf("reported non-voting replicas %s differ from target %s", formatReplicaMap(reported.NonVotingReplicas), formatReplicaMap(target.NonVotingReplicas)))
	}
	return warnings
}

func reportedShardMembershipMatches(reported logpb.LogShardInfo, target logpb.LogShardInfo) bool {
	return reported.ShardID == target.ShardID &&
		sameReplicaMap(reported.Replicas, target.Replicas) &&
		sameReplicaMap(reported.NonVotingReplicas, target.NonVotingReplicas)
}

func sameReplicaMap(a map[uint64]string, b map[uint64]string) bool {
	if len(a) != len(b) {
		return false
	}
	for id, store := range a {
		if b[id] != store {
			return false
		}
	}
	return true
}

func formatReplicaMap(m map[uint64]string) string {
	if len(m) == 0 {
		return "{}"
	}
	keys := make([]uint64, 0, len(m))
	for id := range m {
		keys = append(keys, id)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	parts := make([]string, 0, len(keys))
	for _, id := range keys {
		parts = append(parts, fmt.Sprintf("%d:%s", id, m[id]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func localShardReplicas(nodeHostDir string, deploymentID uint64, shardID uint64) []uint64 {
	seen := make(map[uint64]bool)
	metadataPath := filepath.Join(nodeHostDir, logMetadataFilename)
	if md, err := readLogMetadata(metadataPath); err == nil {
		for _, rec := range md.Shards {
			if rec.ShardID == shardID {
				seen[rec.ReplicaID] = true
			}
		}
	}
	deploymentDir := fmt.Sprintf("%020d", deploymentID)
	matches, _ := filepath.Glob(filepath.Join(nodeHostDir, "*", deploymentDir, "tandb", fmt.Sprintf("node-%d-*", shardID)))
	for _, match := range matches {
		name := filepath.Base(match)
		idText := strings.TrimPrefix(name, fmt.Sprintf("node-%d-", shardID))
		if id, err := strconv.ParseUint(idText, 10, 64); err == nil {
			seen[id] = true
		}
	}
	ret := make([]uint64, 0, len(seen))
	for id := range seen {
		ret = append(ret, id)
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i] < ret[j] })
	return ret
}

func buildLocalActions(plan *repairPlan) []planAction {
	actions := make([]planAction, 0)
	actions = append(actions, planAction{
		Type:        "hakeeper-repair",
		Description: "Write repair state and block stale/dirty stores before local cleanup.",
		Command:     fmt.Sprintf("mo-logservice-repair hakeeper repair --addresses %s --payload '%s'", strings.Join(plan.HAKeeperAddresses, ","), mustJSON(repairPayloadForPlan(plan, plan.InitialBlockedStores, "wizard: block stale/dirty stores before cleanup"))),
		ShardID:     plan.ShardID,
	})
	for _, store := range plan.Stores {
		if len(store.CleanupReplicas) == 0 {
			continue
		}
		actions = append(actions, planAction{
			Type:        "stop-store",
			Description: "Stop this LogService before deleting Dragonboat local data.",
			Store:       store.UUID,
			Command:     fmt.Sprintf("pgrep -f -- 'mo-service -cfg %s' | xargs -r kill -TERM", shellQuote(store.ConfigPath)),
		})
	}
	actions = append(actions, planAction{
		Type:        "backup",
		Description: "Back up logservice-data for stores that will be cleaned.",
		Command:     fmt.Sprintf("mkdir -p %s && cp -a <node-host-dir> %s/", shellQuote(plan.Local.BackupDir), shellQuote(plan.Local.BackupDir)),
	})
	for _, store := range plan.Stores {
		if len(store.CleanupReplicas) == 0 {
			continue
		}
		for _, replicaID := range store.CleanupReplicas {
			actions = append(actions, planAction{
				Type:        "clean-replica",
				Description: "Clean one dirty local replica while the owning LogService is stopped.",
				Store:       store.UUID,
				ShardID:     plan.ShardID,
				ReplicaID:   replicaID,
				Command: fmt.Sprintf("mo-logservice-repair local clean-replica --deployment-id %d --node-host-id %s --node-host-dir %s --raft-address %s --listen-address %s --gossip-address %s --shard-id %d --replica-id %d",
					store.DeploymentID,
					shellQuote(store.UUID),
					shellQuote(store.NodeHostDir),
					shellQuote(store.RaftAddress),
					shellQuote(store.ListenAddress),
					shellQuote(store.GossipAddress),
					plan.ShardID,
					replicaID,
				),
			})
		}
	}
	for _, store := range plan.Stores {
		if len(store.CleanupReplicas) == 0 {
			continue
		}
		actions = append(actions, planAction{
			Type:        "start-store",
			Description: "Restart the cleaned LogService.",
			Store:       store.UUID,
			Command:     fmt.Sprintf("%s -cfg %s", shellQuote(firstNonEmpty(store.MOServicePath, plan.Local.MOServicePath, "mo-service")), shellQuote(store.ConfigPath)),
		})
	}
	for _, storeUUID := range plan.RebuildStores {
		actions = append(actions, planAction{
			Type:        "hakeeper-unblock",
			Description: "Unblock one cleaned store and wait for L/Start/snapshot restore before continuing.",
			Store:       storeUUID,
			ShardID:     plan.ShardID,
			Command:     fmt.Sprintf("mo-logservice-repair hakeeper unblock --addresses %s --payload '{\"shardID\":%d,\"stores\":[\"%s\"],\"reason\":\"wizard: cleaned %s\"}'", strings.Join(plan.HAKeeperAddresses, ","), plan.ShardID, storeUUID, storeUUID),
		})
	}
	actions = append(actions, planAction{
		Type:        "hakeeper-repair-final",
		Description: "Refresh final repair state and keep only persistent stale stores blocked.",
		Command:     fmt.Sprintf("mo-logservice-repair hakeeper repair --addresses %s --payload '%s'", strings.Join(plan.HAKeeperAddresses, ","), mustJSON(repairPayloadForPlan(plan, plan.PersistentBlockedStores, "wizard: repair complete"))),
		ShardID:     plan.ShardID,
	})
	actions = append(actions, planAction{
		Type:        "verify",
		Description: "Read final HAKeeper state and scan new logs for repair errors.",
		Command:     fmt.Sprintf("mo-logservice-repair hakeeper state --addresses %s", strings.Join(plan.HAKeeperAddresses, ",")),
	})
	return actions
}

func applyRepairPlan(ctx context.Context, plan *repairPlan, opts wizardOptions) error {
	if !plan.ApplySupported {
		return fmt.Errorf("%s mode apply is not supported yet; use the generated actions as a runbook", plan.Mode)
	}
	if plan.Mode != modeLocal {
		return fmt.Errorf("unsupported apply mode %q", plan.Mode)
	}
	timeout := opts.timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	stableHAKeeperAddresses := stableHAKeeperAddressesForApply(plan)
	fmt.Println("step 1: write repair state and block stale/dirty stores")
	if err := confirmApplyStep(opts, "step 1", fmt.Sprintf("block stores %v in HAKeeper repair state", plan.InitialBlockedStores)); err != nil {
		return err
	}
	if _, err := applyHAKeeperWithNewConnection(ctx, stableHAKeeperAddresses, timeout, repairPayloadForPlan(plan, plan.InitialBlockedStores, "wizard: block stale/dirty stores before cleanup")); err != nil {
		return err
	}
	rebuildStores := storesByUUID(plan.Stores, plan.RebuildStores)
	fmt.Println("step 2: stop LogService stores that need local cleanup")
	if err := confirmApplyStep(opts, "step 2", fmt.Sprintf("stop stores %v", storesWithCleanup(rebuildStores))); err != nil {
		return err
	}
	for _, store := range rebuildStores {
		if len(store.CleanupReplicas) == 0 {
			continue
		}
		if err := stopStore(store); err != nil {
			return err
		}
	}
	fmt.Println("step 3: back up local logservice-data")
	if err := confirmApplyStep(opts, "step 3", fmt.Sprintf("backup store data to %s", plan.Local.BackupDir)); err != nil {
		return err
	}
	for _, store := range rebuildStores {
		if len(store.CleanupReplicas) == 0 {
			continue
		}
		if err := backupStore(plan.Local.BackupDir, store); err != nil {
			return err
		}
	}
	fmt.Println("step 4: clean dirty local replicas")
	if err := confirmApplyStep(opts, "step 4", cleanupSummary(rebuildStores)); err != nil {
		return err
	}
	for _, store := range rebuildStores {
		for _, replicaID := range store.CleanupReplicas {
			if err := cleanReplica(
				store.DeploymentID,
				store.UUID,
				store.NodeHostDir,
				store.RaftAddress,
				store.ListenAddress,
				store.GossipAddress,
				plan.ShardID,
				replicaID,
				200,
			); err != nil {
				return err
			}
		}
	}
	fmt.Println("step 5: restart cleaned LogService stores")
	if err := confirmApplyStep(opts, "step 5", fmt.Sprintf("restart stores %v", storesWithCleanup(rebuildStores))); err != nil {
		return err
	}
	for _, store := range rebuildStores {
		if len(store.CleanupReplicas) == 0 {
			continue
		}
		if err := startStore(plan, store); err != nil {
			return err
		}
	}
	fmt.Println("step 6: unblock cleaned stores one by one")
	for _, store := range rebuildStores {
		if len(store.CleanupReplicas) == 0 {
			continue
		}
		if err := confirmApplyStep(opts, "step 6", "unblock cleaned store "+store.UUID); err != nil {
			return err
		}
		req := repairPayload{
			Op:         "unblock",
			ShardID:    plan.ShardID,
			ShardIDSet: true,
			Stores:     []string{store.UUID},
			Reason:     "wizard: cleaned " + store.UUID,
		}
		if _, err := applyHAKeeperWithNewConnection(ctx, stableHAKeeperAddresses, timeout, req); err != nil {
			return err
		}
		fmt.Printf("unblocked %s; wait 30s for L/Start/snapshot restore\n", store.UUID)
		time.Sleep(30 * time.Second)
	}
	fmt.Println("step 7: write final repair state")
	if err := confirmApplyStep(opts, "step 7", fmt.Sprintf("keep persistent blocked stores %v", plan.PersistentBlockedStores)); err != nil {
		return err
	}
	if _, err := applyHAKeeperWithNewConnection(ctx, stableHAKeeperAddresses, timeout, repairPayloadForPlan(plan, plan.PersistentBlockedStores, "wizard: repair complete")); err != nil {
		return err
	}
	fmt.Println("step 8: final HAKeeper state")
	state, err := getHAKeeperStateWithNewConnection(ctx, stableHAKeeperAddresses, timeout)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{
		"logShards": state.LogState.Shards,
		"repairs":   state.LogShardRepairs,
	})
}

func stableHAKeeperAddressesForApply(plan *repairPlan) []string {
	addressByStore := make(map[string]string)
	rebuildStores := make(map[string]bool)
	sourceStores := make(map[string]bool)
	for _, store := range plan.Stores {
		if store.ServiceAddress != "" {
			addressByStore[store.UUID] = store.ServiceAddress
		}
		if len(store.CleanupReplicas) > 0 {
			rebuildStores[store.UUID] = true
		}
		if store.UUID == plan.SourceStore || store.Role == "source" {
			sourceStores[store.UUID] = true
		}
	}
	rank := make(map[string]int)
	for store, address := range addressByStore {
		switch {
		case sourceStores[store]:
			rank[address] = 0
		case rebuildStores[store]:
			rank[address] = 2
		default:
			rank[address] = 1
		}
	}
	ret := append([]string(nil), plan.HAKeeperAddresses...)
	sort.SliceStable(ret, func(i, j int) bool {
		ri, ok := rank[ret[i]]
		if !ok {
			ri = 1
		}
		rj, ok := rank[ret[j]]
		if !ok {
			rj = 1
		}
		if ri != rj {
			return ri < rj
		}
		return ret[i] < ret[j]
	})
	return uniqueStringsKeepOrder(ret)
}

func confirmApplyStep(opts wizardOptions, step string, description string) error {
	if opts.yes {
		return nil
	}
	ok, err := askYesNo(fmt.Sprintf("Run %s: %s", step, description), false)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("apply cancelled at %s", step)
	}
	return nil
}

func storesWithCleanup(stores []planStore) []string {
	ret := make([]string, 0, len(stores))
	for _, store := range stores {
		if len(store.CleanupReplicas) > 0 {
			ret = append(ret, store.UUID)
		}
	}
	return ret
}

func cleanupSummary(stores []planStore) string {
	parts := make([]string, 0, len(stores))
	for _, store := range stores {
		if len(store.CleanupReplicas) == 0 {
			continue
		}
		ids := make([]string, 0, len(store.CleanupReplicas))
		for _, replicaID := range store.CleanupReplicas {
			ids = append(ids, strconv.FormatUint(replicaID, 10))
		}
		parts = append(parts, fmt.Sprintf("%s replicas [%s]", store.UUID, strings.Join(ids, ",")))
	}
	if len(parts) == 0 {
		return "no local replicas need cleanup"
	}
	return "delete local data for " + strings.Join(parts, "; ")
}

func applyHAKeeperWithNewConnection(
	ctx context.Context,
	addresses []string,
	timeout time.Duration,
	req repairPayload,
) (repairResult, error) {
	opCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client, addr, err := connect(opCtx, addresses)
	if err != nil {
		return repairResult{}, fmt.Errorf("connect hakeeper: %w", err)
	}
	defer client.Close()
	return applyHAKeeperPayload(opCtx, client, addr, req)
}

func getHAKeeperStateWithNewConnection(
	ctx context.Context,
	addresses []string,
	timeout time.Duration,
) (logpb.CheckerState, error) {
	opCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client, addr, err := connect(opCtx, addresses)
	if err != nil {
		return logpb.CheckerState{}, fmt.Errorf("connect hakeeper: %w", err)
	}
	defer client.Close()
	return getClusterState(opCtx, client, addr)
}

func stopStore(store planStore) error {
	pids, _ := findMOServiceProcesses(store.ConfigPath)
	for _, pid := range pids {
		proc, err := os.FindProcess(pid)
		if err != nil {
			return err
		}
		if err := proc.Signal(syscall.SIGTERM); err != nil {
			return err
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		pids, _ := findMOServiceProcesses(store.ConfigPath)
		if len(pids) == 0 {
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("store %s still has running mo-service processes for %s", store.UUID, store.ConfigPath)
}

func backupStore(backupRoot string, store planStore) error {
	if err := os.MkdirAll(backupRoot, 0750); err != nil {
		return err
	}
	dst := filepath.Join(backupRoot, store.UUID+"-logservice-data")
	cmd := exec.Command("cp", "-a", store.NodeHostDir, dst)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func startStore(plan *repairPlan, store planStore) error {
	binary := firstNonEmpty(store.MOServicePath, plan.Local.MOServicePath, "mo-service")
	if binary == "" {
		return fmt.Errorf("no mo-service binary found for store %s; pass --mo-service", store.UUID)
	}
	if err := os.MkdirAll(plan.Local.LogDir, 0750); err != nil {
		return err
	}
	logPath := filepath.Join(plan.Local.LogDir, filepath.Base(strings.TrimSuffix(store.ConfigPath, ".toml"))+"-wizard-restart.out")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	defer logFile.Close()
	cmd := exec.Command(binary, "-cfg", store.ConfigPath)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	fmt.Printf("started %s pid=%d log=%s\n", store.UUID, cmd.Process.Pid, logPath)
	return nil
}

func repairPayloadForPlan(plan *repairPlan, blockedStores []string, reason string) repairPayload {
	return repairPayload{
		Op: "repair",
		Shard: shardInput{
			ShardID:           plan.TargetShard.ShardID,
			ShardIDSet:        true,
			Replicas:          copyReplicaMap(plan.TargetShard.Replicas),
			NonVotingReplicas: copyReplicaMap(plan.TargetShard.NonVotingReplicas),
			Epoch:             plan.TargetShard.Epoch,
			LeaderID:          plan.TargetShard.LeaderID,
			Term:              plan.TargetShard.Term,
		},
		BlockedStores: blockedStores,
		Reason:        reason,
		Force:         true,
	}
}

func toPlanShard(shard logpb.LogShardInfo) planShard {
	return planShard{
		ShardID:           shard.ShardID,
		Replicas:          copyReplicaMap(shard.Replicas),
		NonVotingReplicas: copyReplicaMap(shard.NonVotingReplicas),
		Epoch:             shard.Epoch,
		LeaderID:          shard.LeaderID,
		Term:              shard.Term,
	}
}

func copyReplicaMap(in map[uint64]string) map[uint64]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[uint64]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func replicaByStore(shard logpb.LogShardInfo) map[uint64]string {
	out := make(map[uint64]string)
	for replicaID, store := range shard.Replicas {
		out[replicaID] = store
	}
	for replicaID, store := range shard.NonVotingReplicas {
		out[replicaID] = store
	}
	return out
}

func targetStores(shard logpb.LogShardInfo) map[string]bool {
	out := make(map[string]bool)
	for _, store := range shard.Replicas {
		out[store] = true
	}
	for _, store := range shard.NonVotingReplicas {
		out[store] = true
	}
	return out
}

func discoverHAKeeperAddresses(configs []localLogConfig, preferRunning bool) []string {
	configByServiceAddress := make(map[string]localLogConfig)
	for _, cfg := range configs {
		if cfg.ServiceAddress != "" {
			configByServiceAddress[cfg.ServiceAddress] = cfg
		}
	}
	for _, cfg := range configs {
		if len(cfg.HAKeeperAddrs) > 0 {
			addresses := uniqueStrings(cfg.HAKeeperAddrs)
			if preferRunning {
				filtered := filterRunningLocalAddresses(addresses, configByServiceAddress)
				if len(filtered) > 0 {
					return filtered
				}
			}
			return addresses
		}
	}
	addrs := make([]string, 0, len(configs))
	for _, cfg := range configs {
		if cfg.ServiceAddress != "" {
			addrs = append(addrs, cfg.ServiceAddress)
		}
	}
	addresses := uniqueStrings(addrs)
	if preferRunning {
		filtered := filterRunningLocalAddresses(addresses, configByServiceAddress)
		if len(filtered) > 0 {
			return filtered
		}
	}
	return addresses
}

func filterRunningLocalAddresses(addresses []string, configs map[string]localLogConfig) []string {
	ret := make([]string, 0, len(addresses))
	for _, addr := range addresses {
		cfg, ok := configs[addr]
		if !ok {
			ret = append(ret, addr)
			continue
		}
		pids, _ := findMOServiceProcesses(cfg.ConfigPath)
		if len(pids) > 0 {
			ret = append(ret, addr)
		}
	}
	return ret
}

func findMOServiceProcesses(configPath string) ([]int, string) {
	cmd := exec.Command("pgrep", "-af", "--", "mo-service -cfg "+configPath)
	out, err := cmd.Output()
	if err != nil {
		return nil, ""
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	pids := make([]int, 0, len(lines))
	binary := ""
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}
		pids = append(pids, pid)
		if len(fields) > 1 && binary == "" {
			binary = fields[1]
		}
	}
	return pids, binary
}

func storesByUUID(stores []planStore, uuids []string) []planStore {
	want := make(map[string]bool)
	for _, uuid := range uuids {
		want[uuid] = true
	}
	out := make([]planStore, 0, len(uuids))
	for _, store := range stores {
		if want[store.UUID] {
			out = append(out, store)
		}
	}
	return out
}

func confirmPlanDetails(plan *repairPlan) error {
	if len(plan.HAKeeperAddresses) > 0 {
		ok, err := askYesNo("Confirm HAKeeper addresses "+strings.Join(plan.HAKeeperAddresses, ","), false)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("apply cancelled: HAKeeper addresses not confirmed")
		}
	}
	if plan.SourceStore != "" {
		ok, err := askYesNo("Confirm source store "+plan.SourceStore, false)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("apply cancelled: source store not confirmed")
		}
	}
	for _, store := range plan.Stores {
		if !storeNeedsHumanConfirmation(store) {
			continue
		}
		fmt.Printf("Review store %s\n", store.UUID)
		fmt.Printf("  role=%s targetReplica=%d local=%v cleanup=%v\n", store.Role, store.TargetReplicaID, store.LocalReplicas, store.CleanupReplicas)
		if store.ConfigPath != "" {
			fmt.Printf("  config=%s\n", store.ConfigPath)
		}
		if store.NodeHostDir != "" {
			fmt.Printf("  nodeHostDir=%s\n", store.NodeHostDir)
		}
		for _, warning := range store.Warnings {
			fmt.Printf("  warning: %s\n", warning)
		}
		ok, err := askYesNo("Confirm this store action", false)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("apply cancelled: store %s not confirmed", store.UUID)
		}
	}
	return nil
}

func storeNeedsHumanConfirmation(store planStore) bool {
	return len(store.CleanupReplicas) > 0 || store.Role == "stale" || len(store.Warnings) > 0
}

func printPlanSummary(plan *repairPlan) {
	fmt.Printf("Repair plan %s mode=%s shard=%d\n", plan.Version, plan.Mode, plan.ShardID)
	if len(plan.HAKeeperAddresses) > 0 {
		fmt.Printf("HAKeeper: %s\n", strings.Join(plan.HAKeeperAddresses, ","))
	}
	if len(plan.Stores) > 0 {
		fmt.Println("Stores:")
		for _, store := range plan.Stores {
			fmt.Printf("  %s role=%s targetReplica=%d local=%v cleanup=%v\n",
				store.UUID,
				store.Role,
				store.TargetReplicaID,
				store.LocalReplicas,
				store.CleanupReplicas,
			)
			for _, warning := range store.Warnings {
				fmt.Printf("    warning: %s\n", warning)
			}
		}
	}
	if len(plan.InitialBlockedStores) > 0 {
		fmt.Printf("Initial blocked stores: %v\n", plan.InitialBlockedStores)
	}
	if len(plan.PersistentBlockedStores) > 0 {
		fmt.Printf("Persistent blocked stores: %v\n", plan.PersistentBlockedStores)
	}
	if len(plan.Warnings) > 0 {
		fmt.Println("Warnings:")
		for _, warning := range plan.Warnings {
			fmt.Printf("  - %s\n", warning)
		}
	}
	fmt.Printf("Actions: %d\n", len(plan.Actions))
}

func writePlanFile(path string, plan *repairPlan) error {
	data, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0640)
}

func readPlanFile(path string) (*repairPlan, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var plan repairPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		return nil, err
	}
	if plan.Version != repairPlanVersion {
		return nil, fmt.Errorf("unsupported plan version %q", plan.Version)
	}
	return &plan, nil
}

func askChoice(prompt string, choices []string, def string) (string, error) {
	fmt.Printf("%s [%s] default=%s: ", prompt, strings.Join(choices, "/"), def)
	line, err := readStdinLine()
	if err != nil {
		return "", err
	}
	line = strings.TrimSpace(line)
	if line == "" {
		line = def
	}
	for _, choice := range choices {
		if line == choice {
			return line, nil
		}
	}
	return "", fmt.Errorf("invalid choice %q", line)
}

func askLine(prompt string) (string, error) {
	fmt.Printf("%s: ", prompt)
	line, err := readStdinLine()
	if err != nil {
		return "", err
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return "", fmt.Errorf("%s is required", prompt)
	}
	return line, nil
}

func askLineDefault(prompt string, def string) (string, error) {
	fmt.Printf("%s [%s]: ", prompt, def)
	line, err := readStdinLine()
	if err != nil {
		return "", err
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return def, nil
	}
	return line, nil
}

func askYesNo(prompt string, def bool) (bool, error) {
	defText := "n"
	if def {
		defText = "y"
	}
	line, err := askLineDefault(prompt+" (y/n)", defText)
	if err != nil {
		return false, err
	}
	switch strings.ToLower(line) {
	case "y", "yes":
		return true, nil
	case "n", "no":
		return false, nil
	default:
		return false, fmt.Errorf("invalid yes/no answer %q", line)
	}
}

func readStdinLine() (string, error) {
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil && len(line) == 0 {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func addressFromHostPort(host string, port int) string {
	if port == 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d", host, port)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]bool)
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func uniqueStringsKeepOrder(values []string) []string {
	seen := make(map[string]bool)
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func uniqueUint64s(values []uint64) []uint64 {
	seen := make(map[uint64]bool)
	out := make([]uint64, 0, len(values))
	for _, value := range values {
		if value == 0 || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func kubectlCommand(kubeContext string) string {
	if kubeContext == "" {
		return "kubectl"
	}
	return "kubectl --context " + shellQuote(kubeContext)
}

func shortStoreID(uuid string) string {
	parts := strings.Split(uuid, "-")
	if len(parts) > 0 && parts[len(parts)-1] != "" {
		return parts[len(parts)-1]
	}
	if len(uuid) <= 8 {
		return uuid
	}
	return uuid[len(uuid)-8:]
}

func k8sNameSafe(value string) string {
	value = strings.ToLower(value)
	replacer := strings.NewReplacer("_", "-", ".", "-", ":", "-")
	value = replacer.Replace(value)
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

func k8sRepairPodName(storeUUID string) string {
	name := "mo-logservice-repair-" + k8sNameSafe(shortStoreID(storeUUID))
	if len(name) > 63 {
		return name[:63]
	}
	return name
}

func k8sStorePVCPlaceholder(storeUUID string) string {
	return "<pvc-name-for-" + shortStoreID(storeUUID) + ">"
}

func k8sRepairPodManifestCommand(kubectl string, namespace string, podName string, pvcName string, settings *k8sPlanSettings) string {
	image := "matrixorigin/matrixone:latest"
	mountPath := "/repair-pvc"
	if settings != nil {
		image = firstNonEmpty(settings.RepairImage, image)
		mountPath = firstNonEmpty(settings.PVCMountPath, mountPath)
	}
	return fmt.Sprintf(`cat <<'YAML' >/tmp/%s.yaml
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  restartPolicy: Never
  containers:
  - name: repair
    image: %s
    command: ["sleep", "36000"]
    volumeMounts:
    - name: logservice-data
      mountPath: %s
  volumes:
  - name: logservice-data
    persistentVolumeClaim:
      claimName: %s
YAML
%s -n %s apply -f /tmp/%s.yaml
%s -n %s wait --for=condition=Ready pod/%s --timeout=120s`,
		podName,
		podName,
		image,
		mountPath,
		pvcName,
		kubectl,
		shellQuote(namespace),
		podName,
		kubectl,
		shellQuote(namespace),
		shellQuote(podName),
	)
}

func k8sCleanReplicaCommand(kubectl string, namespace string, repairPod string, settings *k8sPlanSettings, store planStore, shardID uint64, replicaID uint64) string {
	binary := "/tmp/mo-logservice-repair"
	deploymentID := "<deployment-id>"
	if settings != nil {
		binary = firstNonEmpty(settings.RepairBinary, binary)
		if settings.DeploymentID != 0 {
			deploymentID = strconv.FormatUint(settings.DeploymentID, 10)
		}
	}
	raftAddress := firstNonEmpty(store.RaftAddress, "127.0.0.1:19000")
	listenAddress := firstNonEmpty(store.ListenAddress, raftAddress)
	gossipAddress := firstNonEmpty(store.GossipAddress, "127.0.0.1:19002")
	command := fmt.Sprintf("%s local clean-replica --deployment-id %s --node-host-id %s --node-host-dir %s --raft-address %s --listen-address %s --gossip-address %s --shard-id %d --replica-id %d",
		shellQuote(binary),
		deploymentID,
		shellQuote(store.UUID),
		shellQuote(store.NodeHostDir),
		shellQuote(raftAddress),
		shellQuote(listenAddress),
		shellQuote(gossipAddress),
		shardID,
		replicaID,
	)
	return fmt.Sprintf("%s -n %s exec %s -- sh -c %s", kubectl, shellQuote(namespace), shellQuote(repairPod), shellQuote(command))
}

func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}

func mustJSON(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(data)
}
