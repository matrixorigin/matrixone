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
	mode                 string
	baseDir              string
	confDir              string
	shardID              uint64
	shardIDSet           bool
	shards               string
	addresses            string
	output               string
	planPath             string
	apply                bool
	yes                  bool
	timeout              time.Duration
	moServicePath        string
	namespace            string
	kubeContext          string
	kubectlPath          string
	k8sLogSelector       string
	pvcMountPath         string
	pvcDataDir           string
	repairImage          string
	repairBinary         string
	deploymentID         uint64
	manualServiceControl bool
	autoRestartPods      bool
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
		stdoutf("plan written to %s\n", opts.output)
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
	plans, err := readPlanFiles(opts.planPath)
	if err != nil {
		return err
	}
	for i, plan := range plans {
		if len(plans) > 1 {
			stdoutf("=== Plan %d/%d ===\n", i+1, len(plans))
		}
		printPlanSummary(plan)
	}
	if err := validateLocalPlanFreshness(plans); err != nil {
		return err
	}
	if !opts.yes {
		for _, plan := range plans {
			if err := confirmPlanDetails(plan); err != nil {
				return err
			}
		}
		confirm, err := askLine("This may stop/restart LogService and edit local replica data. Type APPLY to continue")
		if err != nil {
			return err
		}
		if confirm != "APPLY" {
			return fmt.Errorf("apply cancelled")
		}
	}
	return applyRepairPlans(context.Background(), plans, opts)
}

func runRecover(mode string, args []string) error {
	opts, err := parseWizardFlags(mode+" recover", args)
	if err != nil {
		return err
	}
	opts.mode = mode
	shardIDs, err := selectedRepairShardIDs(opts)
	if err != nil {
		return err
	}
	switch mode {
	case modeLocal:
		if opts.baseDir == "" && opts.confDir == "" {
			return fmt.Errorf("--base or --conf-dir is required for local recover")
		}
	case modeK8s:
		if opts.namespace == "" {
			return fmt.Errorf("--namespace is required for k8s recover")
		}
		if opts.addresses == "" {
			return fmt.Errorf("--addresses is required for k8s recover after HAKeeper port-forward/service discovery")
		}
	default:
		return fmt.Errorf("unsupported recover mode %q", mode)
	}

	plans, err := buildOnlineRecoveryPlans(opts, shardIDs)
	if err != nil {
		return err
	}
	for i, plan := range plans {
		if len(plans) > 1 {
			stdoutf("=== Online repair plan %d/%d ===\n", i+1, len(plans))
		}
		printPlanSummary(plan)
	}
	if opts.output != "" {
		if err := writePlanBundleFile(opts.output, plans); err != nil {
			return err
		}
		stdoutf("plan bundle written to %s\n", opts.output)
	}
	if !opts.yes {
		prompt := "Proceed with online recovery; the CLI will not backup LogService data"
		if opts.mode == modeLocal || opts.manualServiceControl || !opts.autoRestartPods {
			prompt += " and expects you to stop/start listed LogService pods/processes"
		} else {
			prompt += " and will restart listed LogService pods one by one"
		}
		ok, err := askYesNo(prompt, false)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("recover cancelled")
		}
		confirm, err := askLine("This writes HAKeeper repair state and expects you to restart listed LogService pods/processes. Type APPLY to continue")
		if err != nil {
			return err
		}
		if confirm != "APPLY" {
			return fmt.Errorf("recover cancelled")
		}
	}
	return applyOnlineRecoveryPlans(context.Background(), plans, opts)
}

func selectedRepairShardIDs(opts wizardOptions) ([]uint64, error) {
	raw := opts.shards
	if raw == "" {
		if !opts.shardIDSet {
			return nil, fmt.Errorf("--shard or --shards is required")
		}
		return []uint64{opts.shardID}, nil
	}
	parts := strings.Split(raw, ",")
	ids := make([]uint64, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid shard id %q: %w", part, err)
		}
		ids = append(ids, id)
	}
	if opts.shardIDSet {
		ids = append(ids, opts.shardID)
	}
	ids = uniqueShardIDs(ids)
	if len(ids) == 0 {
		return nil, fmt.Errorf("--shard or --shards is required")
	}
	return ids, nil
}

func uniqueShardIDs(values []uint64) []uint64 {
	seen := make(map[uint64]bool)
	out := make([]uint64, 0, len(values))
	for _, value := range values {
		if seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func parseWizardFlags(name string, args []string) (wizardOptions, error) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	var opts wizardOptions
	fs.StringVar(&opts.mode, "mode", "", "repair mode: local or k8s")
	fs.StringVar(&opts.baseDir, "base", "", "local MatrixOne repair base directory")
	fs.StringVar(&opts.confDir, "conf-dir", "", "local MatrixOne config directory")
	fs.Uint64Var(&opts.shardID, "shard", 0, "log shard id to repair")
	fs.StringVar(&opts.shards, "shards", "", "comma-separated log shard ids to repair together")
	fs.StringVar(&opts.addresses, "addresses", "", "comma-separated HAKeeper service addresses")
	fs.StringVar(&opts.output, "output", "", "write generated plan to this file")
	fs.StringVar(&opts.planPath, "plan", "", "repair plan JSON file")
	fs.BoolVar(&opts.apply, "apply", false, "apply after planning")
	fs.BoolVar(&opts.yes, "yes", false, "skip interactive confirmations")
	fs.DurationVar(&opts.timeout, "timeout", 10*time.Second, "HAKeeper request timeout")
	fs.StringVar(&opts.moServicePath, "mo-service", "", "mo-service binary path used when restarting local stores")
	fs.StringVar(&opts.namespace, "namespace", "", "kubernetes namespace")
	fs.StringVar(&opts.kubeContext, "context", "", "kubernetes context")
	fs.StringVar(&opts.kubectlPath, "kubectl", "", "kubectl binary path")
	fs.StringVar(&opts.k8sLogSelector, "k8s-log-selector", "app.kubernetes.io/component=logservice", "kubernetes selector used to list LogService pods")
	fs.StringVar(&opts.pvcMountPath, "pvc-mount-path", "/repair-pvc", "mount path used by the temporary k8s repair pod")
	fs.StringVar(&opts.pvcDataDir, "pvc-logservice-data-dir", "", "logservice-data directory inside the temporary k8s repair pod")
	fs.StringVar(&opts.repairImage, "repair-image", "matrixorigin/matrixone:latest", "image used by the temporary k8s repair pod")
	fs.StringVar(&opts.repairBinary, "repair-binary", "/tmp/mo-logservice-repair", "mo-logservice-repair path inside the temporary repair pod")
	fs.Uint64Var(&opts.deploymentID, "deployment-id", 0, "dragonboat deployment id; required for exact k8s PVC clean-replica commands")
	fs.BoolVar(&opts.manualServiceControl, "manual-service-control", false, "do not stop or restart local mo-service processes; require the operator to do it")
	fs.BoolVar(&opts.autoRestartPods, "auto-restart-pods", false, "k8s mode only: let this CLI delete LogService pods during recovery")
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

func buildOnlineRecoveryPlans(opts wizardOptions, shardIDs []uint64) ([]*repairPlan, error) {
	if opts.mode == modeLocal {
		return buildLocalOnlineRecoveryPlans(opts, shardIDs)
	}
	plans := make([]*repairPlan, 0, len(shardIDs))
	for _, shardID := range shardIDs {
		plan, err := buildPlanForShard(opts, shardID)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	sortPlansByShard(plans)
	return plans, nil
}

func buildLocalOnlineRecoveryPlans(opts wizardOptions, shardIDs []uint64) ([]*repairPlan, error) {
	planned := make(map[uint64]*repairPlan)
	queued := make(map[uint64]bool)
	queue := append([]uint64(nil), shardIDs...)
	for _, shardID := range queue {
		queued[shardID] = true
	}
	for len(queue) > 0 {
		shardID := queue[0]
		queue = queue[1:]
		if planned[shardID] != nil {
			continue
		}
		plan, err := buildPlanForShard(opts, shardID)
		if err != nil {
			return nil, err
		}
		planned[shardID] = plan

		for _, store := range plan.Stores {
			if len(store.CleanupReplicas) == 0 || store.NodeHostDir == "" || store.DeploymentID == 0 {
				continue
			}
			for relatedShardID, replicas := range localReplicasByShard(store.NodeHostDir, store.DeploymentID) {
				if len(replicas) <= 1 || queued[relatedShardID] {
					continue
				}
				queued[relatedShardID] = true
				queue = append(queue, relatedShardID)
			}
		}
	}
	plans := make([]*repairPlan, 0, len(planned))
	for _, plan := range planned {
		plans = append(plans, plan)
	}
	sortPlansByShard(plans)
	if len(plans) > 1 {
		if err := validateCombinedRepairPlans(plans); err != nil {
			return nil, err
		}
		if err := validateCombinedPlannedLocalCleanupCompleteness(plans); err != nil {
			return nil, err
		}
		return plans, nil
	}
	if len(plans) == 1 {
		if err := validatePlannedLocalCleanupCompleteness(plans[0], storesWithAnyCleanup(plans[0].Stores)); err != nil {
			return nil, err
		}
	}
	return plans, nil
}

func buildPlanForShard(opts wizardOptions, shardID uint64) (*repairPlan, error) {
	next := opts
	next.shardID = shardID
	next.shardIDSet = true
	return buildRepairPlan(next)
}

func sortPlansByShard(plans []*repairPlan) {
	sort.Slice(plans, func(i, j int) bool {
		return plans[i].ShardID < plans[j].ShardID
	})
}

func buildK8sRepairPlan(opts wizardOptions) (*repairPlan, error) {
	if opts.namespace == "" {
		return nil, fmt.Errorf("--namespace is required for k8s mode")
	}
	kubectl := kubectlCommand(opts.kubectlPath, opts.kubeContext)
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
			"k8s recover writes HAKeeper repair state only; it does not back up PVCs",
			"k8s recover prints exact pod restart commands and waits for startup cleanup logs before unblock",
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
	repairShard := shard
	repairShard.Epoch = repairEpochAfterReportedState(state, opts.shardID, shard.Epoch)
	plan.TargetShard = toPlanShard(repairShard)
	targetByStore := replicaByStore(repairShard)
	sourceStore := targetByStore[repairShard.LeaderID]
	plan.SourceStore = sourceStore
	if sourceStore == "" {
		plan.Warnings = append(plan.Warnings, "HAKeeper has no leader for the target shard; source replica must be reviewed manually")
	}

	storeSet := targetStores(repairShard)
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
		store := buildK8sPlanStore(opts.shardID, repairShard, uuid, sourceStore, storeInfo, hasStoreInfo, opts)
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
	} else if role == "target" {
		for _, replicaID := range reportedReplicas {
			if replicaID != targetReplicaID {
				cleanupReplicas = append(cleanupReplicas, replicaID)
			}
		}
		if len(cleanupReplicas) > 0 {
			role = "cleanup"
		}
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

func repairEpochAfterReportedState(state logpb.CheckerState, shardID uint64, targetEpoch uint64) uint64 {
	maxEpoch := targetEpoch
	for _, store := range state.LogState.Stores {
		for _, replica := range store.Replicas {
			if replica.ShardID == shardID && replica.Epoch > maxEpoch {
				maxEpoch = replica.Epoch
			}
		}
	}
	return maxEpoch + 1
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
			Description: "Port-forward one running LogService/HAKeeper service, then rerun with --addresses 127.0.0.1:<local-port>.",
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
			Description: "Write repair state, block stale/dirty stores, and ask LogService to clean listed replicas on next startup.",
			Command:     fmt.Sprintf("mo-logservice-repair hakeeper repair --addresses %s --payload '%s'", strings.Join(plan.HAKeeperAddresses, ","), mustJSON(repairPayloadForPlanWithCleanup(plan, plan.InitialBlockedStores, "k8s online recover: block stale/dirty stores before restart", true))),
			ShardID:     plan.ShardID,
		})
	}
	for _, store := range plan.Stores {
		if len(store.CleanupReplicas) == 0 {
			continue
		}
		logSelector := "app.kubernetes.io/component=logservice"
		if k != nil && k.LogSelector != "" {
			logSelector = k.LogSelector
		}
		actions = append(actions, planAction{
			Type:        "k8s-restart-logservice-pod",
			Description: "Restart the LogService pod that owns this store UUID. The pod cleans requested local replicas during startup.",
			Store:       store.UUID,
			Command:     fmt.Sprintf("%s -n %s get pod -l %s -o wide --show-labels\n%s -n %s delete pod <logservice-pod-for-%s>", kubectl, namespace, shellQuote(logSelector), kubectl, namespace, store.UUID),
		})
		actions = append(actions, planAction{
			Type:        "hakeeper-unblock",
			Description: "Unblock the cleaned store after the restarted pod has heartbeated.",
			Store:       store.UUID,
			ShardID:     plan.ShardID,
			Command:     fmt.Sprintf("mo-logservice-repair hakeeper unblock --addresses %s --payload '{\"shardID\":%d,\"stores\":[\"%s\"],\"reason\":\"k8s online recover: restarted %s\"}'", strings.Join(plan.HAKeeperAddresses, ","), plan.ShardID, store.UUID, store.UUID),
		})
	}
	if len(plan.TargetShard.Replicas) > 0 {
		actions = append(actions, planAction{
			Type:        "hakeeper-repair-final",
			Description: "Refresh final repair state and keep only persistent stale stores blocked.",
			Command:     fmt.Sprintf("mo-logservice-repair hakeeper repair --addresses %s --payload '%s'", strings.Join(plan.HAKeeperAddresses, ","), mustJSON(repairPayloadForPlan(plan, plan.PersistentBlockedStores, "k8s online recover: repair complete"))),
			ShardID:     plan.ShardID,
		})
	}
	actions = append(actions, planAction{
		Type:        "verify",
		Description: "Verify final HAKeeper state after cleaned pods rejoin.",
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
	repairShard := shard
	repairShard.Epoch = repairEpochAfterReportedState(state, opts.shardID, shard.Epoch)
	plan := &repairPlan{
		Version:           repairPlanVersion,
		Mode:              modeLocal,
		CreatedAt:         time.Now().Format(time.RFC3339),
		BaseDir:           opts.baseDir,
		ConfDir:           opts.confDir,
		ShardID:           opts.shardID,
		HAKeeperAddresses: addresses,
		TargetShard:       toPlanShard(repairShard),
		ApplySupported:    true,
		Local: &localPlanSettings{
			MOServicePath: opts.moServicePath,
			BackupDir:     filepath.Join(opts.baseDir, "repair-backup", time.Now().Format("20060102-150405")),
			LogDir:        filepath.Join(opts.baseDir, "logs"),
		},
	}
	targetByStore := replicaByStore(repairShard)
	sourceStore := targetByStore[repairShard.LeaderID]
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
		store := buildPlanStore(opts.shardID, repairShard, cfg, sourceStore, storeInfo, hasStoreInfo)
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
	for store := range targetStores(repairShard) {
		if !seenStores[store] {
			plan.Warnings = append(plan.Warnings, fmt.Sprintf("target store %s has no local log config; it cannot be stopped or cleaned by local apply", store))
		}
	}
	for store, info := range state.LogState.Stores {
		if seenStores[store] || targetStores(repairShard)[store] {
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

func sameUint64s(a []uint64, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func hasUint64(values []uint64, target uint64) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
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
	byShard := localReplicasByShard(nodeHostDir, deploymentID)
	return byShard[shardID]
}

func localReplicasByShard(nodeHostDir string, deploymentID uint64) map[uint64][]uint64 {
	seen := make(map[uint64]map[uint64]bool)
	metadataPath := filepath.Join(nodeHostDir, logMetadataFilename)
	if md, err := readLogMetadata(metadataPath); err == nil {
		for _, rec := range md.Shards {
			if seen[rec.ShardID] == nil {
				seen[rec.ShardID] = make(map[uint64]bool)
			}
			seen[rec.ShardID][rec.ReplicaID] = true
		}
	}
	deploymentDir := fmt.Sprintf("%020d", deploymentID)
	matches, _ := filepath.Glob(filepath.Join(nodeHostDir, "*", deploymentDir, "tandb", "node-*-*"))
	for _, match := range matches {
		name := filepath.Base(match)
		parts := strings.Split(name, "-")
		if len(parts) != 3 || parts[0] != "node" {
			continue
		}
		shardID, shardErr := strconv.ParseUint(parts[1], 10, 64)
		replicaID, replicaErr := strconv.ParseUint(parts[2], 10, 64)
		if shardErr != nil || replicaErr != nil {
			continue
		}
		if seen[shardID] == nil {
			seen[shardID] = make(map[uint64]bool)
		}
		seen[shardID][replicaID] = true
	}
	ret := make(map[uint64][]uint64, len(seen))
	for shardID, replicas := range seen {
		ids := make([]uint64, 0, len(replicas))
		for id := range replicas {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		ret[shardID] = ids
	}
	return ret
}

func validateNoDuplicateLocalShards(stores []planStore) error {
	messages := make([]string, 0)
	for _, store := range stores {
		if len(store.CleanupReplicas) == 0 || store.NodeHostDir == "" || store.DeploymentID == 0 {
			continue
		}
		byShard := localReplicasByShard(store.NodeHostDir, store.DeploymentID)
		shardIDs := make([]uint64, 0, len(byShard))
		for shardID := range byShard {
			shardIDs = append(shardIDs, shardID)
		}
		sort.Slice(shardIDs, func(i, j int) bool { return shardIDs[i] < shardIDs[j] })
		for _, shardID := range shardIDs {
			replicas := byShard[shardID]
			if len(replicas) <= 1 {
				continue
			}
			messages = append(messages, duplicateLocalShardMessage(store, shardID, replicas))
		}
	}
	if len(messages) == 0 {
		return nil
	}
	return fmt.Errorf("local cleanup is incomplete; refusing to restart stores with duplicate local shard replicas:\n%s", strings.Join(messages, "\n"))
}

func validatePlannedLocalCleanupCompleteness(plan *repairPlan, stores []planStore) error {
	messages := make([]string, 0)
	for _, store := range stores {
		if len(store.CleanupReplicas) == 0 || store.NodeHostDir == "" || store.DeploymentID == 0 {
			continue
		}
		byShard := localReplicasByShard(store.NodeHostDir, store.DeploymentID)
		for _, replicaID := range store.CleanupReplicas {
			byShard[plan.ShardID] = removeUint64(byShard[plan.ShardID], replicaID)
		}
		shardIDs := make([]uint64, 0, len(byShard))
		for shardID := range byShard {
			shardIDs = append(shardIDs, shardID)
		}
		sort.Slice(shardIDs, func(i, j int) bool { return shardIDs[i] < shardIDs[j] })
		for _, shardID := range shardIDs {
			replicas := byShard[shardID]
			if len(replicas) <= 1 {
				continue
			}
			messages = append(messages, fmt.Sprintf("- store %s would still have duplicate local replicas for shard %d after this plan: %v", store.UUID, shardID, replicas))
		}
	}
	if len(messages) == 0 {
		return nil
	}
	return fmt.Errorf("repair plan is incomplete; refusing to start apply before mutating HAKeeper or local data:\n%s\nGenerate and review a plan for the listed shard(s), or clean the confirmed stale replicas manually before applying this plan", strings.Join(messages, "\n"))
}

func removeUint64(values []uint64, target uint64) []uint64 {
	out := values[:0]
	for _, value := range values {
		if value != target {
			out = append(out, value)
		}
	}
	return out
}

func duplicateLocalShardMessage(store planStore, shardID uint64, replicas []uint64) string {
	commands := make([]string, 0, len(replicas))
	for _, replicaID := range replicas {
		commands = append(commands, fmt.Sprintf(
			"mo-logservice-repair local clean-replica --deployment-id %d --node-host-id %s --node-host-dir %s --raft-address %s --listen-address %s --gossip-address %s --shard-id %d --replica-id %d",
			store.DeploymentID,
			shellQuote(store.UUID),
			shellQuote(store.NodeHostDir),
			shellQuote(store.RaftAddress),
			shellQuote(store.ListenAddress),
			shellQuote(store.GossipAddress),
			shardID,
			replicaID,
		))
	}
	return fmt.Sprintf("- store %s shard %d still has local replicas %v; inspect HAKeeper target membership and clean only confirmed stale replicas before restart. Candidate commands, do not run all of them blindly:\n  %s", store.UUID, shardID, replicas, strings.Join(commands, "\n  "))
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

func applyOnlineRecoveryPlans(ctx context.Context, plans []*repairPlan, opts wizardOptions) error {
	if len(plans) == 0 {
		return fmt.Errorf("no repair plans to apply")
	}
	timeout := opts.timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	tasks := cleanupTasksForPlansAllStores(plans)
	restartStores := storesFromCleanupTasks(tasks)

	stdoutln("step 1: write HAKeeper repair state with requested cleanup replicas")
	beforeTicks, err := currentStoreTicks(ctx, plans[0], timeout, restartStores)
	if err != nil {
		return err
	}
	for _, plan := range plans {
		req := repairPayloadForPlanWithCleanup(plan, plan.InitialBlockedStores, "online recover: block stale/dirty stores before restart", true)
		if _, err := applyHAKeeperWithNewConnection(ctx, stableHAKeeperAddressesForApply(plan), timeout, req); err != nil {
			return fmt.Errorf("write repair state for shard %d: %w", plan.ShardID, err)
		}
	}

	if len(restartStores) > 0 {
		if plans[0].Mode == modeLocal {
			stdoutln("step 2: stop listed LogService stores outside this CLI")
			printLocalStopInstructions(plans, restartStores)
			if !opts.yes {
				line, err := askLine("After all listed LogService processes are stopped, type STOPPED")
				if err != nil {
					return err
				}
				if line != "STOPPED" {
					return fmt.Errorf("recover cancelled before local cleanup")
				}
			}
			if err := verifyLocalStoresStopped(restartStores); err != nil {
				return err
			}
			stdoutln("step 3: clean confirmed local replicas")
			stdoutln(combinedCleanupSummary(tasks))
			if !opts.yes {
				line, err := askLine("Type CLEAN to delete the listed local replica data")
				if err != nil {
					return err
				}
				if line != "CLEAN" {
					return fmt.Errorf("recover cancelled before local cleanup")
				}
			}
			for _, task := range tasks {
				if err := cleanReplica(
					task.Store.DeploymentID,
					task.Store.UUID,
					task.Store.NodeHostDir,
					task.Store.RaftAddress,
					task.Store.ListenAddress,
					task.Store.GossipAddress,
					task.Plan.ShardID,
					task.ReplicaID,
					200,
				); err != nil {
					return fmt.Errorf("clean shard %d store %s replica %d: %w",
						task.Plan.ShardID, task.Store.UUID, task.ReplicaID, err)
				}
			}
			if err := verifyOnlineCleanupComplete(plans); err != nil {
				return err
			}
			stdoutln("step 4: clear startup cleanup requests and keep stores blocked")
			for _, plan := range plans {
				req := repairPayloadForPlanWithCleanup(plan, plan.InitialBlockedStores, "online recover: local cleanup complete; keep stores blocked before restart", false)
				if _, err := applyHAKeeperWithNewConnection(ctx, stableHAKeeperAddressesForApply(plan), timeout, req); err != nil {
					stdoutf("warning: failed to clear startup cleanup request for shard %d before restart: %v\n", plan.ShardID, err)
					stdoutln("warning: continue after local cleanup; restart LogService so HAKeeper becomes available, then final repair state will be refreshed")
				}
			}
			stdoutln("step 5: start listed LogService stores outside this CLI")
			printRestartInstructions(plans, restartStores)
		} else {
			if opts.manualServiceControl || !opts.autoRestartPods {
				stdoutln("step 2: restart listed LogService stores outside this CLI")
				if plans[0].Mode == modeK8s {
					if err := printK8sRestartInstructionsWithCurrentPods(ctx, plans, restartStores); err != nil {
						return err
					}
				} else {
					printRestartInstructions(plans, restartStores)
				}
			} else {
				stdoutln("step 2: restart listed LogService pods one by one")
				if err := restartK8sStoresAndVerifyCleanup(ctx, plans, restartStores, timeout); err != nil {
					return err
				}
			}
		}
		autoRestartedK8s := plans[0].Mode == modeK8s && opts.autoRestartPods && !opts.manualServiceControl
		if plans[0].Mode == modeK8s && !autoRestartedK8s && opts.yes {
			return fmt.Errorf("manual pod restart is required before unblock; rerun without --yes, or pass --auto-restart-pods to let the CLI delete pods")
		}
		if !autoRestartedK8s && !opts.yes {
			line, err := askLine("After all listed LogService pods/processes have restarted, type DONE")
			if err != nil {
				return err
			}
			if line != "DONE" {
				return fmt.Errorf("recover cancelled before unblock")
			}
		}
		stdoutln("step 6: wait for restarted stores to heartbeat")
		if err := waitForStoreHeartbeats(ctx, plans[0], timeout, restartStores, beforeTicks, opts.yes); err != nil {
			return err
		}
		if err := verifyOnlineCleanupComplete(plans); err != nil {
			return err
		}
		if plans[0].Mode == modeK8s && !autoRestartedK8s {
			if err := verifyK8sCleanupLogsAfterManualRestart(ctx, plans, restartStores, timeout); err != nil {
				return err
			}
		}
	} else {
		stdoutln("step 2: no LogService restart is required by this plan")
	}

	stdoutln("step 4: unblock cleaned stores")
	for _, plan := range plans {
		for _, store := range storesWithAnyCleanup(plan.Stores) {
			req := repairPayload{
				Op:         "unblock",
				ShardID:    plan.ShardID,
				ShardIDSet: true,
				Stores:     []string{store.UUID},
				Reason:     fmt.Sprintf("online recover: store %s restarted for shard %d", store.UUID, plan.ShardID),
			}
			if _, err := applyHAKeeperWithNewConnection(ctx, stableHAKeeperAddressesForApply(plan), timeout, req); err != nil {
				return fmt.Errorf("unblock shard %d store %s: %w", plan.ShardID, store.UUID, err)
			}
			stdoutf("unblocked store %s for shard %d\n", store.UUID, plan.ShardID)
		}
	}

	stdoutln("step 5: refresh final repair state")
	for _, plan := range plans {
		if len(plan.PersistentBlockedStores) == 0 {
			req := repairPayload{
				Op:         "unblock",
				ShardID:    plan.ShardID,
				ShardIDSet: true,
				Reason:     fmt.Sprintf("online recover: clear repair state for shard %d", plan.ShardID),
			}
			if _, err := applyHAKeeperWithNewConnection(ctx, stableHAKeeperAddressesForApply(plan), timeout, req); err != nil {
				return fmt.Errorf("clear repair state for shard %d: %w", plan.ShardID, err)
			}
			continue
		}
		req := repairPayloadForPlanWithCleanup(plan, plan.PersistentBlockedStores, "online recover: repair complete; stale stores remain blocked", false)
		if _, err := applyHAKeeperWithNewConnection(ctx, stableHAKeeperAddressesForApply(plan), timeout, req); err != nil {
			return fmt.Errorf("final repair state for shard %d: %w", plan.ShardID, err)
		}
	}

	stdoutln("step 6: final HAKeeper state")
	state, err := getHAKeeperStateWithNewConnection(ctx, stableHAKeeperAddressesForApply(plans[0]), timeout)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{
		"logShards": state.LogState.Shards,
		"repairs":   state.LogShardRepairs,
	})
}

func verifyLocalStoresStopped(stores []planStore) error {
	messages := make([]string, 0)
	for _, store := range stores {
		if store.ConfigPath == "" {
			continue
		}
		pids, _ := findMOServiceProcesses(store.ConfigPath)
		if len(pids) == 0 {
			continue
		}
		messages = append(messages, fmt.Sprintf(
			"- store %s still has running mo-service processes for %s: pids=%v",
			store.UUID,
			store.ConfigPath,
			pids,
		))
	}
	if len(messages) == 0 {
		return nil
	}
	return fmt.Errorf(
		"LogService processes are still running; refusing to clean local replica data:\n%s\nStop the listed processes, then rerun recover/apply",
		strings.Join(messages, "\n"),
	)
}

func verifyOnlineCleanupComplete(plans []*repairPlan) error {
	messages := make([]string, 0)
	for _, plan := range plans {
		if plan.Mode != modeLocal {
			continue
		}
		for _, store := range storesWithAnyCleanup(plan.Stores) {
			if store.NodeHostDir == "" || store.DeploymentID == 0 {
				continue
			}
			current := localShardReplicas(store.NodeHostDir, store.DeploymentID, plan.ShardID)
			for _, replicaID := range store.CleanupReplicas {
				if !hasUint64(current, replicaID) {
					continue
				}
				messages = append(messages, fmt.Sprintf(
					"- shard %d store %s replica %d still exists under %s",
					plan.ShardID,
					store.UUID,
					replicaID,
					store.NodeHostDir,
				))
			}
		}
	}
	if len(messages) == 0 {
		return nil
	}
	return fmt.Errorf(
		"local cleanup did not finish; refusing to unblock cleaned stores:\n%s\nRestart the listed LogService stores again, or clean the listed replicas manually, then rerun recover/apply",
		strings.Join(messages, "\n"),
	)
}

type k8sPodList struct {
	Items []k8sPod `json:"items"`
}

type k8sPod struct {
	Metadata k8sPodMetadata `json:"metadata"`
	Status   k8sPodStatus   `json:"status"`
}

type k8sPodMetadata struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels"`
}

type k8sPodStatus struct {
	Phase      string            `json:"phase"`
	Conditions []k8sPodCondition `json:"conditions"`
}

type k8sPodCondition struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

func restartK8sStoresAndVerifyCleanup(ctx context.Context, plans []*repairPlan, stores []planStore, timeout time.Duration) error {
	if len(plans) == 0 {
		return nil
	}
	for _, store := range stores {
		pod, err := findK8sPodForStore(ctx, plans[0], store, "")
		if err != nil {
			return err
		}
		stdoutf("restart store %s: delete pod %s\n", store.UUID, pod.Metadata.Name)
		if _, err := runKubectl(ctx, plans[0], "delete", "pod", pod.Metadata.Name); err != nil {
			return fmt.Errorf("delete pod %s for store %s: %w", pod.Metadata.Name, store.UUID, err)
		}
		readyPod, err := waitForK8sStorePodReady(ctx, plans[0], store, pod.Metadata.Name, timeout)
		if err != nil {
			return err
		}
		stdoutf("store %s ready in pod %s\n", store.UUID, readyPod.Metadata.Name)
		if err := waitForK8sCleanupLogs(ctx, plans, store.UUID, readyPod.Metadata.Name, timeout); err != nil {
			return err
		}
	}
	return nil
}

func printK8sRestartInstructionsWithCurrentPods(ctx context.Context, plans []*repairPlan, stores []planStore) error {
	if len(plans) == 0 {
		return nil
	}
	stdoutln("Run these pod restart commands in another shell, one store at a time:")
	for _, store := range stores {
		pod, err := findK8sPodForStore(ctx, plans[0], store, "")
		if err != nil {
			return err
		}
		kubectl := "kubectl"
		namespace := plans[0].Namespace
		if plans[0].K8s != nil && plans[0].K8s.Kubectl != "" {
			kubectl = plans[0].K8s.Kubectl
		}
		stdoutf("- store %s cleanup=%s\n", store.UUID, cleanupReplicasForStore(plans, store.UUID))
		stdoutf("  %s -n %s delete pod %s\n", kubectl, shellQuote(namespace), shellQuote(pod.Metadata.Name))
		stdoutf("  %s -n %s wait --for=condition=Ready pod -l app=%s --timeout=120s\n",
			kubectl,
			shellQuote(namespace),
			shellQuote(appLabelForPod(pod)),
		)
	}
	return nil
}

func verifyK8sCleanupLogsAfterManualRestart(ctx context.Context, plans []*repairPlan, stores []planStore, timeout time.Duration) error {
	if len(plans) == 0 {
		return nil
	}
	stdoutln("step 6: verify startup cleanup logs")
	for _, store := range stores {
		pod, err := waitForK8sStorePodReady(ctx, plans[0], store, "", timeout)
		if err != nil {
			return err
		}
		if err := waitForK8sCleanupLogs(ctx, plans, store.UUID, pod.Metadata.Name, timeout); err != nil {
			return err
		}
	}
	return nil
}

func waitForK8sStorePodReady(ctx context.Context, plan *repairPlan, store planStore, oldPodName string, timeout time.Duration) (k8sPod, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		pod, err := findK8sPodForStore(ctx, plan, store, oldPodName)
		if err == nil && isK8sPodReady(pod) {
			return pod, nil
		}
		if err != nil {
			lastErr = err
		}
		time.Sleep(time.Second)
	}
	pod, err := findK8sPodForStore(ctx, plan, store, "")
	if err == nil && isK8sPodReady(pod) {
		return pod, nil
	}
	if lastErr != nil {
		return k8sPod{}, fmt.Errorf("wait for store %s pod ready: %w", store.UUID, lastErr)
	}
	return k8sPod{}, fmt.Errorf("timeout waiting for store %s pod to become Ready", store.UUID)
}

func waitForK8sCleanupLogs(ctx context.Context, plans []*repairPlan, storeUUID string, podName string, timeout time.Duration) error {
	replicas := cleanupReplicasForStoreIDs(plans, storeUUID)
	if len(replicas) == 0 {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := runKubectl(ctx, plans[0], "logs", podName, "--since=5m", "--tail=300")
		if err == nil && k8sCleanupLogsContainReplicas(out, replicas) {
			stdoutf("store %s cleanup confirmed in pod %s\n", storeUUID, podName)
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf(
		"timeout waiting for cleanup logs in pod %s for store %s replicas %v; refusing to unblock this store",
		podName,
		storeUUID,
		replicas,
	)
}

func k8sCleanupLogsContainReplicas(logs string, replicas []uint64) bool {
	for _, replicaID := range replicas {
		if !strings.Contains(logs, "clean local log replica requested by HAKeeper repair state") ||
			!strings.Contains(logs, strconv.FormatUint(replicaID, 10)) {
			return false
		}
	}
	return true
}

func cleanupReplicasForStoreIDs(plans []*repairPlan, storeUUID string) []uint64 {
	replicas := make([]uint64, 0)
	for _, plan := range plans {
		for _, store := range plan.Stores {
			if store.UUID == storeUUID {
				replicas = append(replicas, store.CleanupReplicas...)
			}
		}
	}
	return uniqueUint64s(replicas)
}

func findK8sPodForStore(ctx context.Context, plan *repairPlan, store planStore, skipPodName string) (k8sPod, error) {
	selectors := k8sSelectorsForStore(plan, store)
	seen := make(map[string]bool)
	for _, selector := range selectors {
		if seen[selector] {
			continue
		}
		seen[selector] = true
		pods, err := listK8sPods(ctx, plan, selector)
		if err != nil {
			return k8sPod{}, err
		}
		for _, pod := range pods {
			if pod.Metadata.Name == skipPodName {
				continue
			}
			if k8sPodMatchesStore(pod, store) {
				return pod, nil
			}
		}
	}
	return k8sPod{}, fmt.Errorf("cannot find LogService pod for store %s", store.UUID)
}

func k8sSelectorsForStore(plan *repairPlan, store planStore) []string {
	ret := make([]string, 0, 3)
	if ordinal, ok := logStoreOrdinal(store.UUID); ok {
		ret = append(ret, fmt.Sprintf("app=log-%d", ordinal))
	}
	if plan.K8s != nil && plan.K8s.LogSelector != "" {
		ret = append(ret, plan.K8s.LogSelector)
	}
	ret = append(ret, "")
	return ret
}

func listK8sPods(ctx context.Context, plan *repairPlan, selector string) ([]k8sPod, error) {
	args := []string{"get", "pod", "-o", "json"}
	if selector != "" {
		args = append(args, "-l", selector)
	}
	out, err := runKubectl(ctx, plan, args...)
	if err != nil {
		return nil, err
	}
	var pods k8sPodList
	if err := json.Unmarshal([]byte(out), &pods); err != nil {
		return nil, fmt.Errorf("parse kubectl pod JSON: %w", err)
	}
	return pods.Items, nil
}

func k8sPodMatchesStore(pod k8sPod, store planStore) bool {
	candidates := []string{pod.Metadata.Name}
	for key, value := range pod.Metadata.Labels {
		candidates = append(candidates, key, value)
	}
	if ordinal, ok := logStoreOrdinal(store.UUID); ok {
		needle := fmt.Sprintf("log-%d", ordinal)
		for _, candidate := range candidates {
			if strings.Contains(candidate, needle) {
				return true
			}
		}
	}
	short := shortStoreID(store.UUID)
	for _, candidate := range candidates {
		if strings.Contains(candidate, store.UUID) || strings.Contains(candidate, short) {
			return true
		}
	}
	return false
}

func isK8sPodReady(pod k8sPod) bool {
	if pod.Status.Phase != "Running" {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == "Ready" && condition.Status == "True" {
			return true
		}
	}
	return false
}

func appLabelForPod(pod k8sPod) string {
	if value := pod.Metadata.Labels["app"]; value != "" {
		return value
	}
	return pod.Metadata.Name
}

func logStoreOrdinal(uuid string) (int, bool) {
	short := shortStoreID(uuid)
	idx := strings.LastIndex(short, "d")
	if idx < 0 || idx == len(short)-1 {
		return 0, false
	}
	value, err := strconv.Atoi(short[idx+1:])
	if err != nil {
		return 0, false
	}
	return value, true
}

func runKubectl(ctx context.Context, plan *repairPlan, args ...string) (string, error) {
	kubectl := "kubectl"
	namespace := ""
	if plan != nil {
		namespace = plan.Namespace
		if plan.K8s != nil && plan.K8s.Kubectl != "" {
			kubectl = plan.K8s.Kubectl
		}
	}
	fullArgs := make([]string, 0, len(args)+2)
	if namespace != "" {
		fullArgs = append(fullArgs, "-n", namespace)
	}
	fullArgs = append(fullArgs, args...)
	cmd := exec.CommandContext(ctx, "sh", "-c", kubectl+" "+shellJoin(fullArgs))
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%s %s: %w\n%s", kubectl, strings.Join(fullArgs, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func shellJoin(args []string) string {
	parts := make([]string, 0, len(args))
	for _, arg := range args {
		parts = append(parts, shellQuote(arg))
	}
	return strings.Join(parts, " ")
}

func cleanupTasksForPlansAllStores(plans []*repairPlan) []cleanupTask {
	tasks := make([]cleanupTask, 0)
	for _, plan := range plans {
		for _, store := range plan.Stores {
			for _, replicaID := range store.CleanupReplicas {
				tasks = append(tasks, cleanupTask{
					Plan:      plan,
					Store:     store,
					ReplicaID: replicaID,
				})
			}
		}
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].Store.UUID != tasks[j].Store.UUID {
			return tasks[i].Store.UUID < tasks[j].Store.UUID
		}
		if tasks[i].Plan.ShardID != tasks[j].Plan.ShardID {
			return tasks[i].Plan.ShardID < tasks[j].Plan.ShardID
		}
		return tasks[i].ReplicaID < tasks[j].ReplicaID
	})
	return tasks
}

func currentStoreTicks(ctx context.Context, plan *repairPlan, timeout time.Duration, stores []planStore) (map[string]uint64, error) {
	state, err := getHAKeeperStateWithNewConnection(ctx, stableHAKeeperAddressesForApply(plan), timeout)
	if err != nil {
		return nil, err
	}
	ticks := make(map[string]uint64, len(stores))
	for _, store := range stores {
		ticks[store.UUID] = state.LogState.Stores[store.UUID].Tick
	}
	return ticks, nil
}

func waitForStoreHeartbeats(
	ctx context.Context,
	plan *repairPlan,
	timeout time.Duration,
	stores []planStore,
	beforeTicks map[string]uint64,
	assumeYes bool,
) error {
	deadline := time.Now().Add(2 * time.Minute)
	pending := make(map[string]planStore)
	for _, store := range stores {
		pending[store.UUID] = store
	}
	for len(pending) > 0 && time.Now().Before(deadline) {
		state, err := getHAKeeperStateWithNewConnection(ctx, stableHAKeeperAddressesForApply(plan), timeout)
		if err != nil {
			return err
		}
		for uuid := range pending {
			info, ok := state.LogState.Stores[uuid]
			if ok && info.Tick > beforeTicks[uuid] {
				delete(pending, uuid)
			}
		}
		if len(pending) == 0 {
			return nil
		}
		time.Sleep(5 * time.Second)
	}
	if len(pending) == 0 {
		return nil
	}
	pendingStores := make([]string, 0, len(pending))
	for uuid := range pending {
		pendingStores = append(pendingStores, uuid)
	}
	sort.Strings(pendingStores)
	msg := fmt.Sprintf("stores did not heartbeat after restart: %v", pendingStores)
	if assumeYes {
		return fmt.Errorf("%s", msg)
	}
	stdoutln("warning: " + msg)
	ok, err := askYesNo("Continue to unblock anyway", false)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("recover stopped before unblock: %s", msg)
	}
	return nil
}

func printRestartInstructions(plans []*repairPlan, stores []planStore) {
	mode := plans[0].Mode
	stdoutln("Start or restart these LogService stores:")
	for _, store := range stores {
		shards := cleanupShardsForStore(plans, store.UUID)
		stdoutf("- store %s shards=%v cleanup=%s\n", store.UUID, shards, cleanupReplicasForStore(plans, store.UUID))
		switch mode {
		case modeLocal:
			if store.ConfigPath != "" {
				stdoutf("  config: %s\n", store.ConfigPath)
				stdoutf("  example: pgrep -af -- 'mo-service -cfg %s'\n", store.ConfigPath)
				stdoutf("  example: kill -TERM <pid>; nohup %s -cfg %s >> %s 2>&1 &\n",
					shellQuote(firstNonEmpty(store.MOServicePath, "mo-service")),
					shellQuote(store.ConfigPath),
					shellQuote(localRestartLogPath(plans[0], store)),
				)
			}
		case modeK8s:
			kubectl := "kubectl"
			namespace := plans[0].Namespace
			selector := ""
			if plans[0].K8s != nil {
				kubectl = firstNonEmpty(plans[0].K8s.Kubectl, kubectl)
				selector = plans[0].K8s.LogSelector
			}
			if selector != "" {
				stdoutf("  find pod: %s -n %s get pod -l %s -o wide --show-labels\n", kubectl, shellQuote(namespace), shellQuote(selector))
			}
			stdoutf("  restart pod: %s -n %s delete pod <logservice-pod-for-%s>\n", kubectl, shellQuote(namespace), store.UUID)
		}
	}
}

func printLocalStopInstructions(plans []*repairPlan, stores []planStore) {
	stdoutln("Stop these LogService processes before local cleanup:")
	for _, store := range stores {
		shards := cleanupShardsForStore(plans, store.UUID)
		stdoutf("- store %s shards=%v cleanup=%s\n", store.UUID, shards, cleanupReplicasForStore(plans, store.UUID))
		if store.ConfigPath == "" {
			continue
		}
		stdoutf("  config: %s\n", store.ConfigPath)
		stdoutf("  find process: pgrep -af -- 'mo-service -cfg %s'\n", store.ConfigPath)
		stdoutln("  stop process: kill -TERM <pid>")
	}
}

func cleanupShardsForStore(plans []*repairPlan, storeUUID string) []uint64 {
	shards := make([]uint64, 0)
	for _, plan := range plans {
		for _, store := range plan.Stores {
			if store.UUID == storeUUID && len(store.CleanupReplicas) > 0 {
				shards = append(shards, plan.ShardID)
			}
		}
	}
	return uniqueShardIDs(shards)
}

func cleanupReplicasForStore(plans []*repairPlan, storeUUID string) string {
	parts := make([]string, 0)
	for _, plan := range plans {
		for _, store := range plan.Stores {
			if store.UUID != storeUUID || len(store.CleanupReplicas) == 0 {
				continue
			}
			parts = append(parts, fmt.Sprintf("shard %d replicas %v", plan.ShardID, store.CleanupReplicas))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "; ")
}

func localRestartLogPath(plan *repairPlan, store planStore) string {
	logDir := "."
	if plan.Local != nil {
		logDir = firstNonEmpty(plan.Local.LogDir, logDir)
	}
	name := "logservice-" + shortStoreID(store.UUID)
	if store.ConfigPath != "" {
		name = filepath.Base(strings.TrimSuffix(store.ConfigPath, ".toml"))
	}
	return filepath.Join(logDir, name+"-manual-restart.out")
}

func applyRepairPlan(ctx context.Context, plan *repairPlan, opts wizardOptions) error {
	return applyRepairPlans(ctx, []*repairPlan{plan}, opts)
}

func applyRepairPlans(ctx context.Context, plans []*repairPlan, opts wizardOptions) error {
	if len(plans) == 0 {
		return fmt.Errorf("no repair plans to apply")
	}
	if len(plans) == 1 {
		return applySingleRepairPlan(ctx, plans[0], opts)
	}
	if err := validateCombinedRepairPlans(plans); err != nil {
		return err
	}
	timeout := opts.timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	tasks := cleanupTasksForPlans(plans)
	rebuildStores := storesFromCleanupTasks(tasks)
	if err := validateCombinedPlannedLocalCleanupCompleteness(plans); err != nil {
		return err
	}

	stdoutln("step 1: write repair states and block stale/dirty stores")
	if err := confirmApplyStep(opts, "step 1", fmt.Sprintf("block stores for shards %v", planShardIDs(plans))); err != nil {
		return err
	}
	for _, plan := range plans {
		if _, err := applyHAKeeperWithNewConnection(ctx, stableHAKeeperAddressesForApply(plan), timeout, repairPayloadForPlan(plan, plan.InitialBlockedStores, "wizard: block stale/dirty stores before combined cleanup")); err != nil {
			return fmt.Errorf("shard %d repair state: %w", plan.ShardID, err)
		}
	}

	stdoutln("step 2: stop LogService stores that need local cleanup")
	if err := confirmApplyStep(opts, "step 2", fmt.Sprintf("stop stores %v", storesWithCleanup(rebuildStores))); err != nil {
		return err
	}
	if opts.manualServiceControl {
		stdoutf("manual service control enabled; stop these stores before continuing: %v\n", storesWithCleanup(rebuildStores))
	} else {
		for _, store := range rebuildStores {
			if err := stopStore(store); err != nil {
				return err
			}
		}
	}

	stdoutln("step 3: back up local logservice-data")
	backupDir := combinedBackupDir(plans)
	if err := confirmApplyStep(opts, "step 3", fmt.Sprintf("backup store data to %s", backupDir)); err != nil {
		return err
	}
	for _, store := range rebuildStores {
		if err := backupStore(backupDir, store); err != nil {
			return err
		}
	}

	stdoutln("step 4: clean dirty local replicas")
	if err := confirmApplyStep(opts, "step 4", combinedCleanupSummary(tasks)); err != nil {
		return err
	}
	for _, task := range tasks {
		if err := cleanReplica(
			task.Store.DeploymentID,
			task.Store.UUID,
			task.Store.NodeHostDir,
			task.Store.RaftAddress,
			task.Store.ListenAddress,
			task.Store.GossipAddress,
			task.Plan.ShardID,
			task.ReplicaID,
			200,
		); err != nil {
			return err
		}
	}
	if err := validateNoDuplicateLocalShards(rebuildStores); err != nil {
		return err
	}

	stdoutln("step 5: restart cleaned LogService stores")
	if err := confirmApplyStep(opts, "step 5", fmt.Sprintf("restart stores %v", storesWithCleanup(rebuildStores))); err != nil {
		return err
	}
	if opts.manualServiceControl {
		stdoutf("manual service control enabled; start these stores and wait until their HAKeeper service addresses are reachable: %v\n", storesWithCleanup(rebuildStores))
	} else {
		for _, store := range rebuildStores {
			if err := startStore(plans[0], store); err != nil {
				return err
			}
		}
	}

	stdoutln("step 6: unblock cleaned stores one shard at a time")
	for _, plan := range plans {
		for _, store := range storesByUUID(plan.Stores, plan.RebuildStores) {
			if len(store.CleanupReplicas) == 0 {
				continue
			}
			if err := confirmApplyStep(opts, "step 6", fmt.Sprintf("unblock cleaned store %s for shard %d", store.UUID, plan.ShardID)); err != nil {
				return err
			}
			req := repairPayload{
				Op:         "unblock",
				ShardID:    plan.ShardID,
				ShardIDSet: true,
				Stores:     []string{store.UUID},
				Reason:     fmt.Sprintf("wizard: combined cleanup shard %d store %s", plan.ShardID, store.UUID),
			}
			if _, err := applyHAKeeperWithNewConnection(ctx, stableHAKeeperAddressesForApply(plan), timeout, req); err != nil {
				return fmt.Errorf("unblock shard %d store %s: %w", plan.ShardID, store.UUID, err)
			}
			stdoutf("unblocked %s for shard %d; wait 30s for L/Start/snapshot restore\n", store.UUID, plan.ShardID)
			time.Sleep(30 * time.Second)
		}
	}

	stdoutln("step 7: write final repair states")
	for _, plan := range plans {
		if err := confirmApplyStep(opts, "step 7", fmt.Sprintf("shard %d keep persistent blocked stores %v", plan.ShardID, plan.PersistentBlockedStores)); err != nil {
			return err
		}
		if _, err := applyHAKeeperWithNewConnection(ctx, stableHAKeeperAddressesForApply(plan), timeout, repairPayloadForPlan(plan, plan.PersistentBlockedStores, "wizard: combined repair complete")); err != nil {
			return fmt.Errorf("final repair state shard %d: %w", plan.ShardID, err)
		}
	}

	stdoutln("step 8: final HAKeeper state")
	state, err := getHAKeeperStateWithNewConnection(ctx, stableHAKeeperAddressesForApply(plans[0]), timeout)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{
		"logShards": state.LogState.Shards,
		"repairs":   state.LogShardRepairs,
	})
}

func applySingleRepairPlan(ctx context.Context, plan *repairPlan, opts wizardOptions) error {
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
	if err := validatePlannedLocalCleanupCompleteness(plan, storesByUUID(plan.Stores, plan.RebuildStores)); err != nil {
		return err
	}
	stdoutln("step 1: write repair state and block stale/dirty stores")
	if err := confirmApplyStep(opts, "step 1", fmt.Sprintf("block stores %v in HAKeeper repair state", plan.InitialBlockedStores)); err != nil {
		return err
	}
	if _, err := applyHAKeeperWithNewConnection(ctx, stableHAKeeperAddresses, timeout, repairPayloadForPlan(plan, plan.InitialBlockedStores, "wizard: block stale/dirty stores before cleanup")); err != nil {
		return err
	}
	rebuildStores := storesByUUID(plan.Stores, plan.RebuildStores)
	stdoutln("step 2: stop LogService stores that need local cleanup")
	if err := confirmApplyStep(opts, "step 2", fmt.Sprintf("stop stores %v", storesWithCleanup(rebuildStores))); err != nil {
		return err
	}
	if opts.manualServiceControl {
		stdoutf("manual service control enabled; stop these stores before continuing: %v\n", storesWithCleanup(rebuildStores))
	} else {
		for _, store := range rebuildStores {
			if len(store.CleanupReplicas) == 0 {
				continue
			}
			if err := stopStore(store); err != nil {
				return err
			}
		}
	}
	stdoutln("step 3: back up local logservice-data")
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
	stdoutln("step 4: clean dirty local replicas")
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
	if err := validateNoDuplicateLocalShards(rebuildStores); err != nil {
		return err
	}
	stdoutln("step 5: restart cleaned LogService stores")
	if err := confirmApplyStep(opts, "step 5", fmt.Sprintf("restart stores %v", storesWithCleanup(rebuildStores))); err != nil {
		return err
	}
	if opts.manualServiceControl {
		stdoutf("manual service control enabled; start these stores and wait until their HAKeeper service addresses are reachable: %v\n", storesWithCleanup(rebuildStores))
	} else {
		for _, store := range rebuildStores {
			if len(store.CleanupReplicas) == 0 {
				continue
			}
			if err := startStore(plan, store); err != nil {
				return err
			}
		}
	}
	stdoutln("step 6: unblock cleaned stores one by one")
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
		stdoutf("unblocked %s; wait 30s for L/Start/snapshot restore\n", store.UUID)
		time.Sleep(30 * time.Second)
	}
	stdoutln("step 7: write final repair state")
	if err := confirmApplyStep(opts, "step 7", fmt.Sprintf("keep persistent blocked stores %v", plan.PersistentBlockedStores)); err != nil {
		return err
	}
	if _, err := applyHAKeeperWithNewConnection(ctx, stableHAKeeperAddresses, timeout, repairPayloadForPlan(plan, plan.PersistentBlockedStores, "wizard: repair complete")); err != nil {
		return err
	}
	stdoutln("step 8: final HAKeeper state")
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

type cleanupTask struct {
	Plan      *repairPlan
	Store     planStore
	ReplicaID uint64
}

func validateCombinedRepairPlans(plans []*repairPlan) error {
	seenShards := make(map[uint64]bool)
	for _, plan := range plans {
		if !plan.ApplySupported {
			return fmt.Errorf("%s mode apply is not supported yet; use the generated actions as a runbook", plan.Mode)
		}
		if plan.Mode != modeLocal {
			return fmt.Errorf("unsupported apply mode %q", plan.Mode)
		}
		if seenShards[plan.ShardID] {
			return fmt.Errorf("duplicate repair plan for shard %d", plan.ShardID)
		}
		seenShards[plan.ShardID] = true
		if plan.Local == nil {
			return fmt.Errorf("shard %d has no local plan settings", plan.ShardID)
		}
	}
	return nil
}

func validateLocalPlanFreshness(plans []*repairPlan) error {
	messages := make([]string, 0)
	for _, plan := range plans {
		if plan.Mode != modeLocal {
			continue
		}
		for _, store := range plan.Stores {
			if store.NodeHostDir == "" || store.DeploymentID == 0 {
				continue
			}
			current := localShardReplicas(store.NodeHostDir, store.DeploymentID, plan.ShardID)
			if sameUint64s(current, store.LocalReplicas) {
				continue
			}
			messages = append(messages, fmt.Sprintf(
				"- shard %d store %s local replicas changed: plan has %v, current disk has %v",
				plan.ShardID,
				store.UUID,
				store.LocalReplicas,
				current,
			))
		}
	}
	if len(messages) == 0 {
		return nil
	}
	return fmt.Errorf("repair plan is stale; regenerate affected plan files before applying:\n%s", strings.Join(messages, "\n"))
}

func cleanupTasksForPlans(plans []*repairPlan) []cleanupTask {
	tasks := make([]cleanupTask, 0)
	for _, plan := range plans {
		for _, store := range storesByUUID(plan.Stores, plan.RebuildStores) {
			for _, replicaID := range store.CleanupReplicas {
				tasks = append(tasks, cleanupTask{
					Plan:      plan,
					Store:     store,
					ReplicaID: replicaID,
				})
			}
		}
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].Store.UUID != tasks[j].Store.UUID {
			return tasks[i].Store.UUID < tasks[j].Store.UUID
		}
		if tasks[i].Plan.ShardID != tasks[j].Plan.ShardID {
			return tasks[i].Plan.ShardID < tasks[j].Plan.ShardID
		}
		return tasks[i].ReplicaID < tasks[j].ReplicaID
	})
	return tasks
}

func storesFromCleanupTasks(tasks []cleanupTask) []planStore {
	byUUID := make(map[string]planStore)
	for _, task := range tasks {
		store := byUUID[task.Store.UUID]
		if store.UUID == "" {
			store = task.Store
			store.CleanupReplicas = nil
		}
		store.CleanupReplicas = append(store.CleanupReplicas, task.ReplicaID)
		store.CleanupReplicas = uniqueUint64s(store.CleanupReplicas)
		byUUID[store.UUID] = store
	}
	ids := make([]string, 0, len(byUUID))
	for uuid := range byUUID {
		ids = append(ids, uuid)
	}
	sort.Strings(ids)
	ret := make([]planStore, 0, len(ids))
	for _, uuid := range ids {
		ret = append(ret, byUUID[uuid])
	}
	return ret
}

func validateCombinedPlannedLocalCleanupCompleteness(plans []*repairPlan) error {
	tasks := cleanupTasksForPlans(plans)
	byStore := make(map[string]planStore)
	replicasByStore := make(map[string]map[uint64][]uint64)
	for _, task := range tasks {
		byStore[task.Store.UUID] = task.Store
		if replicasByStore[task.Store.UUID] == nil {
			replicasByStore[task.Store.UUID] = localReplicasByShard(task.Store.NodeHostDir, task.Store.DeploymentID)
		}
		byShard := replicasByStore[task.Store.UUID]
		byShard[task.Plan.ShardID] = removeUint64(byShard[task.Plan.ShardID], task.ReplicaID)
	}
	messages := make([]string, 0)
	storeIDs := make([]string, 0, len(replicasByStore))
	for uuid := range replicasByStore {
		storeIDs = append(storeIDs, uuid)
	}
	sort.Strings(storeIDs)
	for _, uuid := range storeIDs {
		store := byStore[uuid]
		byShard := replicasByStore[uuid]
		shardIDs := make([]uint64, 0, len(byShard))
		for shardID := range byShard {
			shardIDs = append(shardIDs, shardID)
		}
		sort.Slice(shardIDs, func(i, j int) bool { return shardIDs[i] < shardIDs[j] })
		for _, shardID := range shardIDs {
			replicas := byShard[shardID]
			if len(replicas) <= 1 {
				continue
			}
			messages = append(messages, fmt.Sprintf("- store %s would still have duplicate local replicas for shard %d after combined plans: %v", store.UUID, shardID, replicas))
		}
	}
	if len(messages) == 0 {
		return nil
	}
	return fmt.Errorf("combined repair plans are incomplete; refusing to start apply before mutating HAKeeper or local data:\n%s", strings.Join(messages, "\n"))
}

func combinedCleanupSummary(tasks []cleanupTask) string {
	parts := make([]string, 0, len(tasks))
	for _, task := range tasks {
		parts = append(parts, fmt.Sprintf("%s shard %d replica %d", task.Store.UUID, task.Plan.ShardID, task.ReplicaID))
	}
	if len(parts) == 0 {
		return "no local replicas need cleanup"
	}
	return "delete local data for " + strings.Join(parts, "; ")
}

func combinedBackupDir(plans []*repairPlan) string {
	if len(plans) == 0 || plans[0].Local == nil {
		return ""
	}
	return plans[0].Local.BackupDir
}

func planShardIDs(plans []*repairPlan) []uint64 {
	ret := make([]uint64, 0, len(plans))
	for _, plan := range plans {
		ret = append(ret, plan.ShardID)
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i] < ret[j] })
	return ret
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
	stdoutf("started %s pid=%d log=%s\n", store.UUID, cmd.Process.Pid, logPath)
	return nil
}

func repairPayloadForPlan(plan *repairPlan, blockedStores []string, reason string) repairPayload {
	return repairPayloadForPlanWithCleanup(plan, blockedStores, reason, false)
}

func repairPayloadForPlanWithCleanup(plan *repairPlan, blockedStores []string, reason string, includeCleanup bool) repairPayload {
	cleanup := map[string][]uint64(nil)
	if includeCleanup {
		cleanup = cleanupReplicasByStoreForPlan(plan)
	}
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
		BlockedStores:          blockedStores,
		Reason:                 reason,
		CleanupReplicasByStore: cleanup,
		Force:                  true,
	}
}

func cleanupReplicasByStoreForPlan(plan *repairPlan) map[string][]uint64 {
	cleanup := make(map[string][]uint64)
	for _, store := range plan.Stores {
		if len(store.CleanupReplicas) == 0 {
			continue
		}
		cleanup[store.UUID] = append([]uint64(nil), store.CleanupReplicas...)
	}
	if len(cleanup) == 0 {
		return nil
	}
	return cleanup
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

func storesWithAnyCleanup(stores []planStore) []planStore {
	out := make([]planStore, 0, len(stores))
	for _, store := range stores {
		if len(store.CleanupReplicas) > 0 {
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
		stdoutf("Review store %s\n", store.UUID)
		stdoutf("  role=%s targetReplica=%d local=%v cleanup=%v\n", store.Role, store.TargetReplicaID, store.LocalReplicas, store.CleanupReplicas)
		if store.ConfigPath != "" {
			stdoutf("  config=%s\n", store.ConfigPath)
		}
		if store.NodeHostDir != "" {
			stdoutf("  nodeHostDir=%s\n", store.NodeHostDir)
		}
		for _, warning := range store.Warnings {
			stdoutf("  warning: %s\n", warning)
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
	stdoutf("Repair plan %s mode=%s shard=%d\n", plan.Version, plan.Mode, plan.ShardID)
	if len(plan.HAKeeperAddresses) > 0 {
		stdoutf("HAKeeper: %s\n", strings.Join(plan.HAKeeperAddresses, ","))
	}
	if len(plan.Stores) > 0 {
		stdoutln("Stores:")
		for _, store := range plan.Stores {
			stdoutf("  %s role=%s targetReplica=%d local=%v cleanup=%v\n",
				store.UUID,
				store.Role,
				store.TargetReplicaID,
				store.LocalReplicas,
				store.CleanupReplicas,
			)
			for _, warning := range store.Warnings {
				stdoutf("    warning: %s\n", warning)
			}
		}
	}
	if len(plan.InitialBlockedStores) > 0 {
		stdoutf("Initial blocked stores: %v\n", plan.InitialBlockedStores)
	}
	if len(plan.PersistentBlockedStores) > 0 {
		stdoutf("Persistent blocked stores: %v\n", plan.PersistentBlockedStores)
	}
	if len(plan.Warnings) > 0 {
		stdoutln("Warnings:")
		for _, warning := range plan.Warnings {
			stdoutf("  - %s\n", warning)
		}
	}
	stdoutf("Actions: %d\n", len(plan.Actions))
}

func writePlanFile(path string, plan *repairPlan) error {
	data, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0640)
}

func writePlanBundleFile(path string, plans []*repairPlan) error {
	if len(plans) == 1 {
		return writePlanFile(path, plans[0])
	}
	data, err := json.MarshalIndent(plans, "", "  ")
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

func readPlanFiles(paths string) ([]*repairPlan, error) {
	parts := strings.Split(paths, ",")
	plans := make([]*repairPlan, 0, len(parts))
	for _, part := range parts {
		path := strings.TrimSpace(part)
		if path == "" {
			continue
		}
		plan, err := readPlanFile(path)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	if len(plans) == 0 {
		return nil, fmt.Errorf("no plan files specified")
	}
	return plans, nil
}

func askChoice(prompt string, choices []string, def string) (string, error) {
	stdoutf("%s [%s] default=%s: ", prompt, strings.Join(choices, "/"), def)
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
	stdoutf("%s: ", prompt)
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
	stdoutf("%s [%s]: ", prompt, def)
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

func kubectlCommand(kubectlPath string, kubeContext string) string {
	kubectl := firstNonEmpty(kubectlPath, "kubectl")
	if kubeContext == "" {
		return shellQuote(kubectl)
	}
	return shellQuote(kubectl) + " --context " + shellQuote(kubeContext)
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
