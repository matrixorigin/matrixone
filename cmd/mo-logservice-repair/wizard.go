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
	mode          string
	baseDir       string
	confDir       string
	shardID       uint64
	shardIDSet    bool
	addresses     string
	output        string
	planPath      string
	apply         bool
	yes           bool
	timeout       time.Duration
	moServicePath string
	namespace     string
	kubeContext   string
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
}

type localPlanSettings struct {
	MOServicePath string `json:"moServicePath,omitempty"`
	BackupDir     string `json:"backupDir,omitempty"`
	LogDir        string `json:"logDir,omitempty"`
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
	if opts.output != "" {
		if err := writePlanFile(opts.output, plan); err != nil {
			return err
		}
		fmt.Printf("plan written to %s\n", opts.output)
	}
	if !opts.apply {
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
	plan := &repairPlan{
		Version:        repairPlanVersion,
		Mode:           modeK8s,
		CreatedAt:      time.Now().Format(time.RFC3339),
		Namespace:      opts.namespace,
		KubeContext:    opts.kubeContext,
		ShardID:        opts.shardID,
		ApplySupported: false,
		Warnings: []string{
			"k8s mode is plan-only in this version; it does not edit PVC data directly",
			"cleaning replicas in k8s must be done from a maintenance pod after the target LogService pod is stopped",
		},
	}
	kubectl := "kubectl"
	if opts.kubeContext != "" {
		kubectl += " --context " + shellQuote(opts.kubeContext)
	}
	plan.Actions = []planAction{
		{
			Type:        "discover-k8s",
			Description: "Discover LogService pods, services, configmaps, and PVCs in the namespace.",
			Command:     fmt.Sprintf("%s -n %s get pod,svc,cm,pvc", kubectl, shellQuote(opts.namespace)),
		},
		{
			Type:        "read-hakeeper-state",
			Description: "Port-forward or exec into a LogService pod and run hakeeper state to confirm live membership.",
		},
		{
			Type:        "stop-target-pods",
			Description: "Stop only the LogService pods that contain dirty local replicas before any PVC cleanup.",
		},
		{
			Type:        "repair-pod-cleanup",
			Description: "Start a temporary repair pod mounting the same PVC and run local clean-replica inside that pod.",
		},
		{
			Type:        "restart-and-unblock",
			Description: "Restart LogService pods, then unblock cleaned stores one at a time and verify snapshot restore.",
		},
	}
	return plan, nil
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
		addresses = discoverHAKeeperAddresses(configs)
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
		store := buildPlanStore(opts.shardID, shard, cfg, sourceStore)
		if opts.moServicePath != "" && store.MOServicePath == "" {
			store.MOServicePath = opts.moServicePath
		}
		if store.Role == "rebuild" {
			plan.RebuildStores = append(plan.RebuildStores, store.UUID)
			plan.InitialBlockedStores = append(plan.InitialBlockedStores, store.UUID)
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

func buildPlanStore(shardID uint64, shard logpb.LogShardInfo, cfg localLogConfig, sourceStore string) planStore {
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
	role := "unused"
	switch {
	case cfg.UUID == sourceStore:
		role = "source"
	case targetReplicaID != 0:
		role = "rebuild"
	case len(localReplicas) > 0:
		role = "stale"
	}
	cleanupReplicas := []uint64(nil)
	if role == "rebuild" {
		cleanupReplicas = append(cleanupReplicas, localReplicas...)
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
		MOServicePath:      binary,
		NeedsStopAndStart:  role == "rebuild" && len(cleanupReplicas) > 0,
		PresentInHAKeeper:  targetReplicaID != 0,
		PresentInLocalData: len(localReplicas) > 0,
	}
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
		if store.Role != "rebuild" || len(store.CleanupReplicas) == 0 {
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
		if store.Role != "rebuild" {
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
		if store.Role != "rebuild" || len(store.CleanupReplicas) == 0 {
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
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client, addr, err := connect(ctx, plan.HAKeeperAddresses)
	if err != nil {
		return fmt.Errorf("connect hakeeper: %w", err)
	}
	defer client.Close()

	fmt.Println("step 1: write repair state and block stale/dirty stores")
	if _, err := applyHAKeeperPayload(ctx, client, addr, repairPayloadForPlan(plan, plan.InitialBlockedStores, "wizard: block stale/dirty stores before cleanup")); err != nil {
		return err
	}
	rebuildStores := storesByUUID(plan.Stores, plan.RebuildStores)
	fmt.Println("step 2: stop LogService stores that need local cleanup")
	for _, store := range rebuildStores {
		if len(store.CleanupReplicas) == 0 {
			continue
		}
		if err := stopStore(store); err != nil {
			return err
		}
	}
	fmt.Println("step 3: back up local logservice-data")
	for _, store := range rebuildStores {
		if len(store.CleanupReplicas) == 0 {
			continue
		}
		if err := backupStore(plan.Local.BackupDir, store); err != nil {
			return err
		}
	}
	fmt.Println("step 4: clean dirty local replicas")
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
		req := repairPayload{
			Op:         "unblock",
			ShardID:    plan.ShardID,
			ShardIDSet: true,
			Stores:     []string{store.UUID},
			Reason:     "wizard: cleaned " + store.UUID,
		}
		if _, err := applyHAKeeperPayload(ctx, client, addr, req); err != nil {
			return err
		}
		fmt.Printf("unblocked %s; wait 30s for L/Start/snapshot restore\n", store.UUID)
		time.Sleep(30 * time.Second)
	}
	fmt.Println("step 7: write final repair state")
	if _, err := applyHAKeeperPayload(ctx, client, addr, repairPayloadForPlan(plan, plan.PersistentBlockedStores, "wizard: repair complete")); err != nil {
		return err
	}
	fmt.Println("step 8: final HAKeeper state")
	state, err := getClusterState(ctx, client, addr)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{
		"logShards": state.LogState.Shards,
		"repairs":   state.LogShardRepairs,
	})
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

func discoverHAKeeperAddresses(configs []localLogConfig) []string {
	for _, cfg := range configs {
		if len(cfg.HAKeeperAddrs) > 0 {
			return uniqueStrings(cfg.HAKeeperAddrs)
		}
	}
	addrs := make([]string, 0, len(configs))
	for _, cfg := range configs {
		if cfg.ServiceAddress != "" {
			addrs = append(addrs, cfg.ServiceAddress)
		}
	}
	return uniqueStrings(addrs)
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
