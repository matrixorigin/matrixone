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
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/plugin/tan"
	"github.com/lni/dragonboat/v4/tools"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

const repairReasonPrefix = "__mo_log_shard_repair__:"

type repairPayload struct {
	Op                     string              `json:"op"`
	Shard                  shardInput          `json:"shard"`
	ShardID                uint64              `json:"shardID"`
	BlockedStores          []string            `json:"blockedStores"`
	Stores                 []string            `json:"stores"`
	Reason                 string              `json:"reason"`
	CleanupReplicasByStore map[string][]uint64 `json:"cleanupReplicasByStore"`
	Force                  bool                `json:"force"`
	DryRun                 bool                `json:"dryRun"`
}

type shardInput struct {
	ShardID           uint64            `json:"shardID"`
	Replicas          map[uint64]string `json:"replicas"`
	NonVotingReplicas map[uint64]string `json:"nonVotingReplicas"`
	Epoch             uint64            `json:"epoch"`
	LeaderID          uint64            `json:"leaderID"`
	Term              uint64            `json:"term"`
}

type reasonPayload struct {
	Reason                 string              `json:"reason,omitempty"`
	CleanupReplicasByStore map[string][]uint64 `json:"cleanupReplicasByStore,omitempty"`
}

type repairResult struct {
	Op             string                               `json:"op"`
	DryRun         bool                                 `json:"dryRun"`
	Before         logpb.LogShardInfo                   `json:"before"`
	After          logpb.LogShardInfo                   `json:"after"`
	RepairState    logpb.LogShardRepairState            `json:"repairState"`
	RepairStateSet bool                                 `json:"repairStateSet"`
	AllRepairs     map[uint64]logpb.LogShardRepairState `json:"allRepairs"`
}

func main() {
	logutil.SetupMOLogger(&logutil.LogConfig{})
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return usage()
	}
	switch args[0] {
	case "hakeeper":
		return runHAKeeper(args[1:])
	case "local":
		return runLocal(args[1:])
	case "k8s":
		return runK8s(args[1:])
	default:
		return usage()
	}
}

func usage() error {
	return fmt.Errorf(`usage:
  mo-logservice-repair hakeeper state --addresses host:port[,host:port]
  mo-logservice-repair hakeeper repair --addresses host:port[,host:port] --payload JSON
  mo-logservice-repair hakeeper unblock --addresses host:port[,host:port] --payload JSON
  mo-logservice-repair local import-snapshot --deployment-id ID --node-host-id ID --node-host-dir DIR --raft-address ADDR --replica-id ID --snapshot-dir DIR --members JSON

K8s/local plan/apply/verify commands are intentionally reserved for the next phase.`)
}

func runK8s(args []string) error {
	if len(args) == 0 {
		return usage()
	}
	switch args[0] {
	case "diagnose", "online-plan", "online-apply", "recover-plan", "recover-apply", "verify":
		return fmt.Errorf("k8s %s is not implemented yet", args[0])
	default:
		return usage()
	}
}

func runLocal(args []string) error {
	if len(args) == 0 {
		return usage()
	}
	switch args[0] {
	case "diagnose", "online-plan", "online-apply", "recover-plan", "recover-apply", "verify":
		return fmt.Errorf("local %s is not implemented yet", args[0])
	case "import-snapshot":
		return runImportSnapshot(args[1:])
	default:
		return usage()
	}
}

func runHAKeeper(args []string) error {
	if len(args) == 0 {
		return usage()
	}
	fs := flag.NewFlagSet("hakeeper "+args[0], flag.ExitOnError)
	var addresses string
	var payload string
	var timeout time.Duration
	fs.StringVar(&addresses, "addresses", "127.0.0.1:65201,127.0.0.1:65301,127.0.0.1:65401", "comma-separated HAKeeper service addresses")
	fs.StringVar(&payload, "payload", "", "repair/unblock JSON payload")
	fs.DurationVar(&timeout, "timeout", 10*time.Second, "request timeout")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	client, addr, err := connect(ctx, splitAddresses(addresses))
	if err != nil {
		return fmt.Errorf("connect hakeeper: %w", err)
	}
	defer client.Close()

	switch args[0] {
	case "state":
		state, err := getClusterState(ctx, client, addr)
		if err != nil {
			return err
		}
		return printJSON(state)
	case "repair", "unblock":
		if payload == "" {
			return fmt.Errorf("-payload is required")
		}
		var req repairPayload
		if err := json.Unmarshal([]byte(payload), &req); err != nil {
			return fmt.Errorf("invalid payload: %w", err)
		}
		req.Op = args[0]
		ret, err := applyHAKeeperPayload(ctx, client, addr, req)
		if err != nil {
			return err
		}
		return printJSON(ret)
	default:
		return usage()
	}
}

func applyHAKeeperPayload(
	ctx context.Context,
	client morpc.RPCClient,
	addr string,
	req repairPayload,
) (repairResult, error) {
	before, err := getClusterState(ctx, client, addr)
	if err != nil {
		return repairResult{}, fmt.Errorf("get cluster state before: %w", err)
	}
	ret := repairResult{
		Op:         req.Op,
		DryRun:     req.DryRun,
		AllRepairs: before.LogShardRepairs,
	}

	switch strings.ToLower(strings.TrimSpace(req.Op)) {
	case "repair":
		repair := logpb.RepairLogShard{
			Shard:         req.Shard.toLogShardInfo(),
			BlockedStores: req.BlockedStores,
			Reason:        encodeReason(req.Reason, req.CleanupReplicasByStore),
			Force:         req.Force,
		}
		if repair.Shard.ShardID == 0 {
			return repairResult{}, fmt.Errorf("repair shardID is required")
		}
		ret.Before = before.LogState.Shards[repair.Shard.ShardID]
		if repair.Shard.LeaderID == 0 {
			repair.Shard.LeaderID = ret.Before.LeaderID
		}
		if repair.Shard.Term == 0 {
			repair.Shard.Term = ret.Before.Term
		}
		ret.After = repair.Shard
		if req.DryRun {
			return ret, nil
		}
		if _, err := requestRPC(ctx, client, addr, logpb.Request{
			Method:         logpb.REPAIR_LOG_SHARD,
			RepairLogShard: &repair,
		}); err != nil {
			return repairResult{}, fmt.Errorf("repair log shard: %w", err)
		}
		after, err := getClusterState(ctx, client, addr)
		if err != nil {
			return repairResult{}, fmt.Errorf("get cluster state after: %w", err)
		}
		ret.After = after.LogState.Shards[repair.Shard.ShardID]
		ret.RepairState, ret.RepairStateSet = after.LogShardRepairs[repair.Shard.ShardID]
		ret.AllRepairs = after.LogShardRepairs
		return ret, nil
	case "unblock":
		if req.ShardID == 0 {
			return repairResult{}, fmt.Errorf("unblock shardID is required")
		}
		unblock := logpb.UnblockLogShardStores{
			ShardID: req.ShardID,
			Stores:  req.Stores,
			Reason:  req.Reason,
		}
		ret.Before = before.LogState.Shards[req.ShardID]
		ret.After = ret.Before
		ret.RepairState, ret.RepairStateSet = before.LogShardRepairs[req.ShardID]
		if req.DryRun {
			return ret, nil
		}
		if _, err := requestRPC(ctx, client, addr, logpb.Request{
			Method:                logpb.UNBLOCK_LOG_SHARD_STORES,
			UnblockLogShardStores: &unblock,
		}); err != nil {
			return repairResult{}, fmt.Errorf("unblock log shard stores: %w", err)
		}
		after, err := getClusterState(ctx, client, addr)
		if err != nil {
			return repairResult{}, fmt.Errorf("get cluster state after: %w", err)
		}
		ret.After = after.LogState.Shards[req.ShardID]
		ret.RepairState, ret.RepairStateSet = after.LogShardRepairs[req.ShardID]
		ret.AllRepairs = after.LogShardRepairs
		return ret, nil
	default:
		return repairResult{}, fmt.Errorf("unsupported repair op %q", req.Op)
	}
}

func runImportSnapshot(args []string) error {
	fs := flag.NewFlagSet("local import-snapshot", flag.ExitOnError)
	var deploymentID uint64
	var nodeHostID string
	var nodeHostDir string
	var raftAddress string
	var listenAddress string
	var snapshotDir string
	var replicaID uint64
	var rtt uint64
	var membersJSON string

	fs.Uint64Var(&deploymentID, "deployment-id", 0, "dragonboat deployment id")
	fs.StringVar(&nodeHostID, "node-host-id", "", "dragonboat nodehost id")
	fs.StringVar(&nodeHostDir, "node-host-dir", "", "dragonboat nodehost data dir")
	fs.StringVar(&raftAddress, "raft-address", "", "raft service address")
	fs.StringVar(&listenAddress, "listen-address", "", "raft listen address")
	fs.StringVar(&snapshotDir, "snapshot-dir", "", "exported snapshot dir")
	fs.Uint64Var(&replicaID, "replica-id", 0, "target replica id on this nodehost")
	fs.Uint64Var(&rtt, "rtt-ms", 200, "dragonboat rtt in milliseconds")
	fs.StringVar(&membersJSON, "members", "", "JSON object mapping replica id to raft address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := importSnapshot(deploymentID, nodeHostID, nodeHostDir, raftAddress, listenAddress, snapshotDir, replicaID, rtt, membersJSON); err != nil {
		return fmt.Errorf("import snapshot: %w", err)
	}
	return nil
}

func importSnapshot(
	deploymentID uint64,
	nodeHostID string,
	nodeHostDir string,
	raftAddress string,
	listenAddress string,
	snapshotDir string,
	replicaID uint64,
	rtt uint64,
	membersJSON string,
) error {
	if deploymentID == 0 {
		return fmt.Errorf("missing -deployment-id")
	}
	if nodeHostID == "" {
		return fmt.Errorf("missing -node-host-id")
	}
	if nodeHostDir == "" {
		return fmt.Errorf("missing -node-host-dir")
	}
	if raftAddress == "" {
		return fmt.Errorf("missing -raft-address")
	}
	if listenAddress == "" {
		listenAddress = raftAddress
	}
	if snapshotDir == "" {
		return fmt.Errorf("missing -snapshot-dir")
	}
	if replicaID == 0 {
		return fmt.Errorf("missing -replica-id")
	}
	members, err := parseMembers(membersJSON)
	if err != nil {
		return err
	}
	if _, ok := members[replicaID]; !ok {
		return fmt.Errorf("replica %d is not included in members", replicaID)
	}
	logdb := config.GetTinyMemLogDBConfig()
	cfg := config.NodeHostConfig{
		DeploymentID:        deploymentID,
		NodeHostID:          nodeHostID,
		NodeHostDir:         nodeHostDir,
		RTTMillisecond:      rtt,
		AddressByNodeHostID: true,
		RaftAddress:         raftAddress,
		ListenAddress:       listenAddress,
		Expert: config.ExpertConfig{
			LogDBFactory: tan.Factory,
			LogDB:        logdb,
		},
	}
	return tools.ImportSnapshot(cfg, snapshotDir, members, replicaID)
}

type rpcRequest struct {
	logpb.Request
	payload []byte
	pool    *sync.Pool
}

func (r *rpcRequest) ProtoSize() int {
	return r.Request.ProtoSize() + len(r.payload)
}

func (r *rpcRequest) Release() {
	if r.pool != nil {
		r.Request = logpb.Request{}
		r.payload = nil
		r.pool.Put(r)
	}
}

func (r *rpcRequest) SetID(id uint64) {
	r.RequestID = id
}

func (r *rpcRequest) GetID() uint64 {
	return r.RequestID
}

func (r *rpcRequest) DebugString() string {
	return r.Request.Method.String()
}

func (r *rpcRequest) GetPayloadField() []byte {
	return r.payload
}

func (r *rpcRequest) SetPayloadField(data []byte) {
	r.payload = data
}

type rpcResponse struct {
	logpb.Response
	payload []byte
	pool    *sync.Pool
}

func (r *rpcResponse) Release() {
	if r.pool != nil {
		r.Response = logpb.Response{}
		r.payload = nil
		r.pool.Put(r)
	}
}

func (r *rpcResponse) SetID(id uint64) {
	r.RequestID = id
}

func (r *rpcResponse) GetID() uint64 {
	return r.RequestID
}

func (r *rpcResponse) DebugString() string {
	return r.Response.Method.String()
}

func (r *rpcResponse) GetPayloadField() []byte {
	return r.payload
}

func (r *rpcResponse) SetPayloadField(data []byte) {
	r.payload = data
}

func connect(ctx context.Context, addresses []string) (morpc.RPCClient, string, error) {
	cfg := morpc.Config{}
	cfg.Adjust()
	client, err := cfg.NewClient("", "logservice-repair", func() morpc.Message {
		return &rpcResponse{}
	})
	if err != nil {
		return nil, "", err
	}
	for _, addr := range addresses {
		ok, err := checkHAKeeper(ctx, client, addr)
		if err == nil && ok {
			return client, addr, nil
		}
	}
	_ = client.Close()
	return nil, "", fmt.Errorf("no HAKeeper found at %v", addresses)
}

func checkHAKeeper(ctx context.Context, client morpc.RPCClient, addr string) (bool, error) {
	resp, err := requestRPC(ctx, client, addr, logpb.Request{Method: logpb.CHECK_HAKEEPER})
	if err != nil {
		return false, err
	}
	return resp.IsHAKeeper, nil
}

func getClusterState(ctx context.Context, client morpc.RPCClient, addr string) (logpb.CheckerState, error) {
	resp, err := requestRPC(ctx, client, addr, logpb.Request{Method: logpb.GET_CLUSTER_STATE})
	if err != nil {
		return logpb.CheckerState{}, err
	}
	if resp.CheckerState == nil {
		return logpb.CheckerState{}, fmt.Errorf("empty checker state response")
	}
	return *resp.CheckerState, nil
}

func requestRPC(ctx context.Context, client morpc.RPCClient, addr string, req logpb.Request) (logpb.Response, error) {
	future, err := client.Send(ctx, addr, &rpcRequest{Request: req})
	if err != nil {
		return logpb.Response{}, err
	}
	defer future.Close()
	msg, err := future.Get()
	if err != nil {
		return logpb.Response{}, err
	}
	response, ok := msg.(*rpcResponse)
	if !ok {
		return logpb.Response{}, fmt.Errorf("unexpected response type %T", msg)
	}
	resp := response.Response
	defer response.Release()
	if resp.ErrorCode != 0 {
		return logpb.Response{}, fmt.Errorf("remote error %d: %s", resp.ErrorCode, resp.ErrorMessage)
	}
	return resp, nil
}

func parseMembers(data string) (map[uint64]string, error) {
	if data == "" {
		return nil, fmt.Errorf("missing -members")
	}
	var raw map[string]string
	if err := json.Unmarshal([]byte(data), &raw); err != nil {
		return nil, err
	}
	members := make(map[uint64]string, len(raw))
	for replicaID, address := range raw {
		id, err := strconv.ParseUint(replicaID, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid replica id %q: %w", replicaID, err)
		}
		if address == "" {
			return nil, fmt.Errorf("empty address for replica %d", id)
		}
		members[id] = address
	}
	return members, nil
}

func splitAddresses(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func encodeReason(reason string, cleanup map[string][]uint64) string {
	if len(cleanup) == 0 {
		return reason
	}
	data, err := json.Marshal(reasonPayload{
		Reason:                 reason,
		CleanupReplicasByStore: cleanup,
	})
	if err != nil {
		panic(err)
	}
	return repairReasonPrefix + string(data)
}

func (s shardInput) toLogShardInfo() logpb.LogShardInfo {
	return logpb.LogShardInfo{
		ShardID:           s.ShardID,
		Replicas:          s.Replicas,
		NonVotingReplicas: s.NonVotingReplicas,
		Epoch:             s.Epoch,
		LeaderID:          s.LeaderID,
		Term:              s.Term,
	}
}

func printJSON(v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}
