// Copyright 2021 - 2022 Matrix Origin
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

/*
Package hakeeper implements MO's hakeeper component.
*/
package hakeeper

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/lni/dragonboat/v4/logger"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/mohae/deepcopy"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

var (
	plog = logger.GetLogger("hakeeper")
)

var (
	binaryEnc = binary.BigEndian
)

const (
	// When bootstrapping, k8s will first bootstrap the HAKeeper by starting some
	// Log stores with command line options specifying that those stores will be hosting
	// a HAKeeper replicas. It will be k8s's responsibility to assign Replica IDs to those
	// HAKeeper replicas, and those IDs will have to be assigned from the range
	// [K8SIDRangeStart, K8SIDRangeEnd)

	K8SIDRangeStart uint64 = 131072
	K8SIDRangeEnd   uint64 = 262144
	// CheckDuration defines how often HAKeeper checks the health state of the cluster
	CheckDuration = 3 * time.Second
	// DefaultHAKeeperShardID is the shard ID assigned to the special HAKeeper
	// shard.
	DefaultHAKeeperShardID uint64 = 0
	headerSize                    = pb.HeaderSize
)

type IndexQuery struct{}
type StateQuery struct{}
type ScheduleCommandQuery struct{ UUID string }
type CommandDeliveryStateQuery struct{}
type ClusterDetailsQuery struct{ Cfg Config }

type CommandDeliveryState struct {
	Preparing              bool
	Enabled                bool
	HAKeeperAdmissionReady bool
	Ready                  map[string]bool
	CNReady                map[string]bool
	TNReady                map[string]bool
}

type stateMachine struct {
	replicaID uint64
	state     pb.HAKeeperRSMState
}

func parseCmdTag(cmd []byte) pb.HAKeeperUpdateType {
	return pb.HAKeeperUpdateType(binaryEnc.Uint32(cmd))
}

func GetInitialClusterRequestCmd(
	numOfLogShards uint64,
	numOfTNShards uint64,
	numOfLogReplicas uint64,
	nextID uint64,
	nextIDByKey map[string]uint64,
	nonVotingLocality map[string]string,
) []byte {
	return GetInitialClusterRequestCmdWithRecovery(
		numOfLogShards,
		numOfTNShards,
		numOfLogReplicas,
		nextID,
		nextIDByKey,
		nonVotingLocality,
		false,
	)
}

func GetInitialClusterRequestCmdWithRecovery(
	numOfLogShards uint64,
	numOfTNShards uint64,
	numOfLogReplicas uint64,
	nextID uint64,
	nextIDByKey map[string]uint64,
	nonVotingLocality map[string]string,
	logServiceRecovery bool,
) []byte {
	req := pb.InitialClusterRequest{
		NumOfLogShards:     numOfLogShards,
		NumOfTNShards:      numOfTNShards,
		NumOfLogReplicas:   numOfLogReplicas,
		NextID:             nextID,
		NextIDByKey:        nextIDByKey,
		NonVotingLocality:  nonVotingLocality,
		LogServiceRecovery: logServiceRecovery,
	}
	payload, err := req.Marshal()
	if err != nil {
		panic(err)
	}
	cmd := make([]byte, headerSize+len(payload))
	binaryEnc.PutUint32(cmd, uint32(pb.InitialClusterUpdate))
	copy(cmd[headerSize:], payload)
	return cmd
}

// GetRestoreIDWatermarkCmd returns an explicit, idempotent restore request.
func GetRestoreIDWatermarkCmd(
	nextID uint64,
	nextIDByKey map[string]uint64,
	logServiceRecovery bool,
) []byte {
	req := pb.RestoreIDWatermarkRequest{
		NextID:             nextID,
		NextIDByKey:        nextIDByKey,
		LogServiceRecovery: logServiceRecovery,
	}
	cmd := make([]byte, headerSize+req.ProtoSize())
	binaryEnc.PutUint32(cmd, uint32(pb.RestoreIDWatermarkUpdate))
	if _, err := req.MarshalTo(cmd[headerSize:]); err != nil {
		panic(err)
	}
	return cmd
}

func GetCompleteLogServiceRecoveryCmd() []byte {
	cmd := make([]byte, headerSize)
	binaryEnc.PutUint32(cmd, uint32(pb.CompleteLogServiceRecoveryUpdate))
	return cmd
}

// GetEnableCommandDeliveryCmd creates the phase-one, one-way protocol
// activation entry. It must only be proposed after every current HAKeeper
// replica has advertised support; older state machines deliberately reject the
// unknown command tag.
func GetEnableCommandDeliveryCmd() []byte {
	return getEnableCommandDeliveryCmd(nil)
}

// GetEnableCommandDeliveryCmdForTargets creates the phase-two activation entry
// with the CN/TN stores that were live when the leader evaluated the barrier.
// HAKeeper keeps historical TN records, so carrying this replicated target set
// lets every RSM ignore stores that had already expired without making
// activation depend on leader-local state.
func GetEnableCommandDeliveryCmdForTargets(
	cnStoreUUIDs, tnStoreUUIDs []string,
) []byte {
	targets := pb.CommandDeliveryTargets{
		CNStoreUUIDs: append([]string(nil), cnStoreUUIDs...),
		TNStoreUUIDs: append([]string(nil), tnStoreUUIDs...),
		Explicit:     true,
	}
	return getEnableCommandDeliveryCmd(&targets)
}

// GetEnableCommandDeliveryCmdForConfig creates a phase-two entry whose RSM
// evaluates the current live CN/TN set at commit time. Carrying timeout ticks
// keeps that decision deterministic across replicas without depending on
// leader-local configuration or a pre-proposal UUID snapshot.
func GetEnableCommandDeliveryCmdForConfig(cfg Config) []byte {
	cfg.Fill()
	targets := pb.CommandDeliveryTargets{
		Explicit:              true,
		CNStoreTimeoutTicks:   uint64(cfg.CNStoreTimeout/time.Second) * uint64(cfg.TickPerSecond),
		TNStoreTimeoutTicks:   uint64(cfg.TNStoreTimeout/time.Second) * uint64(cfg.TickPerSecond),
		EvaluateCurrentStores: true,
	}
	return getEnableCommandDeliveryCmd(&targets)
}

func getEnableCommandDeliveryCmd(targets *pb.CommandDeliveryTargets) []byte {
	payload := []byte(nil)
	if targets != nil {
		payload = make([]byte, targets.ProtoSize())
		if _, err := targets.MarshalTo(payload); err != nil {
			panic(err)
		}
	}
	cmd := make([]byte, headerSize+len(payload))
	binaryEnc.PutUint32(cmd, uint32(pb.EnableCommandDeliveryUpdate))
	copy(cmd[headerSize:], payload)
	return cmd
}

func parseEnableCommandDeliveryCmd(cmd []byte) (pb.CommandDeliveryTargets, bool) {
	if len(cmd) <= headerSize {
		return pb.CommandDeliveryTargets{}, false
	}
	var targets pb.CommandDeliveryTargets
	if err := targets.Unmarshal(cmd[headerSize:]); err != nil {
		panic(err)
	}
	return targets, targets.Explicit
}

func commandDeliveryStoreExpired(last, current, timeout uint64) bool {
	return current > last && current-last > timeout
}

func parseInitialClusterRequestCmd(cmd []byte) pb.InitialClusterRequest {
	if parseCmdTag(cmd) != pb.InitialClusterUpdate {
		panic("not a initial cluster update")
	}
	payload := cmd[headerSize:]
	var result pb.InitialClusterRequest
	if err := result.Unmarshal(payload); err != nil {
		panic(err)
	}
	return result
}

func parseTaskTableUserCmd(cmd []byte) pb.TaskTableUser {
	if parseCmdTag(cmd) != pb.SetTaskTableUserUpdate {
		panic("not a task table user update")
	}
	payload := cmd[headerSize:]
	var result pb.TaskTableUser
	if err := result.Unmarshal(payload); err != nil {
		panic(err)
	}
	return result
}

func GetUpdateCommandsCmd(term uint64, cmds []pb.ScheduleCommand) []byte {
	b := pb.CommandBatch{
		Term:     term,
		Commands: cmds,
	}
	data := make([]byte, headerSize+b.ProtoSize())
	binaryEnc.PutUint32(data, uint32(pb.ScheduleCommandUpdate))
	if _, err := b.MarshalTo(data[headerSize:]); err != nil {
		panic(err)
	}
	return data
}

func parseHeartbeatCmd(cmd []byte) []byte {
	return cmd[headerSize:]
}

func parseSetStateCmd(cmd []byte) pb.HAKeeperState {
	return pb.HAKeeperState(binaryEnc.Uint32(cmd[headerSize:]))
}

func parseSetInitTaskStateCmd(cmd []byte) pb.TaskSchedulerState {
	return pb.TaskSchedulerState(binaryEnc.Uint32(cmd[headerSize:]))
}

func parseAllocateIDCmd(cmd []byte) pb.CNAllocateID {
	if parseCmdTag(cmd) != pb.GetIDUpdate {
		panic("not a allocate ID cmd")
	}
	payload := cmd[headerSize:]
	var result pb.CNAllocateID
	if err := result.Unmarshal(payload); err != nil {
		panic(err)
	}
	return result
}

func parseUpdateCNLabelCmd(cmd []byte) pb.CNStoreLabel {
	if parseCmdTag(cmd) != pb.UpdateCNLabel {
		panic("not a SetCNLabel cmd")
	}
	payload := cmd[headerSize:]
	var result pb.CNStoreLabel
	if err := result.Unmarshal(payload); err != nil {
		panic(err)
	}
	return result
}

func parseUpdateCNWorkStateCmd(cmd []byte) pb.CNWorkState {
	if parseCmdTag(cmd) != pb.UpdateCNWorkState {
		panic("not a SetCNWorkState cmd")
	}
	payload := cmd[headerSize:]
	var result pb.CNWorkState
	if err := result.Unmarshal(payload); err != nil {
		panic(err)
	}
	return result
}

func parsePatchCNStoreCmd(cmd []byte) pb.CNStateLabel {
	if parseCmdTag(cmd) != pb.PatchCNStore {
		panic("not a PatchCNStore cmd")
	}
	payload := cmd[headerSize:]
	var result pb.CNStateLabel
	if err := result.Unmarshal(payload); err != nil {
		panic(err)
	}
	return result
}

func parseDeleteCNStoreCmd(cmd []byte) pb.DeleteCNStore {
	if parseCmdTag(cmd) != pb.RemoveCNStore {
		panic("not a RemoveCNStore cmd")
	}
	payload := cmd[headerSize:]
	var result pb.DeleteCNStore
	if err := result.Unmarshal(payload); err != nil {
		panic(err)
	}
	return result
}

func parseUpdateNonVotingReplicaNumCmd(cmd []byte) uint64 {
	if parseCmdTag(cmd) != pb.UpdateNonVotingReplicaNum {
		panic("not a UpdateNonVotingReplicaNum cmd")
	}
	return binaryEnc.Uint64(cmd[headerSize:])
}

func parseUpdateNonVotingLocalityCmd(cmd []byte) pb.Locality {
	if parseCmdTag(cmd) != pb.UpdateNonVotingLocality {
		panic("not a UpdateNonVotingLocality cmd")
	}
	payload := cmd[headerSize:]
	var locality pb.Locality
	if err := locality.Unmarshal(payload); err != nil {
		panic(err)
	}
	return locality
}

func parseLogShardUpdateCmd(cmd []byte) pb.AddLogShard {
	if parseCmdTag(cmd) != pb.LogShardUpdate {
		panic("not a LogShardUpdate cmd")
	}
	payload := cmd[headerSize:]
	var addLogShard pb.AddLogShard
	if err := addLogShard.Unmarshal(payload); err != nil {
		panic(err)
	}
	return addLogShard
}

func GetSetStateCmd(state pb.HAKeeperState) []byte {
	cmd := make([]byte, headerSize+4)
	binaryEnc.PutUint32(cmd, uint32(pb.SetStateUpdate))
	binaryEnc.PutUint32(cmd[headerSize:], uint32(state))
	return cmd
}

func GetSetTaskSchedulerStateCmd(state pb.TaskSchedulerState) []byte {
	cmd := make([]byte, headerSize+4)
	binaryEnc.PutUint32(cmd, uint32(pb.SetTaskSchedulerStateUpdate))
	binaryEnc.PutUint32(cmd[headerSize:], uint32(state))
	return cmd
}

func GetTaskTableUserCmd(user pb.TaskTableUser) []byte {
	cmd := make([]byte, headerSize+user.ProtoSize())
	binaryEnc.PutUint32(cmd, uint32(pb.SetTaskTableUserUpdate))
	if _, err := user.MarshalTo(cmd[headerSize:]); err != nil {
		panic(err)
	}
	return cmd
}

func GetTickCmd() []byte {
	cmd := make([]byte, headerSize)
	binaryEnc.PutUint32(cmd, uint32(pb.TickUpdate))
	return cmd
}

func GetLogStoreHeartbeatCmd(data []byte) []byte {
	return getHeartbeatCmd(data, pb.LogHeartbeatUpdate)
}

func GetCNStoreHeartbeatCmd(data []byte) []byte {
	return getHeartbeatCmd(data, pb.CNHeartbeatUpdate)
}

func GetTNStoreHeartbeatCmd(data []byte) []byte {
	return getHeartbeatCmd(data, pb.TNHeartbeatUpdate)
}

func GetProxyHeartbeatCmd(data []byte) []byte {
	return getHeartbeatCmd(data, pb.ProxyHeartbeatUpdate)
}

func GetUpdateNonVotingReplicaNumCmd(num uint64) []byte {
	cmd := make([]byte, headerSize+8)
	binaryEnc.PutUint32(cmd, uint32(pb.UpdateNonVotingReplicaNum))
	binaryEnc.PutUint64(cmd[headerSize:], num)
	return cmd
}

func GetUpdateNonVotingLocality(locality pb.Locality) []byte {
	cmd := make([]byte, headerSize+locality.ProtoSize())
	binaryEnc.PutUint32(cmd, uint32(pb.UpdateNonVotingLocality))
	if _, err := locality.MarshalTo(cmd[headerSize:]); err != nil {
		panic(err)
	}
	return cmd
}

func getHeartbeatCmd(data []byte, tag pb.HAKeeperUpdateType) []byte {
	cmd := make([]byte, headerSize+len(data))
	binaryEnc.PutUint32(cmd, uint32(tag))
	copy(cmd[headerSize:], data)
	return cmd
}

func GetAllocateIDCmd(allocID pb.CNAllocateID) []byte {
	cmd := make([]byte, headerSize+allocID.ProtoSize())
	binaryEnc.PutUint32(cmd, uint32(pb.GetIDUpdate))
	if _, err := allocID.MarshalTo(cmd[headerSize:]); err != nil {
		panic(err)
	}
	return cmd
}

func GetUpdateCNLabelCmd(label pb.CNStoreLabel) []byte {
	cmd := make([]byte, headerSize+label.ProtoSize())
	binaryEnc.PutUint32(cmd, uint32(pb.UpdateCNLabel))
	if _, err := label.MarshalTo(cmd[headerSize:]); err != nil {
		panic(err)
	}
	return cmd
}

func GetUpdateCNWorkStateCmd(state pb.CNWorkState) []byte {
	cmd := make([]byte, headerSize+state.ProtoSize())
	binaryEnc.PutUint32(cmd, uint32(pb.UpdateCNWorkState))
	if _, err := state.MarshalTo(cmd[headerSize:]); err != nil {
		panic(err)
	}
	return cmd
}

func GetPatchCNStoreCmd(stateLabel pb.CNStateLabel) []byte {
	cmd := make([]byte, headerSize+stateLabel.ProtoSize())
	binaryEnc.PutUint32(cmd, uint32(pb.PatchCNStore))
	if _, err := stateLabel.MarshalTo(cmd[headerSize:]); err != nil {
		panic(err)
	}
	return cmd
}

func GetDeleteCNStoreCmd(cnStore pb.DeleteCNStore) []byte {
	cmd := make([]byte, headerSize+cnStore.ProtoSize())
	binaryEnc.PutUint32(cmd, uint32(pb.RemoveCNStore))
	if _, err := cnStore.MarshalTo(cmd[headerSize:]); err != nil {
		panic(err)
	}
	return cmd
}

func GetAddLogShardCmd(addLogShard pb.AddLogShard) []byte {
	cmd := make([]byte, headerSize+addLogShard.ProtoSize())
	binaryEnc.PutUint32(cmd, uint32(pb.LogShardUpdate))
	if _, err := addLogShard.MarshalTo(cmd[headerSize:]); err != nil {
		panic(err)
	}
	return cmd
}

func NewStateMachine(shardID uint64, replicaID uint64) sm.IStateMachine {
	if shardID != DefaultHAKeeperShardID {
		panic(moerr.NewInvalidInputNoCtxf("HAKeeper shard ID %d does not match DefaultHAKeeperShardID %d", shardID, DefaultHAKeeperShardID))
	}
	return &stateMachine{
		replicaID: replicaID,
		state:     pb.NewRSMState(),
	}
}

func (s *stateMachine) Close() error {
	return nil
}

func (s *stateMachine) assignID() uint64 {
	s.state.NextID++
	return s.state.NextID
}

func (s *stateMachine) assignIDByKey(key string) uint64 {
	if _, ok := s.state.NextIDByKey[key]; !ok {
		s.state.NextIDByKey[key] = 0
	}
	s.state.NextIDByKey[key]++
	return s.state.NextIDByKey[key]
}

const bootstrapAllocationRequestPrefix = "\x00bootstrap-allocation-request/"

func bootstrapAllocationRequestKey(key, requestID string) string {
	return bootstrapAllocationRequestPrefix + key + "\x00" + requestID
}

func (s *stateMachine) handleUpdateCommandsCmd(cmd []byte) sm.Result {
	data := cmd[headerSize:]
	var b pb.CommandBatch
	if err := b.Unmarshal(data); err != nil {
		panic(err)
	}
	if s.state.Term > b.Term {
		return sm.Result{}
	}
	if s.state.LogServiceRecoveryPending && !s.state.LogServiceRecoveryPrepared {
		// Initial cluster recovery has not installed its ID watermarks yet.
		// Reject every command batch so stale replica IDs cannot escape.
		return sm.Result{Data: []byte{1}}
	}

	for _, c := range b.Commands {
		if c.Bootstrapping {
			if s.state.State != pb.HAKeeperBootstrapping {
				plog.Errorf("ignored bootstrapping cmd: %s", c.LogString())
				return sm.Result{}
			}
		}
	}

	s.state.Term = b.Term
	if s.state.CommandDeliveryEnabled || s.state.CommandDeliveryPreparing {
		if s.state.ScheduleCommands == nil {
			s.state.ScheduleCommands = make(map[string]pb.CommandBatch)
		}
	} else {
		// Preserve the legacy replacement contract until the cluster-wide
		// capability transition has entered its preparation barrier.
		s.state.ScheduleCommands = make(map[string]pb.CommandBatch)
	}
	for _, c := range b.Commands {
		if c.Bootstrapping {
			s.handleSetStateCmd(GetSetStateCmd(pb.HAKeeperBootstrapCommandsReceived))
		}
		if c.DeleteCNStore != nil {
			s.handleDeleteCNCmd(c.UUID)
			delete(s.state.ScheduleCommands, c.UUID)
			continue
		}
		if c.DeleteProxyStore != nil {
			s.handleDeleteProxyCmd(c.UUID)
			delete(s.state.ScheduleCommands, c.UUID)
			continue
		}
		l, ok := s.state.ScheduleCommands[c.UUID]
		if !ok {
			l = pb.CommandBatch{
				Commands: make([]pb.ScheduleCommand, 0),
			}
			if s.state.CommandDeliveryEnabled || s.state.CommandDeliveryPreparing {
				l.BatchID = s.state.Index
			}
		} else if s.state.CommandDeliveryEnabled || s.state.CommandDeliveryPreparing {
			if ensureScheduleCommandIDs(&l, s.state.Index) {
				s.state.ScheduleCommands[c.UUID] = l
			}
			if hasEquivalentScheduleCommand(l.Commands, c) {
				continue
			}
			commandID := nextScheduleCommandID(l, s.state.Index)
			if replaced, ok := replacePendingJoinGossipCommand(l, c, commandID); ok {
				// A changed peer set is a retry of the same join operation, not a
				// second operation. Keep the newest addresses so a stale seed list
				// cannot consume the batch before the useful retry runs.
				l = replaced
				l.BatchID = s.state.Index
				s.state.ScheduleCommands[c.UUID] = l
				continue
			}
			// The operator controller dispatches a newly generated command once.
			// Dropping it merely because this UUID still has an unacknowledged
			// batch would lose that command forever. Roll the old and new commands
			// into a new generation instead. Stable per-command IDs let the service
			// skip inherited work without confusing a later identical command with
			// the earlier operation.
			l.BatchID = s.state.Index
		}
		plog.Infof("adding schedule command to hakeeper rsm: %s", c.LogString())
		l.Commands = append(l.Commands, c)
		if s.state.CommandDeliveryEnabled || s.state.CommandDeliveryPreparing {
			l.CommandIDs = append(l.CommandIDs, nextScheduleCommandID(l, s.state.Index))
		}
		s.state.ScheduleCommands[c.UUID] = l
	}

	return sm.Result{}
}

type scheduleCommandIDKey struct {
	originBatchID uint64
	commandIndex  uint64
}

func nextScheduleCommandID(batch pb.CommandBatch, originBatchID uint64) pb.ScheduleCommandID {
	var next uint64
	for i := range batch.CommandIDs {
		if batch.CommandIDs[i].OriginBatchID == originBatchID &&
			batch.CommandIDs[i].CommandIndex >= next {
			next = batch.CommandIDs[i].CommandIndex + 1
		}
	}
	return pb.ScheduleCommandID{OriginBatchID: originBatchID, CommandIndex: next}
}

// ensureScheduleCommandIDs migrates a batch created by an older HAKeeper. It
// preserves every unique valid ID and repairs missing or duplicate positions.
// The caller supplies the current replicated log index, so all replicas make
// the same repair and the new IDs cannot collide with an earlier log entry.
func ensureScheduleCommandIDs(batch *pb.CommandBatch, originBatchID uint64) bool {
	changed := false
	if batch.BatchID == 0 {
		batch.BatchID = originBatchID
		changed = true
	}
	if len(batch.CommandIDs) != len(batch.Commands) {
		ids := make([]pb.ScheduleCommandID, len(batch.Commands))
		copy(ids, batch.CommandIDs)
		batch.CommandIDs = ids
		changed = true
	}
	used := make(map[scheduleCommandIDKey]struct{}, len(batch.CommandIDs))
	for i := range batch.CommandIDs {
		if batch.CommandIDs[i].OriginBatchID == 0 {
			continue
		}
		key := scheduleCommandIDKey{
			originBatchID: batch.CommandIDs[i].OriginBatchID,
			commandIndex:  batch.CommandIDs[i].CommandIndex,
		}
		if _, ok := used[key]; ok {
			batch.CommandIDs[i] = pb.ScheduleCommandID{}
			changed = true
			continue
		}
		used[key] = struct{}{}
	}
	var next uint64
	for i := range batch.CommandIDs {
		if batch.CommandIDs[i].OriginBatchID != 0 {
			continue
		}
		for {
			key := scheduleCommandIDKey{originBatchID: originBatchID, commandIndex: next}
			next++
			if _, ok := used[key]; ok {
				continue
			}
			batch.CommandIDs[i] = pb.ScheduleCommandID{
				OriginBatchID: key.originBatchID,
				CommandIndex:  key.commandIndex,
			}
			used[key] = struct{}{}
			changed = true
			break
		}
	}
	return changed
}

// hasEquivalentScheduleCommand prevents a stalled target from accumulating
// the same logical operator command on every checker cycle. It handles exact
// retries, allocator-ID churn for add/start operations, and unordered gossip
// peer lists without relaxing equality for unrelated command shapes.
func hasEquivalentScheduleCommand(commands []pb.ScheduleCommand, candidate pb.ScheduleCommand) bool {
	for _, command := range commands {
		if scheduleCommandsEqual(command, candidate) {
			return true
		}
	}
	return false
}

func replacePendingJoinGossipCommand(
	batch pb.CommandBatch,
	candidate pb.ScheduleCommand,
	candidateID pb.ScheduleCommandID,
) (pb.CommandBatch, bool) {
	if candidate.JoinGossipCluster == nil {
		return batch, false
	}
	commands := make([]pb.ScheduleCommand, 0, len(batch.Commands))
	commandIDs := make([]pb.ScheduleCommandID, 0, len(batch.CommandIDs))
	replaced := false
	for i, command := range batch.Commands {
		if command.UUID == candidate.UUID &&
			command.Bootstrapping == candidate.Bootstrapping &&
			command.ServiceType == candidate.ServiceType &&
			command.JoinGossipCluster != nil {
			if !replaced {
				commands = append(commands, candidate)
				commandIDs = append(commandIDs, candidateID)
				replaced = true
			}
			continue
		}
		commands = append(commands, command)
		commandIDs = append(commandIDs, batch.CommandIDs[i])
	}
	if !replaced {
		return batch, false
	}
	batch.Commands = commands
	batch.CommandIDs = commandIDs
	return batch, true
}

func scheduleCommandsEqual(left, right pb.ScheduleCommand) bool {
	if proto.Equal(&left, &right) {
		return true
	}
	if left.UUID != right.UUID ||
		left.Bootstrapping != right.Bootstrapping ||
		left.ServiceType != right.ServiceType {
		return false
	}

	// A checker can allocate a new replica ID while retrying the same add/start
	// operation after a target stopped responding. ReplicaID is the identity of
	// the requested replica, not a new piece of work in that situation; keeping
	// both commands would eventually create duplicate replicas when the target
	// recovers. Preserve every other field, including protobuf unknown fields,
	// so this coalescing remains safe during rolling upgrades.
	if left.ConfigChange != nil && right.ConfigChange != nil &&
		left.ConfigChange.ChangeType == right.ConfigChange.ChangeType &&
		coalescibleReplicaChange(left.ServiceType, left.ConfigChange) {
		left = *proto.Clone(&left).(*pb.ScheduleCommand)
		right = *proto.Clone(&right).(*pb.ScheduleCommand)
		left.ConfigChange.Replica.ReplicaID = 0
		right.ConfigChange.Replica.ReplicaID = 0
		return proto.Equal(&left, &right)
	}

	// Gossip peer order is not semantically significant, so compare that field
	// as an order-insensitive list while retaining normal protobuf equality for
	// every other command field.
	if left.JoinGossipCluster != nil && right.JoinGossipCluster != nil {
		left = *proto.Clone(&left).(*pb.ScheduleCommand)
		right = *proto.Clone(&right).(*pb.ScheduleCommand)
		leftPeers := append([]string(nil), left.JoinGossipCluster.Existing...)
		rightPeers := append([]string(nil), right.JoinGossipCluster.Existing...)
		sort.Strings(leftPeers)
		sort.Strings(rightPeers)
		left.JoinGossipCluster.Existing = leftPeers
		right.JoinGossipCluster.Existing = rightPeers
		return proto.Equal(&left, &right)
	}

	return false
}

func coalescibleReplicaChange(serviceType pb.ServiceType, change *pb.ConfigChange) bool {
	switch change.ChangeType {
	case pb.AddReplica, pb.AddNonVotingReplica:
		return true
	case pb.StartReplica, pb.StartNonVotingReplica:
		// TN add/start commands carry the mapped log-shard ID. Keep commands
		// without that field distinct because there is not enough target identity
		// to prove that two operations with the same shard number are retries.
		return serviceType == pb.TNService && change.Replica.LogShardID != 0
	default:
		return false
	}
}

func (s *stateMachine) getCommandBatch(uuid string) sm.Result {
	return s.getCommandBatchFiltered(uuid, false)
}

func (s *stateMachine) getCommandBatchFiltered(
	uuid string,
	filterHAKeeperAdmissions bool,
) sm.Result {
	if batch, ok := s.state.ScheduleCommands[uuid]; ok {
		deliver := make([]pb.ScheduleCommand, 0, len(batch.Commands))
		pending := make([]pb.ScheduleCommand, 0, len(batch.Commands))
		var deliverIDs, pendingIDs []pb.ScheduleCommandID
		if len(batch.CommandIDs) == len(batch.Commands) {
			deliverIDs = make([]pb.ScheduleCommandID, 0, len(batch.CommandIDs))
			pendingIDs = make([]pb.ScheduleCommandID, 0, len(batch.CommandIDs))
		}
		for i, cmd := range batch.Commands {
			if filterHAKeeperAdmissions && !s.logScheduleCommandDeliverable(cmd) {
				pending = append(pending, cmd)
				if pendingIDs != nil {
					pendingIDs = append(pendingIDs, batch.CommandIDs[i])
				}
				continue
			}
			retryable, applied := s.bootstrapReplicaCommandStatus(cmd)
			if applied {
				continue
			}
			deliver = append(deliver, cmd)
			if deliverIDs != nil {
				deliverIDs = append(deliverIDs, batch.CommandIDs[i])
			}
			if retryable {
				pending = append(pending, cmd)
				if pendingIDs != nil {
					pendingIDs = append(pendingIDs, batch.CommandIDs[i])
				}
			}
		}

		if len(pending) == 0 {
			delete(s.state.ScheduleCommands, uuid)
		} else {
			retained := batch
			retained.Commands = pending
			retained.CommandIDs = pendingIDs
			s.state.ScheduleCommands[uuid] = retained
		}

		batch.Commands = deliver
		batch.CommandIDs = deliverIDs
		data, err := batch.Marshal()
		if err != nil {
			panic(err)
		}
		return sm.Result{Data: data}
	}
	return sm.Result{}

}

// hakeeperAdmissionTarget returns the LogStore that a schedule command would
// admit into the HAKeeper shard. Such a target must understand every entry in
// that replicated state machine before an existing member is allowed to add it.
func hakeeperAdmissionTarget(cmd pb.ScheduleCommand) (string, bool) {
	if cmd.ServiceType != pb.LogService || cmd.ConfigChange == nil ||
		cmd.ConfigChange.Replica.ShardID != DefaultHAKeeperShardID {
		return "", false
	}
	switch cmd.ConfigChange.ChangeType {
	case pb.AddReplica, pb.AddNonVotingReplica,
		pb.StartReplica, pb.StartNonVotingReplica:
		return cmd.ConfigChange.Replica.UUID, true
	default:
		return "", false
	}
}

func (s *stateMachine) hasPendingHAKeeperAdmission() bool {
	for _, batch := range s.state.ScheduleCommands {
		for _, cmd := range batch.Commands {
			if _, admission := hakeeperAdmissionTarget(cmd); admission {
				return true
			}
		}
	}
	return false
}

func (s *stateMachine) logScheduleCommandDeliverable(cmd pb.ScheduleCommand) bool {
	if !s.state.CommandDeliveryPreparing && !s.state.CommandDeliveryEnabled &&
		!s.state.ViewMetadataAdmissionPreparing && !s.state.ViewMetadataAdmissionEnabled {
		return true
	}
	uuid, admission := hakeeperAdmissionTarget(cmd)
	if !admission {
		return true
	}
	store, ok := s.state.LogState.Stores[uuid]
	if !ok {
		return false
	}
	if (s.state.CommandDeliveryPreparing || s.state.CommandDeliveryEnabled) &&
		!store.CommandDeliverySupported {
		return false
	}
	if (s.state.ViewMetadataAdmissionPreparing || s.state.ViewMetadataAdmissionEnabled) &&
		!store.ViewMetadataAdmissionSupported {
		return false
	}
	return true
}

// getCommandBatchWithAck implements at-least-once transport. Delivery is a
// read of durable replicated state; only a later heartbeat that names the
// exact batch can complete it. Bootstrap StartReplica commands keep their
// stronger existing contract and remain pending until the heartbeat reports
// the replica. A stale ack can therefore never delete a newer batch, and a
// local start failure cannot be mistaken for bootstrap completion.
func (s *stateMachine) getCommandBatchWithAck(uuid string, ack uint64) sm.Result {
	batch, ok := s.state.ScheduleCommands[uuid]
	if !ok {
		return sm.Result{}
	}
	if ensureScheduleCommandIDs(&batch, s.state.Index) {
		// A snapshot produced before delivery IDs were introduced can still
		// contain pending work after the feature is enabled. Assign stable IDs on
		// this first acknowledged heartbeat instead of exposing a batch that the
		// poll path must reject and the service cannot acknowledge.
		s.state.ScheduleCommands[uuid] = batch
	}
	if ack != 0 && ack == batch.BatchID {
		pending := make([]pb.ScheduleCommand, 0, len(batch.Commands))
		pendingIDs := make([]pb.ScheduleCommandID, 0, len(batch.CommandIDs))
		for i, command := range batch.Commands {
			retryable, applied := s.bootstrapReplicaCommandStatus(command)
			if retryable && !applied {
				pending = append(pending, command)
				pendingIDs = append(pendingIDs, batch.CommandIDs[i])
			}
		}
		if len(pending) == 0 {
			delete(s.state.ScheduleCommands, uuid)
			return sm.Result{}
		}
		batch.Commands = pending
		batch.CommandIDs = pendingIDs
		s.state.ScheduleCommands[uuid] = batch
	}
	data, err := batch.Marshal()
	if err != nil {
		panic(err)
	}
	return sm.Result{Data: data}
}

// bootstrapReplicaCommandStatus returns whether a bootstrap command must be
// retried until its target reports the replica, and whether that report has
// already arrived. Heartbeat proposals can be committed after their caller's
// context times out. Retaining these commands until a later heartbeat
// acknowledges the replica prevents a committed-but-unobserved response from
// permanently losing the command and stalling bootstrap.
func (s *stateMachine) bootstrapReplicaCommandStatus(
	cmd pb.ScheduleCommand,
) (retryable bool, applied bool) {
	if !cmd.Bootstrapping ||
		cmd.ConfigChange == nil ||
		cmd.ConfigChange.ChangeType != pb.StartReplica {
		return false, false
	}

	replica := cmd.ConfigChange.Replica
	switch cmd.ServiceType {
	case pb.LogService:
		store, ok := s.state.LogState.Stores[cmd.UUID]
		if !ok {
			return true, false
		}
		for _, current := range store.Replicas {
			if current.ShardID == replica.ShardID &&
				current.ReplicaID == replica.ReplicaID {
				return true, true
			}
		}
		return true, false
	case pb.TNService:
		store, ok := s.state.TNState.Stores[cmd.UUID]
		if !ok {
			return true, false
		}
		for _, current := range store.Shards {
			if current.ShardID == replica.ShardID &&
				current.ReplicaID == replica.ReplicaID {
				return true, true
			}
		}
		return true, false
	default:
		return false, false
	}
}

func (s *stateMachine) handleCNHeartbeat(cmd []byte) sm.Result {
	data := parseHeartbeatCmd(cmd)
	var hb pb.CNStoreHeartbeat
	if err := hb.Unmarshal(data); err != nil {
		panic(err)
	}
	if !s.updateCNViewMetadataAdmission(hb) {
		return s.attachViewMetadataAdmission(sm.Result{}, hb.UUID, false)
	}
	var result sm.Result
	if s.state.CommandDeliveryPreparing {
		if s.state.CommandDeliveryCNReady == nil {
			s.state.CommandDeliveryCNReady = make(map[string]bool)
		}
		s.state.CommandDeliveryCNReady[hb.UUID] = hb.CommandDeliveryAckSupported
	}
	if (s.state.CommandDeliveryEnabled || s.state.CommandDeliveryPreparing) &&
		hb.CommandDeliveryAckSupported {
		result = s.getCommandBatchWithAck(hb.UUID, hb.AckedCommandBatchID)
		return s.attachViewMetadataAdmission(result, hb.UUID, false)
	}
	if s.state.CommandDeliveryEnabled || s.state.CommandDeliveryPreparing {
		// An old CN may still heartbeat after HAKeeper activates the protocol
		// or while the activation barrier is being evaluated. Do not fall back
		// to destructive delivery: a lost response would recreate the command-
		// loss window. The command remains durable until this CN is upgraded and
		// starts acknowledging it.
		return s.attachViewMetadataAdmission(result, hb.UUID, false)
	}
	result = s.getCommandBatch(hb.UUID)
	return s.attachViewMetadataAdmission(result, hb.UUID, false)
}

func (s *stateMachine) handleTNHeartbeat(cmd []byte) sm.Result {
	data := parseHeartbeatCmd(cmd)
	var hb pb.TNStoreHeartbeat
	if err := hb.Unmarshal(data); err != nil {
		panic(err)
	}
	s.state.TNState.Update(hb, s.state.Tick)
	if s.state.CommandDeliveryPreparing {
		if s.state.CommandDeliveryTNReady == nil {
			s.state.CommandDeliveryTNReady = make(map[string]bool)
		}
		s.state.CommandDeliveryTNReady[hb.UUID] = hb.CommandDeliveryAckSupported
	}
	if (s.state.CommandDeliveryEnabled || s.state.CommandDeliveryPreparing) &&
		hb.CommandDeliveryAckSupported {
		return s.getCommandBatchWithAck(hb.UUID, hb.AckedCommandBatchID)
	}
	if s.state.CommandDeliveryEnabled || s.state.CommandDeliveryPreparing {
		// See the CN path above. A legacy TN must not consume a pending batch
		// once acknowledged delivery is active or preparing.
		return sm.Result{}
	}
	return s.getCommandBatch(hb.UUID)
}

func (s *stateMachine) handleLogHeartbeat(cmd []byte) sm.Result {
	data := parseHeartbeatCmd(cmd)
	var hb pb.LogStoreHeartbeat
	if err := hb.Unmarshal(data); err != nil {
		panic(err)
	}
	s.state.LogState.Update(hb, s.state.Tick)
	if s.state.ViewMetadataAdmissionPreparing {
		if s.state.ViewMetadataAdmissionLogReady == nil {
			s.state.ViewMetadataAdmissionLogReady = make(map[string]bool)
		}
		if hb.ViewMetadataAdmissionSupported {
			s.state.ViewMetadataAdmissionLogReady[hb.UUID] = true
		} else {
			delete(s.state.ViewMetadataAdmissionLogReady, hb.UUID)
		}
	}
	if s.state.CommandDeliveryPreparing {
		if s.state.CommandDeliveryReady == nil {
			s.state.CommandDeliveryReady = make(map[string]bool)
		}
		if hb.CommandDeliverySupported {
			s.state.CommandDeliveryReady[hb.UUID] = true
		} else {
			delete(s.state.CommandDeliveryReady, hb.UUID)
		}
	}
	return s.getCommandBatchFiltered(hb.UUID, true)
}

func (s *stateMachine) handleTick(cmd []byte) sm.Result {
	// A replica entering the preparation barrier or upgraded from an older
	// snapshot can contain pending batches without delivery IDs. Reuse one
	// ordinary replicated tick to make them poll-safe. The decision depends only
	// on replicated state, so replicas that recovered at different times still
	// produce the same state. New batches and commands are assigned stable IDs
	// when inserted, so this potentially large scan is never repeated on the
	// steady-state tick path.
	if (s.state.CommandDeliveryEnabled || s.state.CommandDeliveryPreparing) &&
		(!s.state.CommandDeliveryBatchIDsAssigned ||
			!s.state.CommandDeliveryCommandIDsAssigned) {
		for uuid, batch := range s.state.ScheduleCommands {
			if ensureScheduleCommandIDs(&batch, s.state.Index) {
				s.state.ScheduleCommands[uuid] = batch
			}
		}
		s.state.CommandDeliveryBatchIDsAssigned = true
		s.state.CommandDeliveryCommandIDsAssigned = true
	}
	s.state.Tick++
	return sm.Result{}
}

func (s *stateMachine) handleEnableCommandDelivery(cmd []byte) sm.Result {
	if s.state.CommandDeliveryEnabled {
		return sm.Result{Value: 1}
	}
	if !s.state.CommandDeliveryPreparing {
		// Do not place an entry unknown to an old HAKeeper behind a pending
		// membership command. Unlike capability fields recovered from an old
		// snapshot, the presence of a command is identical on every replica, so
		// this commit-time guard cannot make state machines diverge.
		if s.hasPendingHAKeeperAdmission() {
			return sm.Result{}
		}
		// This replicated barrier deliberately discards capability observations
		// made before every state-machine replica upgraded. Heartbeats after this
		// entry are interpreted by one protocol version on every replica.
		s.state.CommandDeliveryPreparing = true
		s.resetCommandDeliveryBarrier()
		return sm.Result{Value: 2}
	}
	// A snapshot created by the first version of the barrier can have
	// CommandDeliveryPreparing=true without the service readiness maps. Restart
	// the barrier instead of waiting forever on readiness that cannot exist in
	// that snapshot.
	if s.state.CommandDeliveryReady == nil ||
		s.state.CommandDeliveryCNReady == nil ||
		s.state.CommandDeliveryTNReady == nil {
		s.resetCommandDeliveryBarrier()
		return sm.Result{Value: 2}
	}
	shard, ok := s.state.LogState.Shards[DefaultHAKeeperShardID]
	if !ok || len(shard.Replicas) == 0 {
		return sm.Result{}
	}
	for _, uuid := range shard.Replicas {
		if !s.state.CommandDeliveryReady[uuid] {
			return sm.Result{}
		}
	}
	for _, uuid := range shard.NonVotingReplicas {
		if !s.state.CommandDeliveryReady[uuid] {
			return sm.Result{}
		}
	}
	targets, hasTargets := parseEnableCommandDeliveryCmd(cmd)
	if hasTargets && targets.EvaluateCurrentStores {
		for uuid, info := range s.state.CNState.Stores {
			if commandDeliveryStoreExpired(
				info.Tick, s.state.Tick, targets.CNStoreTimeoutTicks,
			) {
				continue
			}
			if !s.state.CommandDeliveryCNReady[uuid] {
				return sm.Result{}
			}
		}
		for uuid, info := range s.state.TNState.Stores {
			if commandDeliveryStoreExpired(
				info.Tick, s.state.Tick, targets.TNStoreTimeoutTicks,
			) {
				continue
			}
			if !s.state.CommandDeliveryTNReady[uuid] {
				return sm.Result{}
			}
		}
	} else if hasTargets {
		for _, uuid := range targets.CNStoreUUIDs {
			if _, ok := s.state.CNState.Stores[uuid]; ok &&
				!s.state.CommandDeliveryCNReady[uuid] {
				return sm.Result{}
			}
		}
		for _, uuid := range targets.TNStoreUUIDs {
			if _, ok := s.state.TNState.Stores[uuid]; ok &&
				!s.state.CommandDeliveryTNReady[uuid] {
				return sm.Result{}
			}
		}
	} else {
		// Keep the no-payload form strict for old snapshots and direct callers.
		// Production phase two entries carry the replicated live-target set above.
		for uuid := range s.state.CNState.Stores {
			if !s.state.CommandDeliveryCNReady[uuid] {
				return sm.Result{}
			}
		}
		for uuid := range s.state.TNState.Stores {
			if !s.state.CommandDeliveryTNReady[uuid] {
				return sm.Result{}
			}
		}
	}
	s.state.CommandDeliveryEnabled = true
	s.state.CommandDeliveryPreparing = false
	s.state.CommandDeliveryReady = nil
	s.state.CommandDeliveryCNReady = nil
	s.state.CommandDeliveryTNReady = nil
	for uuid, batch := range s.state.ScheduleCommands {
		if ensureScheduleCommandIDs(&batch, s.state.Index) {
			s.state.ScheduleCommands[uuid] = batch
		}
	}
	s.state.CommandDeliveryBatchIDsAssigned = true
	s.state.CommandDeliveryCommandIDsAssigned = true
	return sm.Result{Value: 1}
}

// resetCommandDeliveryBarrier removes capability observations that may have
// been recovered differently by replicas upgraded from old snapshots. New
// heartbeats after the replicated barrier repopulate them deterministically.
// Clearing LogState as well as the readiness maps makes the admission filter
// safe without retaining a second, ever-growing registry after activation.
func (s *stateMachine) resetCommandDeliveryBarrier() {
	for uuid, store := range s.state.LogState.Stores {
		store.CommandDeliverySupported = false
		s.state.LogState.Stores[uuid] = store
	}
	s.state.CommandDeliveryReady = make(map[string]bool)
	s.state.CommandDeliveryCNReady = make(map[string]bool)
	s.state.CommandDeliveryTNReady = make(map[string]bool)
}

func (s *stateMachine) handleGetIDCmd(cmd []byte) sm.Result {
	allocIDCmd := parseAllocateIDCmd(cmd)
	// Empty key means it is a shared ID.
	if len(allocIDCmd.Key) == 0 {
		s.state.NextID++
		v := s.state.NextID
		s.state.NextID += allocIDCmd.Batch - 1
		return sm.Result{Value: v}
	}

	if allocIDCmd.RequestID != "" {
		requestKey := bootstrapAllocationRequestKey(allocIDCmd.Key, allocIDCmd.RequestID)
		if id, ok := s.state.NextIDByKey[requestKey]; ok {
			return sm.Result{Value: id}
		}

		v := s.assignIDByKey(allocIDCmd.Key)
		s.state.NextIDByKey[allocIDCmd.Key] += allocIDCmd.Batch - 1
		s.state.NextIDByKey[requestKey] = v
		return sm.Result{Value: v}
	}

	_, ok := s.state.NextIDByKey[allocIDCmd.Key]
	if !ok {
		s.state.NextIDByKey[allocIDCmd.Key] = 0
	}
	s.state.NextIDByKey[allocIDCmd.Key]++
	v := s.state.NextIDByKey[allocIDCmd.Key]
	s.state.NextIDByKey[allocIDCmd.Key] += allocIDCmd.Batch - 1
	return sm.Result{Value: v}
}

func (s *stateMachine) handleSetStateCmd(cmd []byte) sm.Result {
	re := func() sm.Result {
		data := make([]byte, 4)
		binaryEnc.PutUint32(data, uint32(s.state.State))
		return sm.Result{Data: data}
	}
	defer func() {
		plog.Infof("HAKeeper is in %s state", s.state.State)
	}()
	state := parseSetStateCmd(cmd)
	switch s.state.State {
	case pb.HAKeeperCreated:
		return re()
	case pb.HAKeeperBootstrapping:
		if state == pb.HAKeeperBootstrapCommandsReceived {
			s.state.State = state
			return sm.Result{}
		}
		return re()
	case pb.HAKeeperBootstrapCommandsReceived:
		if state == pb.HAKeeperBootstrapFailed || state == pb.HAKeeperRunning {
			if state == pb.HAKeeperRunning && s.state.LogServiceRecoveryPending {
				return re()
			}
			s.state.State = state
			return sm.Result{}
		}
		return re()
	case pb.HAKeeperBootstrapFailed:
		return re()
	case pb.HAKeeperRunning:
		return re()
	default:
		panic("unknown HAKeeper state")
	}
}

func (s *stateMachine) handleSetTaskSchedulerStateUpdateCmd(cmd []byte) sm.Result {
	re := func() sm.Result {
		data := make([]byte, 4)
		binaryEnc.PutUint32(data, uint32(s.state.TaskSchedulerState))
		return sm.Result{Data: data}
	}
	defer func() {
		plog.Infof("Task scheduler is in %s state", s.state.TaskSchedulerState)
	}()
	state := parseSetInitTaskStateCmd(cmd)
	switch s.state.TaskSchedulerState {
	case pb.TaskSchedulerCreated:
		return re()
	case pb.TaskSchedulerRunning:
		if state == pb.TaskSchedulerStopped {
			s.state.TaskSchedulerState = state
			return sm.Result{}
		}
		return re()
	case pb.TaskSchedulerStopped:
		if state == pb.TaskSchedulerRunning {
			s.state.TaskSchedulerState = state
			return sm.Result{}
		}
		return re()
	default:
		panic("unknown task table init state")
	}
}

func (s *stateMachine) handleTaskTableUserCmd(cmd []byte) sm.Result {
	result := sm.Result{Value: uint64(s.state.TaskSchedulerState)}
	if s.state.TaskSchedulerState != pb.TaskSchedulerCreated {
		return result
	}
	req := parseTaskTableUserCmd(cmd)
	if req.Username == "" || req.Password == "" {
		panic("task table username and password cannot be null")
	}

	s.state.TaskTableUser = req
	plog.Infof("task table user set, TaskSchedulerState in TaskSchedulerRunning state")

	s.state.TaskSchedulerState = pb.TaskSchedulerRunning
	return result
}

func (s *stateMachine) handleDeleteCNCmd(uuid string) sm.Result {
	deletedTimeout := time.Hour * 24 * 7
	var pos int
	for _, store := range s.state.DeletedStores {
		if time.Now().UnixNano()-store.DownTime > int64(deletedTimeout) {
			pos++
		}
	}
	s.state.DeletedStores = s.state.DeletedStores[pos:]
	if store, ok := s.state.CNState.Stores[uuid]; ok {
		delete(s.state.CNState.Stores, uuid)
		var addr string
		addrItems := strings.Split(store.SQLAddress, ":")
		if len(addrItems) > 1 {
			addr = addrItems[0]
		}
		s.state.DeletedStores = append(s.state.DeletedStores, pb.DeletedStore{
			UUID:      uuid,
			StoreType: "CN",
			Address:   addr,
			UpTime:    store.UpTime,
			DownTime:  time.Now().UnixNano(),
		})
	}
	return sm.Result{}
}

func (s *stateMachine) handleDeleteProxyCmd(uuid string) sm.Result {
	delete(s.state.ProxyState.Stores, uuid)
	return sm.Result{}
}

func (s *stateMachine) handleProxyHeartbeat(cmd []byte) sm.Result {
	data := parseHeartbeatCmd(cmd)
	var hb pb.ProxyHeartbeat
	if err := hb.Unmarshal(data); err != nil {
		panic(err)
	}
	if !s.updateProxyViewMetadataAdmission(hb) {
		return s.attachViewMetadataAdmission(sm.Result{}, hb.UUID, true)
	}
	return s.attachViewMetadataAdmission(s.getCommandBatch(hb.UUID), hb.UUID, true)
}

func (s *stateMachine) handleUpdateNonVotingReplicaNum(cmd []byte) sm.Result {
	s.state.NonVotingReplicaNum = parseUpdateNonVotingReplicaNumCmd(cmd)
	return sm.Result{}
}

func (s *stateMachine) handleUpdateNonVotingLocality(cmd []byte) sm.Result {
	locality := parseUpdateNonVotingLocalityCmd(cmd)
	for k, v := range locality.Value {
		if v == "" {
			delete(locality.Value, k)
		}
	}
	s.state.NonVotingLocality = locality
	return sm.Result{}
}

func (s *stateMachine) handleLogShardUpdate(cmd []byte) sm.Result {
	addLogShard := parseLogShardUpdateCmd(cmd)
	_, ok := s.state.LogState.Shards[addLogShard.ShardID]
	if !ok {
		s.state.LogState.Shards[addLogShard.ShardID] = pb.LogShardInfo{
			ShardID: addLogShard.ShardID,
		}
		var exists bool
		var numOfLogReplicas uint64
		for _, logShardRec := range s.state.ClusterInfo.LogShards {
			numOfLogReplicas = logShardRec.NumberOfReplicas
			if logShardRec.ShardID == addLogShard.ShardID {
				exists = true
				break
			}
		}
		if !exists {
			s.state.ClusterInfo.LogShards = append(
				s.state.ClusterInfo.LogShards,
				metadata.LogShardRecord{
					ShardID:          addLogShard.ShardID,
					NumberOfReplicas: numOfLogReplicas,
				},
			)
		}
	}
	return sm.Result{}
}

// FIXME: NextID should be set to K8SIDRangeEnd once HAKeeper state is
// set to HAKeeperBootstrapping.
func (s *stateMachine) handleInitialClusterRequestCmd(cmd []byte) sm.Result {
	result := sm.Result{Value: uint64(s.state.State)}
	if s.state.State != pb.HAKeeperCreated {
		return result
	}
	req := parseInitialClusterRequestCmd(cmd)

	// The number of TN shard should only be 1.
	// There is one corresponding Log shard with that TN shard.
	// If there is more than one Log shard, to be exact, two Log shards,
	// the second one is used to save data related with S3. The data in
	// the second shard comes from the first one, but only related with S3.
	if req.NumOfTNShards != 1 {
		panic("only support 1 dn shards")
	}

	tnShards := make([]metadata.TNShardRecord, 0, 1)
	logShards := make([]metadata.LogShardRecord, 0)
	// HAKeeper shard is assigned ShardID 0
	rec := metadata.LogShardRecord{
		ShardID:          0,
		NumberOfReplicas: req.NumOfLogReplicas,
	}
	logShards = append(logShards, rec)

	s.state.NextID++
	tnShardAppended := false
	for i := uint64(0); i < req.NumOfLogShards; i++ {
		rec := metadata.LogShardRecord{
			ShardID:          s.state.NextID,
			NumberOfReplicas: req.NumOfLogReplicas,
		}
		s.state.NextID++
		logShards = append(logShards, rec)

		if tnShardAppended {
			continue
		}

		drec := metadata.TNShardRecord{
			ShardID:    s.state.NextID,
			LogShardID: rec.ShardID,
		}
		s.state.NextID++
		tnShards = append(tnShards, drec)
		tnShardAppended = true
	}
	s.state.ClusterInfo = pb.ClusterInfo{
		TNShards:  tnShards,
		LogShards: logShards,
	}

	// make sure we are not using the ID range assigned to k8s
	if s.state.NextID > K8SIDRangeStart {
		panic("too many IDs assigned during initial cluster request")
	}
	if req.NextID > K8SIDRangeEnd {
		s.state.NextID = req.NextID
	} else {
		s.state.NextID = K8SIDRangeEnd
	}
	if len(req.NextIDByKey) > 0 {
		s.state.NextIDByKey = req.NextIDByKey
	}

	s.state.NonVotingLocality.Value = req.NonVotingLocality
	if req.LogServiceRecovery {
		s.state.LogServiceRecoveryPending = true
		s.state.LogServiceRecoveryCompleted = false
		if req.NextID != 0 || len(req.NextIDByKey) != 0 {
			s.state.LogServiceRecoveryPrepared = true
			if s.state.IDWatermarkRestoreGeneration < math.MaxUint64 {
				s.state.IDWatermarkRestoreGeneration++
			}
		}
	}

	plog.Infof("initial cluster set, HAKeeper is in BOOTSTRAPPING state")
	s.state.State = pb.HAKeeperBootstrapping
	return result
}

func (s *stateMachine) handleRestoreIDWatermarkCmd(cmd []byte) sm.Result {
	result := sm.Result{Value: uint64(s.state.State)}
	if parseCmdTag(cmd) != pb.RestoreIDWatermarkUpdate {
		panic("not a restore ID watermark update")
	}
	var req pb.RestoreIDWatermarkRequest
	if err := req.Unmarshal(cmd[headerSize:]); err != nil {
		panic(err)
	}
	if s.state.State == pb.HAKeeperCreated {
		return result
	}
	// Watermark restore is only valid while a fresh cluster is bootstrapping.
	// Once Running, managed clients may hold ID batches that cannot be revoked.
	if s.state.State != pb.HAKeeperBootstrapping &&
		s.state.State != pb.HAKeeperBootstrapCommandsReceived {
		result.Data = []byte{1}
		return result
	}
	if !s.state.LogServiceRecoveryPending {
		// Recovery intent must be part of InitialClusterUpdate on every initial
		// HAKeeper member. A late request cannot safely retract IDs or bootstrap
		// commands that may already have escaped.
		result.Data = []byte{1}
		return result
	}

	updated := false
	if req.NextID > s.state.NextID {
		s.state.NextID = req.NextID
		updated = true
	}
	if len(req.NextIDByKey) > 0 {
		if s.state.NextIDByKey == nil {
			s.state.NextIDByKey = make(map[string]uint64)
		}
		for key, nextID := range req.NextIDByKey {
			if nextID > s.state.NextIDByKey[key] {
				s.state.NextIDByKey[key] = nextID
				updated = true
			}
		}
	}
	if updated {
		plog.Infof("patched HAKeeper ID watermarks from restore data, next-id %d, next-id-by-key %v",
			s.state.NextID, s.state.NextIDByKey)
	}
	prepareRecovery := req.LogServiceRecovery && !s.state.LogServiceRecoveryPrepared
	if req.LogServiceRecovery {
		s.state.LogServiceRecoveryPrepared = true
	}
	if (updated || prepareRecovery) && s.state.IDWatermarkRestoreGeneration < math.MaxUint64 {
		s.state.IDWatermarkRestoreGeneration++
	}
	return result
}

func (s *stateMachine) handleCompleteLogServiceRecoveryCmd() sm.Result {
	result := sm.Result{Value: uint64(s.state.State)}
	if s.state.LogServiceRecoveryCompleted {
		return result
	}
	if s.state.State == pb.HAKeeperCreated ||
		!s.state.LogServiceRecoveryPending ||
		!s.state.LogServiceRecoveryPrepared {
		result.Data = []byte{1}
		return result
	}
	s.state.LogServiceRecoveryPending = false
	s.state.LogServiceRecoveryPrepared = false
	s.state.LogServiceRecoveryCompleted = true
	return result
}

func (s *stateMachine) assertState() {
	if s.state.State != pb.HAKeeperRunning && s.state.State != pb.HAKeeperBootstrapping {
		panic(fmt.Sprintf("HAKeeper not in the running state, in %s", s.state.State.String()))
	}
}

func (s *stateMachine) Update(e sm.Entry) (sm.Result, error) {
	// TODO: we need to make sure InitialClusterRequestCmd is the
	// first user cmd added to the Raft log
	cmd := e.Cmd
	s.state.Index = e.Index
	switch parseCmdTag(cmd) {
	case pb.TNHeartbeatUpdate:
		return s.handleTNHeartbeat(cmd), nil
	case pb.CNHeartbeatUpdate:
		return s.handleCNHeartbeat(cmd), nil
	case pb.LogHeartbeatUpdate:
		return s.handleLogHeartbeat(cmd), nil
	case pb.TickUpdate:
		return s.handleTick(cmd), nil
	case pb.GetIDUpdate:
		if (s.state.LogServiceRecoveryPending && !s.state.LogServiceRecoveryPrepared) ||
			(s.state.State != pb.HAKeeperRunning && s.state.State != pb.HAKeeperBootstrapping) {
			// ID allocation is an external request. Reject it deterministically
			// while bootstrap commands are being applied instead of panicking the
			// replicated state machine.
			return sm.Result{}, nil
		}
		return s.handleGetIDCmd(cmd), nil
	case pb.ScheduleCommandUpdate:
		return s.handleUpdateCommandsCmd(cmd), nil
	case pb.SetStateUpdate:
		return s.handleSetStateCmd(cmd), nil
	case pb.SetTaskSchedulerStateUpdate:
		s.assertState()
		return s.handleSetTaskSchedulerStateUpdateCmd(cmd), nil
	case pb.InitialClusterUpdate:
		return s.handleInitialClusterRequestCmd(cmd), nil
	case pb.RestoreIDWatermarkUpdate:
		return s.handleRestoreIDWatermarkCmd(cmd), nil
	case pb.CompleteLogServiceRecoveryUpdate:
		return s.handleCompleteLogServiceRecoveryCmd(), nil
	case pb.EnableCommandDeliveryUpdate:
		return s.handleEnableCommandDelivery(cmd), nil
	case pb.EnableViewMetadataAdmissionUpdate:
		return s.handleEnableViewMetadataAdmission(cmd), nil
	case pb.SetTaskTableUserUpdate:
		s.assertState()
		return s.handleTaskTableUserCmd(cmd), nil
	case pb.UpdateCNLabel:
		return s.handleUpdateCNLabel(cmd), nil
	case pb.UpdateCNWorkState:
		return s.handleUpdateCNWorkState(cmd), nil
	case pb.PatchCNStore:
		return s.handlePatchCNStore(cmd), nil
	case pb.RemoveCNStore:
		return s.handleDeleteCNCmd(parseDeleteCNStoreCmd(cmd).StoreID), nil
	case pb.ProxyHeartbeatUpdate:
		return s.handleProxyHeartbeat(cmd), nil
	case pb.UpdateNonVotingReplicaNum:
		return s.handleUpdateNonVotingReplicaNum(cmd), nil
	case pb.UpdateNonVotingLocality:
		return s.handleUpdateNonVotingLocality(cmd), nil
	case pb.LogShardUpdate:
		return s.handleLogShardUpdate(cmd), nil
	default:
		panic(moerr.NewInvalidInputNoCtxf("unknown haKeeper cmd '%v'", cmd))
	}
}

func (s *stateMachine) handleStateQuery() interface{} {
	internal := &pb.CheckerState{
		Tick:                         s.state.Tick,
		ClusterInfo:                  s.state.ClusterInfo,
		TNState:                      s.state.TNState,
		LogState:                     s.state.LogState,
		CNState:                      s.state.CNState,
		ProxyState:                   s.state.ProxyState,
		State:                        s.state.State,
		TaskSchedulerState:           s.state.TaskSchedulerState,
		TaskTableUser:                s.state.TaskTableUser,
		NextId:                       s.state.NextID,
		NextIDByKey:                  s.state.NextIDByKey,
		NonVotingReplicaNum:          s.state.NonVotingReplicaNum,
		NonVotingLocality:            s.state.NonVotingLocality,
		LogServiceRecoveryPending:    s.state.LogServiceRecoveryPending,
		IDWatermarkRestoreGeneration: s.state.IDWatermarkRestoreGeneration,
		LogServiceRecoveryPrepared:   s.state.LogServiceRecoveryPrepared,
		LogServiceRecoveryCompleted:  s.state.LogServiceRecoveryCompleted,
	}
	copied := deepcopy.Copy(internal)
	result, ok := copied.(*pb.CheckerState)
	if !ok {
		panic("deep copy failed")
	}
	return result
}

func (s *stateMachine) handleScheduleCommandQuery(uuid string) *pb.CommandBatch {
	if batch, ok := s.state.ScheduleCommands[uuid]; ok {
		copied := deepcopy.Copy(&batch)
		result, ok := copied.(*pb.CommandBatch)
		if !ok {
			panic("deep copy failed")
		}
		return result
	}
	return &pb.CommandBatch{}
}

func (s *stateMachine) handleClusterDetailsQuery(cfg Config) *pb.ClusterDetails {
	cfg.Fill()
	cd := &pb.ClusterDetails{
		CNStores:    make([]pb.CNStore, 0, len(s.state.CNState.Stores)),
		TNStores:    make([]pb.TNStore, 0, len(s.state.TNState.Stores)),
		LogStores:   make([]pb.LogStore, 0, len(s.state.LogState.Stores)),
		ProxyStores: make([]pb.ProxyStore, 0, len(s.state.ProxyState.Stores)),
	}
	if s.viewMetadataAdmissionActive() {
		cd.ViewMetadataAdmission = &pb.ViewMetadataAdmission{
			Preparing:            s.state.ViewMetadataAdmissionPreparing,
			Enabled:              s.state.ViewMetadataAdmissionEnabled,
			Epoch:                s.state.ViewMetadataAdmissionEpoch,
			RevalidationRequired: s.state.ViewMetadataRevalidationRequired,
			CatalogFencedEpoch:   s.state.ViewMetadataCatalogFencedEpoch,
		}
	}
	for uuid, info := range s.state.CNState.Stores {
		if s.viewMetadataAdmissionActive() &&
			!info.ViewMetadataAdmissionReady &&
			(!info.ViewMetadataAdmissionSupported ||
				info.ViewMetadataAdmissionGeneration == 0) {
			continue
		}
		state := pb.NormalState
		if cfg.CNStoreExpired(info.Tick, s.state.Tick) {
			state = pb.TimeoutState
		}
		n := pb.CNStore{
			UUID:                            uuid,
			Tick:                            info.Tick,
			ServiceAddress:                  info.ServiceAddress,
			SQLAddress:                      info.SQLAddress,
			LockServiceAddress:              info.LockServiceAddress,
			ShardServiceAddress:             info.ShardServiceAddress,
			State:                           state,
			WorkState:                       info.WorkState,
			Labels:                          info.Labels,
			QueryAddress:                    info.QueryAddress,
			ConfigData:                      info.ConfigData,
			Resource:                        info.Resource,
			UpTime:                          info.UpTime,
			CommitID:                        info.CommitID,
			ViewMetadataAdmissionSupported:  info.ViewMetadataAdmissionSupported,
			ViewMetadataAdmissionGeneration: info.ViewMetadataAdmissionGeneration,
			ViewMetadataObservedEpoch:       info.ViewMetadataObservedEpoch,
			ViewMetadataAdmissionReady:      info.ViewMetadataAdmissionReady,
		}
		cd.CNStores = append(cd.CNStores, n)
	}
	for uuid, info := range s.state.TNState.Stores {
		state := pb.NormalState
		if cfg.TNStoreExpired(info.Tick, s.state.Tick) {
			state = pb.TimeoutState
		}
		n := pb.TNStore{
			UUID:                        uuid,
			Tick:                        info.Tick,
			State:                       state,
			ServiceAddress:              info.ServiceAddress,
			Shards:                      info.Shards,
			LogtailServerAddress:        info.LogtailServerAddress,
			LockServiceAddress:          info.LockServiceAddress,
			ShardServiceAddress:         info.ShardServiceAddress,
			ConfigData:                  info.ConfigData,
			QueryAddress:                info.QueryAddress,
			AutoIncrEpochFenceSupported: info.AutoIncrEpochFenceSupported,
		}
		cd.TNStores = append(cd.TNStores, n)
	}
	for uuid, info := range s.state.LogState.Stores {
		state := pb.NormalState
		if cfg.LogStoreExpired(info.Tick, s.state.Tick) {
			state = pb.TimeoutState
		}
		n := pb.LogStore{
			UUID:           uuid,
			Tick:           info.Tick,
			State:          state,
			ServiceAddress: info.ServiceAddress,
			Replicas:       info.Replicas,
			ConfigData:     info.ConfigData,
			Locality:       info.Locality,
		}
		cd.LogStores = append(cd.LogStores, n)
	}
	for uuid, info := range s.state.ProxyState.Stores {
		if s.viewMetadataAdmissionActive() &&
			!info.ViewMetadataAdmissionReady &&
			(!info.ViewMetadataAdmissionSupported ||
				info.ViewMetadataAdmissionGeneration == 0) {
			continue
		}
		cd.ProxyStores = append(cd.ProxyStores, pb.ProxyStore{
			UUID:                            uuid,
			Tick:                            info.Tick,
			ListenAddress:                   info.ListenAddress,
			ConfigData:                      info.ConfigData,
			ViewMetadataAdmissionSupported:  info.ViewMetadataAdmissionSupported,
			ViewMetadataAdmissionGeneration: info.ViewMetadataAdmissionGeneration,
			ViewMetadataObservedEpoch:       info.ViewMetadataObservedEpoch,
			ViewMetadataAdmissionReady:      info.ViewMetadataAdmissionReady,
		})
	}
	for _, store := range s.state.DeletedStores {
		cd.DeletedStores = append(cd.DeletedStores, pb.DeletedStore{
			UUID:      store.UUID,
			StoreType: store.StoreType,
			Address:   store.Address,
			UpTime:    store.UpTime,
			DownTime:  store.DownTime,
		})
	}
	return cd
}

func (s *stateMachine) handleUpdateCNLabel(cmd []byte) sm.Result {
	s.state.CNState.UpdateLabel(parseUpdateCNLabelCmd(cmd))
	return sm.Result{}
}

func (s *stateMachine) handleUpdateCNWorkState(cmd []byte) sm.Result {
	s.state.CNState.UpdateWorkState(parseUpdateCNWorkStateCmd(cmd))
	return sm.Result{}
}

func (s *stateMachine) handlePatchCNStore(cmd []byte) sm.Result {
	s.state.CNState.PatchCNStore(parsePatchCNStoreCmd(cmd))
	return sm.Result{}
}

func (s *stateMachine) Lookup(query interface{}) (interface{}, error) {
	if _, ok := query.(*StateQuery); ok {
		return s.handleStateQuery(), nil
	} else if q, ok := query.(*ScheduleCommandQuery); ok {
		return s.handleScheduleCommandQuery(q.UUID), nil
	} else if _, ok := query.(*CommandDeliveryStateQuery); ok {
		admissionReady := true
		if !s.state.CommandDeliveryPreparing && !s.state.CommandDeliveryEnabled {
			admissionReady = !s.hasPendingHAKeeperAdmission()
		}
		ready := make(map[string]bool, len(s.state.CommandDeliveryReady))
		for uuid, value := range s.state.CommandDeliveryReady {
			ready[uuid] = value
		}
		var cnReady map[string]bool
		if s.state.CommandDeliveryCNReady != nil {
			cnReady = make(map[string]bool, len(s.state.CommandDeliveryCNReady))
			for uuid, value := range s.state.CommandDeliveryCNReady {
				cnReady[uuid] = value
			}
		}
		var tnReady map[string]bool
		if s.state.CommandDeliveryTNReady != nil {
			tnReady = make(map[string]bool, len(s.state.CommandDeliveryTNReady))
			for uuid, value := range s.state.CommandDeliveryTNReady {
				tnReady[uuid] = value
			}
		}
		return CommandDeliveryState{
			Preparing:              s.state.CommandDeliveryPreparing,
			Enabled:                s.state.CommandDeliveryEnabled,
			HAKeeperAdmissionReady: admissionReady,
			Ready:                  ready,
			CNReady:                cnReady,
			TNReady:                tnReady,
		}, nil
	} else if _, ok := query.(*ViewMetadataAdmissionStateQuery); ok {
		var logReady map[string]bool
		if s.state.ViewMetadataAdmissionLogReady != nil {
			logReady = make(map[string]bool, len(s.state.ViewMetadataAdmissionLogReady))
			for uuid, ready := range s.state.ViewMetadataAdmissionLogReady {
				logReady[uuid] = ready
			}
		}
		var cnReady map[string]bool
		if s.state.ViewMetadataAdmissionCNReady != nil {
			cnReady = make(map[string]bool, len(s.state.ViewMetadataAdmissionCNReady))
			for uuid, ready := range s.state.ViewMetadataAdmissionCNReady {
				cnReady[uuid] = ready
			}
		}
		var proxyReady map[string]bool
		if s.state.ViewMetadataAdmissionProxyReady != nil {
			proxyReady = make(map[string]bool, len(s.state.ViewMetadataAdmissionProxyReady))
			for uuid, ready := range s.state.ViewMetadataAdmissionProxyReady {
				proxyReady[uuid] = ready
			}
		}
		return ViewMetadataAdmissionState{
			Preparing:              s.state.ViewMetadataAdmissionPreparing,
			Enabled:                s.state.ViewMetadataAdmissionEnabled,
			Pending:                s.state.ViewMetadataAdmissionPending,
			HAKeeperAdmissionReady: !s.hasPendingHAKeeperAdmission(),
			LogReady:               logReady,
			CNReady:                cnReady,
			ProxyReady:             proxyReady,
		}, nil
	} else if q, ok := query.(*ClusterDetailsQuery); ok {
		return s.handleClusterDetailsQuery(q.Cfg), nil
	} else if _, ok := query.(*IndexQuery); ok {
		return s.state.Index, nil
	}
	panic("unknown query type")
}

func (s *stateMachine) SaveSnapshot(w io.Writer,
	_ sm.ISnapshotFileCollection, _ <-chan struct{}) error {
	// FIXME: memory recycling when necessary
	data := make([]byte, s.state.ProtoSize())
	n, err := s.state.MarshalToSizedBuffer(data)
	if err != nil {
		return err
	}
	_, err = w.Write(data[:n])
	return err
}

func (s *stateMachine) RecoverFromSnapshot(r io.Reader,
	_ []sm.SnapshotFile, _ <-chan struct{}) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	// The state machine is initialized with maps for normal operation. Clear all
	// delivery fields before decoding so an older snapshot, or a snapshot
	// recovery on a reused instance, cannot retain a newer barrier generation or
	// the one-time BatchID scan marker when those fields are absent on disk.
	s.state.CommandDeliveryEnabled = false
	s.state.CommandDeliveryPreparing = false
	s.state.CommandDeliveryReady = nil
	s.state.CommandDeliveryCNReady = nil
	s.state.CommandDeliveryTNReady = nil
	s.state.CommandDeliveryBatchIDsAssigned = false
	s.state.CommandDeliveryCommandIDsAssigned = false
	s.state.ViewMetadataAdmissionPreparing = false
	s.state.ViewMetadataAdmissionEnabled = false
	s.state.ViewMetadataAdmissionEpoch = 0
	s.state.ViewMetadataRevalidationRequired = false
	s.state.ViewMetadataCatalogFencedEpoch = 0
	s.state.ViewMetadataAdmissionLogReady = nil
	s.state.ViewMetadataAdmissionCNReady = nil
	s.state.ViewMetadataAdmissionProxyReady = nil
	s.state.ViewMetadataAdmissionCNTargets = nil
	s.state.ViewMetadataAdmissionProxyTargets = nil
	s.state.ViewMetadataAdmissionPending = false
	return s.state.Unmarshal(data)
}
