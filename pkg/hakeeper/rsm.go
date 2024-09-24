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
	"strings"
	"time"

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
type ClusterDetailsQuery struct{ Cfg Config }

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
	req := pb.InitialClusterRequest{
		NumOfLogShards:    numOfLogShards,
		NumOfTNShards:     numOfTNShards,
		NumOfLogReplicas:  numOfLogReplicas,
		NextID:            nextID,
		NextIDByKey:       nextIDByKey,
		NonVotingLocality: nonVotingLocality,
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

func (s *stateMachine) handleUpdateCommandsCmd(cmd []byte) sm.Result {
	data := cmd[headerSize:]
	var b pb.CommandBatch
	if err := b.Unmarshal(data); err != nil {
		panic(err)
	}
	if s.state.Term > b.Term {
		return sm.Result{}
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
	s.state.ScheduleCommands = make(map[string]pb.CommandBatch)
	for _, c := range b.Commands {
		if c.Bootstrapping {
			s.handleSetStateCmd(GetSetStateCmd(pb.HAKeeperBootstrapCommandsReceived))
		}
		if c.DeleteCNStore != nil {
			s.handleDeleteCNCmd(c.UUID)
			continue
		}
		if c.DeleteProxyStore != nil {
			s.handleDeleteProxyCmd(c.UUID)
			continue
		}
		l, ok := s.state.ScheduleCommands[c.UUID]
		if !ok {
			l = pb.CommandBatch{
				Commands: make([]pb.ScheduleCommand, 0),
			}
		}
		plog.Infof("adding schedule command to hakeeper rsm: %s", c.LogString())
		l.Commands = append(l.Commands, c)
		s.state.ScheduleCommands[c.UUID] = l
	}

	return sm.Result{}
}

func (s *stateMachine) getCommandBatch(uuid string) sm.Result {
	if batch, ok := s.state.ScheduleCommands[uuid]; ok {
		delete(s.state.ScheduleCommands, uuid)
		data, err := batch.Marshal()
		if err != nil {
			panic(err)
		}
		return sm.Result{Data: data}
	}
	return sm.Result{}

}

func (s *stateMachine) handleCNHeartbeat(cmd []byte) sm.Result {
	data := parseHeartbeatCmd(cmd)
	var hb pb.CNStoreHeartbeat
	if err := hb.Unmarshal(data); err != nil {
		panic(err)
	}
	s.state.CNState.Update(hb, s.state.Tick)
	return s.getCommandBatch(hb.UUID)
}

func (s *stateMachine) handleTNHeartbeat(cmd []byte) sm.Result {
	data := parseHeartbeatCmd(cmd)
	var hb pb.TNStoreHeartbeat
	if err := hb.Unmarshal(data); err != nil {
		panic(err)
	}
	s.state.TNState.Update(hb, s.state.Tick)
	return s.getCommandBatch(hb.UUID)
}

func (s *stateMachine) handleLogHeartbeat(cmd []byte) sm.Result {
	data := parseHeartbeatCmd(cmd)
	var hb pb.LogStoreHeartbeat
	if err := hb.Unmarshal(data); err != nil {
		panic(err)
	}
	s.state.LogState.Update(hb, s.state.Tick)
	return s.getCommandBatch(hb.UUID)
}

func (s *stateMachine) handleTick(cmd []byte) sm.Result {
	s.state.Tick++
	return sm.Result{}
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
	s.state.ProxyState.Update(hb, s.state.Tick)
	return s.getCommandBatch(hb.UUID)
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

	plog.Infof("initial cluster set, HAKeeper is in BOOTSTRAPPING state")
	s.state.State = pb.HAKeeperBootstrapping
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
		s.assertState()
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
		Tick:                s.state.Tick,
		ClusterInfo:         s.state.ClusterInfo,
		TNState:             s.state.TNState,
		LogState:            s.state.LogState,
		CNState:             s.state.CNState,
		ProxyState:          s.state.ProxyState,
		State:               s.state.State,
		TaskSchedulerState:  s.state.TaskSchedulerState,
		TaskTableUser:       s.state.TaskTableUser,
		NextId:              s.state.NextID,
		NextIDByKey:         s.state.NextIDByKey,
		NonVotingReplicaNum: s.state.NonVotingReplicaNum,
		NonVotingLocality:   s.state.NonVotingLocality,
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
		return &batch
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
	for uuid, info := range s.state.CNState.Stores {
		state := pb.NormalState
		if cfg.CNStoreExpired(info.Tick, s.state.Tick) {
			state = pb.TimeoutState
		}
		n := pb.CNStore{
			UUID:                uuid,
			Tick:                info.Tick,
			ServiceAddress:      info.ServiceAddress,
			SQLAddress:          info.SQLAddress,
			LockServiceAddress:  info.LockServiceAddress,
			ShardServiceAddress: info.ShardServiceAddress,
			State:               state,
			WorkState:           info.WorkState,
			Labels:              info.Labels,
			QueryAddress:        info.QueryAddress,
			ConfigData:          info.ConfigData,
			Resource:            info.Resource,
			UpTime:              info.UpTime,
			CommitID:            info.CommitID,
		}
		cd.CNStores = append(cd.CNStores, n)
	}
	for uuid, info := range s.state.TNState.Stores {
		state := pb.NormalState
		if cfg.TNStoreExpired(info.Tick, s.state.Tick) {
			state = pb.TimeoutState
		}
		n := pb.TNStore{
			UUID:                 uuid,
			Tick:                 info.Tick,
			State:                state,
			ServiceAddress:       info.ServiceAddress,
			Shards:               info.Shards,
			LogtailServerAddress: info.LogtailServerAddress,
			LockServiceAddress:   info.LockServiceAddress,
			ShardServiceAddress:  info.ShardServiceAddress,
			ConfigData:           info.ConfigData,
			QueryAddress:         info.QueryAddress,
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
		cd.ProxyStores = append(cd.ProxyStores, pb.ProxyStore{
			UUID:          uuid,
			Tick:          info.Tick,
			ListenAddress: info.ListenAddress,
			ConfigData:    info.ConfigData,
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
	return s.state.Unmarshal(data)
}
