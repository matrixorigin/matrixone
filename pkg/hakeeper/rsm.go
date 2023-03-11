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

func GetInitialClusterRequestCmd(numOfLogShards uint64,
	numOfDNShards uint64, numOfLogReplicas uint64) []byte {
	req := pb.InitialClusterRequest{
		NumOfLogShards:   numOfLogShards,
		NumOfDNShards:    numOfDNShards,
		NumOfLogReplicas: numOfLogReplicas,
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
	data := make([]byte, headerSize+b.Size())
	binaryEnc.PutUint32(data, uint32(pb.ScheduleCommandUpdate))
	if _, err := b.MarshalTo(data[headerSize:]); err != nil {
		panic(err)
	}
	return data
}

func GetGetIDCmd(count uint64) []byte {
	cmd := make([]byte, headerSize+8)
	binaryEnc.PutUint32(cmd, uint32(pb.GetIDUpdate))
	binaryEnc.PutUint64(cmd[headerSize:], count)
	return cmd
}

func parseHeartbeatCmd(cmd []byte) []byte {
	return cmd[headerSize:]
}

func parseGetIDCmd(cmd []byte) uint64 {
	return binaryEnc.Uint64(cmd[headerSize:])
}

func parseSetStateCmd(cmd []byte) pb.HAKeeperState {
	return pb.HAKeeperState(binaryEnc.Uint32(cmd[headerSize:]))
}

func parseSetInitTaskStateCmd(cmd []byte) pb.TaskSchedulerState {
	return pb.TaskSchedulerState(binaryEnc.Uint32(cmd[headerSize:]))
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
	cmd := make([]byte, headerSize+user.Size())
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

func GetDNStoreHeartbeatCmd(data []byte) []byte {
	return getHeartbeatCmd(data, pb.DNHeartbeatUpdate)
}

func getHeartbeatCmd(data []byte, tag pb.HAKeeperUpdateType) []byte {
	cmd := make([]byte, headerSize+len(data))
	binaryEnc.PutUint32(cmd, uint32(tag))
	copy(cmd[headerSize:], data)
	return cmd
}

func NewStateMachine(shardID uint64, replicaID uint64) sm.IStateMachine {
	if shardID != DefaultHAKeeperShardID {
		panic(moerr.NewInvalidInputNoCtx("HAKeeper shard ID %d does not match DefaultHAKeeperShardID %d", shardID, DefaultHAKeeperShardID))
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

func (s *stateMachine) handleDNHeartbeat(cmd []byte) sm.Result {
	data := parseHeartbeatCmd(cmd)
	var hb pb.DNStoreHeartbeat
	if err := hb.Unmarshal(data); err != nil {
		panic(err)
	}
	s.state.DNState.Update(hb, s.state.Tick)
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
	count := parseGetIDCmd(cmd)
	s.state.NextID++
	v := s.state.NextID
	s.state.NextID += count - 1
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
	delete(s.state.CNState.Stores, uuid)
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
	if req.NumOfLogShards != req.NumOfDNShards {
		panic("DN:Log 1:1 mode is the only supported mode")
	}

	dnShards := make([]metadata.DNShardRecord, 0)
	logShards := make([]metadata.LogShardRecord, 0)
	// HAKeeper shard is assigned ShardID 0
	rec := metadata.LogShardRecord{
		ShardID:          0,
		NumberOfReplicas: req.NumOfLogReplicas,
	}
	logShards = append(logShards, rec)

	s.state.NextID++
	for i := uint64(0); i < req.NumOfLogShards; i++ {
		rec := metadata.LogShardRecord{
			ShardID:          s.state.NextID,
			NumberOfReplicas: req.NumOfLogReplicas,
		}
		s.state.NextID++
		logShards = append(logShards, rec)

		drec := metadata.DNShardRecord{
			ShardID:    s.state.NextID,
			LogShardID: rec.ShardID,
		}
		s.state.NextID++
		dnShards = append(dnShards, drec)
	}
	s.state.ClusterInfo = pb.ClusterInfo{
		DNShards:  dnShards,
		LogShards: logShards,
	}

	// make sure we are not using the ID range assigned to k8s
	if s.state.NextID > K8SIDRangeStart {
		panic("too many IDs assigned during initial cluster request")
	}
	s.state.NextID = K8SIDRangeEnd

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
	switch parseCmdTag(cmd) {
	case pb.DNHeartbeatUpdate:
		return s.handleDNHeartbeat(cmd), nil
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
	default:
		panic(moerr.NewInvalidInputNoCtx("unknown haKeeper cmd '%v'", cmd))
	}
}

func (s *stateMachine) handleStateQuery() interface{} {
	internal := &pb.CheckerState{
		Tick:               s.state.Tick,
		ClusterInfo:        s.state.ClusterInfo,
		DNState:            s.state.DNState,
		LogState:           s.state.LogState,
		CNState:            s.state.CNState,
		State:              s.state.State,
		TaskSchedulerState: s.state.TaskSchedulerState,
		TaskTableUser:      s.state.TaskTableUser,
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
		CNStores:  make([]pb.CNStore, 0, len(s.state.CNState.Stores)),
		DNStores:  make([]pb.DNStore, 0, len(s.state.DNState.Stores)),
		LogStores: make([]pb.LogStore, 0, len(s.state.LogState.Stores)),
	}
	for uuid, info := range s.state.CNState.Stores {
		state := pb.NormalState
		if cfg.CNStoreExpired(info.Tick, s.state.Tick) {
			state = pb.TimeoutState
		}
		n := pb.CNStore{
			UUID:               uuid,
			Tick:               info.Tick,
			ServiceAddress:     info.ServiceAddress,
			SQLAddress:         info.SQLAddress,
			LockServiceAddress: info.LockServiceAddress,
			State:              state,
		}
		cd.CNStores = append(cd.CNStores, n)
	}
	for uuid, info := range s.state.DNState.Stores {
		state := pb.NormalState
		if cfg.DNStoreExpired(info.Tick, s.state.Tick) {
			state = pb.TimeoutState
		}
		n := pb.DNStore{
			UUID:                 uuid,
			Tick:                 info.Tick,
			State:                state,
			ServiceAddress:       info.ServiceAddress,
			Shards:               info.Shards,
			LogtailServerAddress: info.LogtailServerAddress,
			LockServiceAddress:   info.LockServiceAddress,
		}
		cd.DNStores = append(cd.DNStores, n)
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
		}
		cd.LogStores = append(cd.LogStores, n)
	}
	return cd
}

func (s *stateMachine) Lookup(query interface{}) (interface{}, error) {
	if _, ok := query.(*StateQuery); ok {
		return s.handleStateQuery(), nil
	} else if q, ok := query.(*ScheduleCommandQuery); ok {
		return s.handleScheduleCommandQuery(q.UUID), nil
	} else if q, ok := query.(*ClusterDetailsQuery); ok {
		return s.handleClusterDetailsQuery(q.Cfg), nil
	}
	panic("unknown query type")
}

func (s *stateMachine) SaveSnapshot(w io.Writer,
	_ sm.ISnapshotFileCollection, _ <-chan struct{}) error {
	// FIXME: memory recycling when necessary
	data := make([]byte, s.state.Size())
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
