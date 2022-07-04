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

package hakeeper

import (
	"encoding/binary"
	"encoding/gob"
	"io"
	"time"

	"github.com/lni/dragonboat/v4/logger"
	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

var (
	plog = logger.GetLogger("hakeeper")
)

var (
	binaryEnc = binary.BigEndian
)

const (
	// TickDuration defines the frequency of ticks.
	TickDuration = time.Second
	// CheckDuration defines how often HAKeeper checks the health state of the cluster
	CheckDuration = 2 * time.Second
	// DefaultHAKeeperShardID is the shard ID assigned to the special HAKeeper
	// shard.
	DefaultHAKeeperShardID uint64 = 0
	headerSize                    = 2
)

const (
	createLogShardTag uint16 = iota + 0xAE01
	tickTag
	dnHeartbeatTag
	logHeartbeatTag
	getIDTag
	updateScheduleCommandTag
)

type StateQuery struct{}
type logShardIDQuery struct{ name string }
type logShardIDQueryResult struct{ id uint64 }
type ScheduleCommandQuery struct{ UUID string }

type stateMachine struct {
	replicaID uint64
	state     pb.HAKeeperRSMState
}

func parseCmdTag(cmd []byte) uint16 {
	return binaryEnc.Uint16(cmd)
}

func GetUpdateCommandsCmd(term uint64, cmds []pb.ScheduleCommand) []byte {
	b := pb.CommandBatch{
		Term:     term,
		Commands: cmds,
	}
	data := make([]byte, headerSize+b.Size())
	binaryEnc.PutUint16(data, updateScheduleCommandTag)
	if _, err := b.MarshalTo(data[headerSize:]); err != nil {
		panic(err)
	}
	return data
}

func isUpdateCommandsCmd(cmd []byte) bool {
	return parseCmdTag(cmd) == updateScheduleCommandTag
}

func GetGetIDCmd(count uint64) []byte {
	cmd := make([]byte, headerSize+8)
	binaryEnc.PutUint16(cmd, getIDTag)
	binaryEnc.PutUint64(cmd[headerSize:], count)
	return cmd
}

func getCreateLogShardCmd(name string) []byte {
	return getLogShardCmd(name, createLogShardTag)
}

func getLogShardCmd(name string, tag uint16) []byte {
	cmd := make([]byte, headerSize+len(name))
	binaryEnc.PutUint16(cmd, tag)
	copy(cmd[headerSize:], []byte(name))
	return cmd
}

func isCreateLogShardCmd(cmd []byte) (string, bool) {
	return isLogShardCmd(cmd, createLogShardTag)
}

func isDNHeartbeatCmd(cmd []byte) bool {
	return isHeartbeatCmd(cmd, dnHeartbeatTag)
}

func isLogHeartbeatCmd(cmd []byte) bool {
	return isHeartbeatCmd(cmd, logHeartbeatTag)
}

func isHeartbeatCmd(cmd []byte, tag uint16) bool {
	if len(cmd) <= headerSize {
		return false
	}
	return parseCmdTag(cmd) == tag
}

func parseHeartbeatCmd(cmd []byte) []byte {
	return cmd[headerSize:]
}

func isLogShardCmd(cmd []byte, tag uint16) (string, bool) {
	if len(cmd) <= headerSize {
		return "", false
	}
	if parseCmdTag(cmd) == tag {
		return string(cmd[headerSize:]), true
	}
	return "", false
}

func isTickCmd(cmd []byte) bool {
	return len(cmd) == headerSize && binaryEnc.Uint16(cmd) == tickTag
}

func isGetIDCmd(cmd []byte) bool {
	return len(cmd) == headerSize+8 && binaryEnc.Uint16(cmd) == getIDTag
}

func parseGetIDCmd(cmd []byte) uint64 {
	return binaryEnc.Uint64(cmd[headerSize:])
}

func GetTickCmd() []byte {
	cmd := make([]byte, headerSize)
	binaryEnc.PutUint16(cmd, tickTag)
	return cmd
}

func GetLogStoreHeartbeatCmd(data []byte) []byte {
	return getHeartbeatCmd(data, logHeartbeatTag)
}

func GetDNStoreHeartbeatCmd(data []byte) []byte {
	return getHeartbeatCmd(data, dnHeartbeatTag)
}

func getHeartbeatCmd(data []byte, tag uint16) []byte {
	cmd := make([]byte, headerSize+len(data))
	binaryEnc.PutUint16(cmd, tag)
	copy(cmd[headerSize:], data)
	return cmd
}

func NewStateMachine(shardID uint64, replicaID uint64) sm.IStateMachine {
	if shardID != DefaultHAKeeperShardID {
		panic(moerr.NewError(moerr.INVALID_INPUT, "invalid HAKeeper shard ID"))
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
	plog.Infof("incoming term: %d, rsm term: %d", b.Term, s.state.Term)
	if s.state.Term > b.Term {
		return sm.Result{}
	}

	s.state.Term = b.Term
	s.state.ScheduleCommands = make(map[string]pb.CommandBatch)
	for _, c := range b.Commands {
		l, ok := s.state.ScheduleCommands[c.UUID]
		if !ok {
			l = pb.CommandBatch{
				Commands: make([]pb.ScheduleCommand, 0),
			}
		}
		l.Commands = append(l.Commands, c)
		s.state.ScheduleCommands[c.UUID] = l
	}

	return sm.Result{}
}

func (s *stateMachine) handleCreateLogShardCmd(cmd []byte) sm.Result {
	name, ok := isCreateLogShardCmd(cmd)
	if !ok {
		panic(moerr.NewError(moerr.INVALID_INPUT, "not create log shard cmd"))
	}
	if shardID, ok := s.state.LogShards[name]; ok {
		data := make([]byte, 8)
		binaryEnc.PutUint64(data, shardID)
		return sm.Result{Value: 0, Data: data}
	}
	s.state.LogShards[name] = s.assignID()
	return sm.Result{Value: s.state.NextID}
}

func (s *stateMachine) handleDNHeartbeat(cmd []byte) sm.Result {
	data := parseHeartbeatCmd(cmd)
	var hb pb.DNStoreHeartbeat
	if err := hb.Unmarshal(data); err != nil {
		panic(err)
	}
	s.state.DNState.Update(hb, s.state.Tick)
	return sm.Result{}
}

func (s *stateMachine) handleLogHeartbeat(cmd []byte) sm.Result {
	data := parseHeartbeatCmd(cmd)
	var hb pb.LogStoreHeartbeat
	if err := hb.Unmarshal(data); err != nil {
		panic(err)
	}
	s.state.LogState.Update(hb, s.state.Tick)
	return sm.Result{}
}

func (s *stateMachine) handleTick(cmd []byte) sm.Result {
	s.state.Tick++
	return sm.Result{}
}

func (s *stateMachine) handleGetIDCmd(cmd []byte) sm.Result {
	count := parseGetIDCmd(cmd)
	s.state.NextID++
	v := s.state.NextID
	s.state.NextID += (count - 1)
	return sm.Result{Value: v}
}

func (s *stateMachine) Update(e sm.Entry) (sm.Result, error) {
	cmd := e.Cmd
	if _, ok := isCreateLogShardCmd(cmd); ok {
		return s.handleCreateLogShardCmd(cmd), nil
	} else if isDNHeartbeatCmd(cmd) {
		return s.handleDNHeartbeat(cmd), nil
	} else if isLogHeartbeatCmd(cmd) {
		return s.handleLogHeartbeat(cmd), nil
	} else if isTickCmd(cmd) {
		return s.handleTick(cmd), nil
	} else if isGetIDCmd(cmd) {
		return s.handleGetIDCmd(cmd), nil
	} else if isUpdateCommandsCmd(cmd) {
		return s.handleUpdateCommandsCmd(cmd), nil
	}
	panic(moerr.NewError(moerr.INVALID_INPUT, "unexpected haKeeper cmd"))
}

func (s *stateMachine) handleStateQuery() interface{} {
	// FIXME: pretty sure we need to deepcopy here
	return &pb.HAKeeperState{
		Tick:        s.state.Tick,
		ClusterInfo: s.state.ClusterInfo,
		DNState:     s.state.DNState,
		LogState:    s.state.LogState,
	}
}

func (s *stateMachine) handleShardIDQuery(name string) *logShardIDQueryResult {
	id, ok := s.state.LogShards[name]
	if ok {
		return &logShardIDQueryResult{id: id}
	}
	return &logShardIDQueryResult{}
}

func (s *stateMachine) handleScheduleCommandQuery(uuid string) *pb.CommandBatch {
	if batch, ok := s.state.ScheduleCommands[uuid]; ok {
		return &batch
	}
	return &pb.CommandBatch{}
}

func (s *stateMachine) Lookup(query interface{}) (interface{}, error) {
	if q, ok := query.(*logShardIDQuery); ok {
		return s.handleShardIDQuery(q.name), nil
	} else if _, ok := query.(*StateQuery); ok {
		return s.handleStateQuery(), nil
	} else if q, ok := query.(*ScheduleCommandQuery); ok {
		return s.handleScheduleCommandQuery(q.UUID), nil
	}
	panic("unknown query type")
}

func (s *stateMachine) SaveSnapshot(w io.Writer,
	_ sm.ISnapshotFileCollection, _ <-chan struct{}) error {
	// FIXME: ready to use gogoproto to marshal the state, just need to figure
	// out how to write to the writer.
	enc := gob.NewEncoder(w)
	return enc.Encode(s.state)
}

func (s *stateMachine) RecoverFromSnapshot(r io.Reader,
	_ []sm.SnapshotFile, _ <-chan struct{}) error {
	dec := gob.NewDecoder(r)
	return dec.Decode(&s.state)
}
