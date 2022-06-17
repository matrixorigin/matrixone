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
	hapb "github.com/matrixorigin/matrixone/pkg/pb/hakeeper"
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

	headerSize = 2
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

type logShardIDQuery struct {
	name string
}

type logShardIDQueryResult struct {
	id    uint64
	found bool
}

type stateMachine struct {
	replicaID        uint64
	term             uint64
	scheduleCommands map[string][]hapb.ScheduleCommand // keyed by UUID
	Tick             uint64
	NextID           uint64
	LogShards        map[string]uint64 // keyed by Log Shard name
	DNState          DNState
	LogState         LogState
	ClusterInfo      ClusterInfo
}

func parseCmdTag(cmd []byte) uint16 {
	return binaryEnc.Uint16(cmd)
}

func GetUpdateCommandsCmd(term uint64, cmds []hapb.ScheduleCommand) []byte {
	b := hapb.CommandBatch{
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
		replicaID:        replicaID,
		scheduleCommands: make(map[string][]hapb.ScheduleCommand),
		LogShards:        make(map[string]uint64),
		DNState:          NewDNState(),
		LogState:         NewLogState(),
	}
}

func (s *stateMachine) Close() error {
	return nil
}

func (s *stateMachine) assignID() uint64 {
	s.NextID++
	return s.NextID
}

func (s *stateMachine) handleUpdateCommandsCmd(cmd []byte) (sm.Result, error) {
	data := cmd[headerSize:]
	var b hapb.CommandBatch
	if err := b.Unmarshal(data); err != nil {
		panic(err)
	}
	plog.Infof("incoming term: %d, rsm term: %d", b.Term, s.term)
	if s.term > b.Term {
		return sm.Result{}, nil
	}

	s.term = b.Term
	s.scheduleCommands = make(map[string][]hapb.ScheduleCommand)
	for _, c := range b.Commands {
		l, ok := s.scheduleCommands[c.UUID]
		if !ok {
			l = make([]hapb.ScheduleCommand, 0)
		}
		l = append(l, c)
		s.scheduleCommands[c.UUID] = l
	}

	return sm.Result{}, nil
}

func (s *stateMachine) handleCreateLogShardCmd(cmd []byte) (sm.Result, error) {
	name, ok := isCreateLogShardCmd(cmd)
	if !ok {
		panic(moerr.NewError(moerr.INVALID_INPUT, "not create log shard cmd"))
	}
	if shardID, ok := s.LogShards[name]; ok {
		data := make([]byte, 8)
		binaryEnc.PutUint64(data, shardID)
		return sm.Result{Value: 0, Data: data}, nil
	}
	s.LogShards[name] = s.assignID()
	return sm.Result{Value: s.NextID}, nil
}

func (s *stateMachine) handleDNHeartbeat(cmd []byte) (sm.Result, error) {
	data := parseHeartbeatCmd(cmd)
	var hb pb.DNStoreHeartbeat
	if err := hb.Unmarshal(data); err != nil {
		panic(err)
	}
	s.DNState.Update(hb, s.Tick)
	return sm.Result{}, nil
}

func (s *stateMachine) handleLogHeartbeat(cmd []byte) (sm.Result, error) {
	data := parseHeartbeatCmd(cmd)
	var hb pb.LogStoreHeartbeat
	if err := hb.Unmarshal(data); err != nil {
		panic(err)
	}
	s.LogState.Update(hb, s.Tick)
	return sm.Result{}, nil
}

func (s *stateMachine) handleTick(cmd []byte) (sm.Result, error) {
	s.Tick++
	return sm.Result{}, nil
}

func (s *stateMachine) handleGetIDCmd(cmd []byte) (sm.Result, error) {
	count := parseGetIDCmd(cmd)
	s.NextID++
	v := s.NextID
	s.NextID += (count - 1)
	plog.Infof("get id returned [%d, %d)", v, v+count)
	return sm.Result{Value: v}, nil
}

func (s *stateMachine) Update(e sm.Entry) (sm.Result, error) {
	cmd := e.Cmd
	if _, ok := isCreateLogShardCmd(cmd); ok {
		return s.handleCreateLogShardCmd(cmd)
	} else if isDNHeartbeatCmd(cmd) {
		return s.handleDNHeartbeat(cmd)
	} else if isLogHeartbeatCmd(cmd) {
		return s.handleLogHeartbeat(cmd)
	} else if isTickCmd(cmd) {
		return s.handleTick(cmd)
	} else if isGetIDCmd(cmd) {
		return s.handleGetIDCmd(cmd)
	} else if isUpdateCommandsCmd(cmd) {
		return s.handleUpdateCommandsCmd(cmd)
	}
	panic(moerr.NewError(moerr.INVALID_INPUT, "unexpected haKeeper cmd"))
}

func (s *stateMachine) handleStateQuery() (interface{}, error) {
	// FIXME: pretty sure we need to deepcopy here
	return &HAKeeperState{
		Tick:        s.Tick,
		ClusterInfo: s.ClusterInfo,
		DNState:     s.DNState,
		LogState:    s.LogState,
	}, nil
}

func (s *stateMachine) Lookup(query interface{}) (interface{}, error) {
	if q, ok := query.(*logShardIDQuery); ok {
		id, ok := s.LogShards[q.name]
		if ok {
			return &logShardIDQueryResult{found: true, id: id}, nil
		}
		return &logShardIDQueryResult{found: false}, nil
	}
	if _, ok := query.(*StateQuery); ok {
		return s.handleStateQuery()
	}
	panic("unknown query type")
}

func (s *stateMachine) SaveSnapshot(w io.Writer,
	_ sm.ISnapshotFileCollection, _ <-chan struct{}) error {
	enc := gob.NewEncoder(w)
	return enc.Encode(s)
}

func (s *stateMachine) RecoverFromSnapshot(r io.Reader,
	_ []sm.SnapshotFile, _ <-chan struct{}) error {
	dec := gob.NewDecoder(r)
	return dec.Decode(s)
}
