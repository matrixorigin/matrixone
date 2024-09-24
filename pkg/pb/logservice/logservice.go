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

package logservice

import (
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

const (
	// NoLeader is the replica ID of the leader node.
	NoLeader uint64 = 0
	// HeaderSize is the size of the header for each logservice and
	// hakeeper command.
	HeaderSize = 4
)

// ResizePayload resizes the payload length to length bytes.
func (m *LogRecord) ResizePayload(length int) {
	m.Data = m.Data[:HeaderSize+8+length]
}

// Payload returns the payload byte slice.
func (m *LogRecord) Payload() []byte {
	return m.Data[HeaderSize+8:]
}

// NewRSMState creates a new HAKeeperRSMState instance.
func NewRSMState() HAKeeperRSMState {
	return HAKeeperRSMState{
		NextIDByKey:      make(map[string]uint64),
		ScheduleCommands: make(map[string]CommandBatch),
		LogShards:        make(map[string]uint64),
		CNState:          NewCNState(),
		TNState:          NewTNState(),
		LogState:         NewLogState(),
		ProxyState:       NewProxyState(),
		ClusterInfo:      newClusterInfo(),
	}
}

func newClusterInfo() ClusterInfo {
	return ClusterInfo{
		TNShards:  make([]metadata.TNShardRecord, 0),
		LogShards: make([]metadata.LogShardRecord, 0),
	}
}

// NewCNState creates a new CNState.
func NewCNState() CNState {
	return CNState{
		Stores: make(map[string]CNStoreInfo),
	}
}

// Update applies the incoming CNStoreHeartbeat into HAKeeper. Tick is the
// current tick of the HAKeeper which is used as the timestamp of the heartbeat.
func (s *CNState) Update(hb CNStoreHeartbeat, tick uint64) {
	storeInfo, ok := s.Stores[hb.UUID]
	if !ok {
		storeInfo = CNStoreInfo{}
		storeInfo.Labels = make(map[string]metadata.LabelList)
		storeInfo.UpTime = time.Now().UnixNano()
	}
	if storeInfo.WorkState == metadata.WorkState_Unknown { // set init value
		v, ok := metadata.WorkState_value[metadata.ToTitle(hb.InitWorkState)]
		if !ok || v == int32(metadata.WorkState_Unknown) {
			storeInfo.WorkState = metadata.WorkState_Working
		} else {
			storeInfo.WorkState = metadata.WorkState(v)
		}
	}
	storeInfo.Tick = tick
	storeInfo.ServiceAddress = hb.ServiceAddress
	storeInfo.SQLAddress = hb.SQLAddress
	storeInfo.LockServiceAddress = hb.LockServiceAddress
	storeInfo.ShardServiceAddress = hb.ShardServiceAddress
	storeInfo.Role = hb.Role
	storeInfo.TaskServiceCreated = hb.TaskServiceCreated
	storeInfo.QueryAddress = hb.QueryAddress
	storeInfo.GossipAddress = hb.GossipAddress
	storeInfo.GossipJoined = hb.GossipJoined
	if hb.ConfigData != nil {
		storeInfo.ConfigData = hb.ConfigData
	}
	storeInfo.Resource = hb.Resource
	storeInfo.CommitID = hb.CommitID
	s.Stores[hb.UUID] = storeInfo
}

// UpdateLabel updates labels of CN store.
func (s *CNState) UpdateLabel(label CNStoreLabel) {
	storeInfo, ok := s.Stores[label.UUID]
	// If the CN store does not exist, we should do nothing and wait for
	// CN heartbeat.
	if !ok {
		return
	}
	storeInfo.Labels = label.Labels
	s.Stores[label.UUID] = storeInfo
}

// UpdateWorkState updates work state of CN store.
func (s *CNState) UpdateWorkState(state CNWorkState) {
	if state.GetState() == metadata.WorkState_Unknown {
		state.State = metadata.WorkState_Working
	}
	storeInfo, ok := s.Stores[state.UUID]
	// If the CN store does not exist, we should do nothing and wait for
	// CN heartbeat.
	if !ok {
		return
	}
	storeInfo.WorkState = state.State
	s.Stores[state.UUID] = storeInfo
}

// PatchCNStore updates work state and labels of CN store.
func (s *CNState) PatchCNStore(stateLabel CNStateLabel) {
	if stateLabel.GetState() == metadata.WorkState_Unknown {
		stateLabel.State = metadata.WorkState_Working
	}
	storeInfo, ok := s.Stores[stateLabel.UUID]
	// If the CN store does not exist, we should do nothing and wait for
	// CN heartbeat.
	if !ok {
		return
	}
	storeInfo.WorkState = stateLabel.State
	if stateLabel.Labels != nil {
		storeInfo.Labels = stateLabel.Labels
	}
	s.Stores[stateLabel.UUID] = storeInfo
}

// NewTNState creates a new DNState.
func NewTNState() TNState {
	return TNState{
		Stores: make(map[string]TNStoreInfo),
	}
}

// Update applies the incoming DNStoreHeartbeat into HAKeeper. Tick is the
// current tick of the HAKeeper which is used as the timestamp of the heartbeat.
func (s *TNState) Update(hb TNStoreHeartbeat, tick uint64) {
	storeInfo, ok := s.Stores[hb.UUID]
	if !ok {
		storeInfo = TNStoreInfo{}
	}
	storeInfo.Tick = tick
	storeInfo.Shards = hb.Shards
	storeInfo.ServiceAddress = hb.ServiceAddress
	storeInfo.LogtailServerAddress = hb.LogtailServerAddress
	storeInfo.LockServiceAddress = hb.LockServiceAddress
	storeInfo.ShardServiceAddress = hb.ShardServiceAddress
	storeInfo.TaskServiceCreated = hb.TaskServiceCreated
	if hb.ConfigData != nil {
		storeInfo.ConfigData = hb.ConfigData
	}
	storeInfo.QueryAddress = hb.QueryAddress
	s.Stores[hb.UUID] = storeInfo
}

// NewLogState creates a new LogState.
func NewLogState() LogState {
	return LogState{
		Shards: make(map[uint64]LogShardInfo),
		Stores: make(map[string]LogStoreInfo),
	}
}

// Update applies the incoming heartbeat message to the LogState with the
// specified tick used as the timestamp.
func (s *LogState) Update(hb LogStoreHeartbeat, tick uint64) {
	s.updateStores(hb, tick)
	s.updateShards(hb)
}

func (s *LogState) updateStores(hb LogStoreHeartbeat, tick uint64) {
	storeInfo, ok := s.Stores[hb.UUID]
	if !ok {
		storeInfo = LogStoreInfo{}
	}
	storeInfo.Tick = tick
	storeInfo.RaftAddress = hb.RaftAddress
	storeInfo.ServiceAddress = hb.ServiceAddress
	storeInfo.GossipAddress = hb.GossipAddress
	storeInfo.Replicas = hb.Replicas
	storeInfo.TaskServiceCreated = hb.TaskServiceCreated
	if hb.ConfigData != nil {
		storeInfo.ConfigData = hb.ConfigData
	}
	storeInfo.Locality = hb.Locality
	s.Stores[hb.UUID] = storeInfo
}

func (s *LogState) updateShards(hb LogStoreHeartbeat) {
	for _, incoming := range hb.Replicas {
		recorded, ok := s.Shards[incoming.ShardID]
		if !ok {
			recorded = LogShardInfo{
				ShardID:           incoming.ShardID,
				Replicas:          make(map[uint64]string),
				NonVotingReplicas: make(map[uint64]string),
			}
		}

		if incoming.Epoch > recorded.Epoch {
			recorded.Epoch = incoming.Epoch
			recorded.Replicas = incoming.Replicas
			recorded.NonVotingReplicas = incoming.NonVotingReplicas
		} else if incoming.Epoch == recorded.Epoch && incoming.Epoch > 0 {
			if !reflect.DeepEqual(recorded.Replicas, incoming.Replicas) ||
				!reflect.DeepEqual(recorded.NonVotingReplicas, incoming.NonVotingReplicas) {
				panic(fmt.Sprintf("inconsistent replicas, recorded: %+v, incoming: %+v",
					recorded, incoming))
			}
		}

		if incoming.Term > recorded.Term && incoming.LeaderID != NoLeader {
			recorded.Term = incoming.Term
			recorded.LeaderID = incoming.LeaderID
		}

		s.Shards[incoming.ShardID] = recorded
	}
}

// LogString returns "ServiceType/ConfigChangeType UUID RepUuid:RepShardID:RepID InitialMembers".
// Do not add CN's StartTaskRunner info to log string, because there has user and password.
func (m *ScheduleCommand) LogString() string {
	c := func(s string) string {
		return s
	}

	serviceType := map[ServiceType]string{
		LogService:   "L",
		TNService:    "D",
		CNService:    "C",
		ProxyService: "P",
	}[m.ServiceType]

	target := c(m.UUID)
	if m.ShutdownStore != nil {
		return fmt.Sprintf("%s/shutdown %s", serviceType, target)
	}
	if m.CreateTaskService != nil {
		return fmt.Sprintf("%s/CreateTask %s", serviceType, target)
	}
	if m.DeleteCNStore != nil {
		return fmt.Sprintf("%s/DeleteCNStore %s", serviceType, target)
	}
	if m.JoinGossipCluster != nil {
		return fmt.Sprintf("%s/JoinGossipCluster %s", serviceType, target)
	}
	if m.DeleteProxyStore != nil {
		return fmt.Sprintf("%s/DeleteProxyStore %s", serviceType, target)
	}
	if m.ConfigChange == nil {
		return fmt.Sprintf("%s/unknown command %s", serviceType, m.String())
	}

	configChangeType := "Unknown"
	if m.ConfigChange != nil {
		configChangeType = map[ConfigChangeType]string{
			AddReplica:    "Add",
			RemoveReplica: "Remove",
			StartReplica:  "Start",
			StopReplica:   "Stop",
			KillZombie:    "Kill",
		}[m.ConfigChange.ChangeType]
	}

	replica := c(m.ConfigChange.Replica.UUID)
	s := fmt.Sprintf("%s/%s %s %s:%d:%d:%d",
		serviceType, configChangeType, target, replica,
		m.ConfigChange.Replica.ShardID,
		m.ConfigChange.Replica.ReplicaID,
		m.ConfigChange.Replica.Epoch)

	if len(m.ConfigChange.InitialMembers) != 0 {
		initMembers := make([]string, 0, len(m.ConfigChange.InitialMembers))
		for repId, uuid := range m.ConfigChange.InitialMembers {
			initMembers = append(initMembers, fmt.Sprintf("%d:%s", repId, c(uuid)))
		}
		sort.Strings(initMembers)
		s += fmt.Sprintf(" %v", initMembers)
	}

	return s
}

// NewProxyState creates a new ProxyState.
func NewProxyState() ProxyState {
	return ProxyState{
		Stores: make(map[string]ProxyStore),
	}
}

func (s *ProxyState) Update(hb ProxyHeartbeat, tick uint64) {
	storeInfo, ok := s.Stores[hb.UUID]
	if !ok {
		storeInfo = ProxyStore{}
	}
	storeInfo.UUID = hb.UUID
	storeInfo.Tick = tick
	storeInfo.ListenAddress = hb.ListenAddress
	if hb.ConfigData != nil {
		storeInfo.ConfigData = hb.ConfigData
	}
	s.Stores[hb.UUID] = storeInfo
}

func (l *Locality) Format() string {
	if l == nil || l.Value == nil {
		return ""
	}
	if len(l.Value) == 0 {
		return ""
	}
	keys := make([]string, 0, len(l.Value))
	for k := range l.Value {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var s string
	for _, k := range keys {
		s += fmt.Sprintf("%s:%s;", k, l.Value[k])
	}
	return s[:len(s)-1]
}
