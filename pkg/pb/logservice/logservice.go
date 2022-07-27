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
		ScheduleCommands: make(map[string]CommandBatch),
		LogShards:        make(map[string]uint64),
		CNState:          NewCNState(),
		DNState:          NewDNState(),
		LogState:         NewLogState(),
		ClusterInfo:      newClusterInfo(),
	}
}

func newClusterInfo() ClusterInfo {
	return ClusterInfo{
		DNShards:  make([]metadata.DNShardRecord, 0),
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
	}
	storeInfo.Tick = tick
	storeInfo.ServiceAddress = hb.ServiceAddress
	s.Stores[hb.UUID] = storeInfo
}

// NewDNState creates a new DNState.
func NewDNState() DNState {
	return DNState{
		Stores: make(map[string]DNStoreInfo),
	}
}

// Update applies the incoming DNStoreHeartbeat into HAKeeper. Tick is the
// current tick of the HAKeeper which is used as the timestamp of the heartbeat.
func (s *DNState) Update(hb DNStoreHeartbeat, tick uint64) {
	storeInfo, ok := s.Stores[hb.UUID]
	if !ok {
		storeInfo = DNStoreInfo{}
	}
	storeInfo.Tick = tick
	storeInfo.Shards = hb.Shards
	storeInfo.ServiceAddress = hb.ServiceAddress
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
	s.Stores[hb.UUID] = storeInfo
}

func (s *LogState) updateShards(hb LogStoreHeartbeat) {
	for _, incoming := range hb.Replicas {
		recorded, ok := s.Shards[incoming.ShardID]
		if !ok {
			recorded = LogShardInfo{
				ShardID:  incoming.ShardID,
				Replicas: make(map[uint64]string),
			}
		}

		if incoming.Epoch > recorded.Epoch {
			recorded.Epoch = incoming.Epoch
			recorded.Replicas = incoming.Replicas
		} else if incoming.Epoch == recorded.Epoch && incoming.Epoch > 0 {
			if !reflect.DeepEqual(recorded.Replicas, incoming.Replicas) {
				panic("inconsistent replicas")
			}
		}

		if incoming.Term > recorded.Term && incoming.LeaderID != NoLeader {
			recorded.Term = incoming.Term
			recorded.LeaderID = incoming.LeaderID
		}

		s.Shards[incoming.ShardID] = recorded
	}
}

// LogString returns "ServiceType/ConfigChangeType UUID RepUuid:RepShardID:RepID InitialMembers"
func (m *ScheduleCommand) LogString() string {
	var serviceType = map[ServiceType]string{
		LogService: "L",
		DnService:  "D",
	}

	var configChangeType = map[ConfigChangeType]string{
		AddReplica:    "Add",
		RemoveReplica: "Remove",
		StartReplica:  "Start",
		StopReplica:   "Stop",
	}
	scheUuid := m.UUID
	if len(m.UUID) > 6 {
		scheUuid = scheUuid[:6]
	}

	if m.ConfigChange == nil {
		return fmt.Sprintf("%s/shutdown %s", serviceType[m.ServiceType], scheUuid)
	}

	repUuid := m.ConfigChange.Replica.UUID
	if len(repUuid) > 6 {
		repUuid = repUuid[:6]
	}

	var initMembers map[uint64]string
	if len(m.ConfigChange.InitialMembers) == 0 {
		initMembers = nil
	} else {
		initMembers = make(map[uint64]string)
		for repId, uuid := range m.ConfigChange.InitialMembers {
			if len(uuid) > 6 {
				initMembers[repId] = uuid[:6]
			} else {
				initMembers[repId] = uuid
			}
		}
	}

	s := fmt.Sprintf("%s/%s %s %s:%d:%d:%d", serviceType[m.ServiceType],
		configChangeType[m.ConfigChange.ChangeType], scheUuid,
		repUuid, m.ConfigChange.Replica.ShardID,
		m.ConfigChange.Replica.ReplicaID, m.ConfigChange.Replica.Epoch)

	if initMembers != nil {
		s += fmt.Sprintf(" %v", initMembers)
	}

	return s
}
