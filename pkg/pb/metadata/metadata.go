// Copyright 2022 Matrix Origin
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

package metadata

import (
	"bytes"
	"fmt"
	"strings"
)

// IsEmpty return true if is a empty DNShard
func (m DNShard) IsEmpty() bool {
	return m.ShardID == 0
}

// Equal returns true if DNShard is same
func (m DNShard) Equal(dn DNShard) bool {
	return m.ShardID == dn.ShardID && m.ReplicaID == dn.ReplicaID
}

// DebugString returns debug string
func (m DNShard) DebugString() string {
	return fmt.Sprintf("%d-%d-%d-%s", m.ShardID, m.ReplicaID, m.LogShardID, m.Address)
}

// DebugString returns debug string
func (m DNStore) DebugString() string {
	n := len(m.Shards)
	var buf bytes.Buffer
	buf.WriteString(m.UUID)
	buf.WriteString("/")
	buf.WriteString(fmt.Sprintf("%d", len(m.Shards)))
	buf.WriteString(" DNShards[")
	for idx, shard := range m.Shards {
		buf.WriteString(shard.DebugString())
		if idx < n-1 {
			buf.WriteString(", ")
		}
	}
	buf.WriteString("]")
	return buf.String()
}

// DebugString returns debug string
func (m CNStore) DebugString() string {
	return fmt.Sprintf("%s/%s", m.UUID, m.Role.String())
}

// MustParseCNRole parse CN Role from role string
func MustParseCNRole(role string) CNRole {
	if v, ok := CNRole_value[strings.ToUpper(role)]; ok {
		return CNRole(v)
	}
	panic(fmt.Sprintf("invalid CN Role %s", role))
}

func (m CNService) DebugString() string {
	var buf bytes.Buffer
	buf.WriteString(m.ServiceID)
	buf.WriteString("/sql(")
	buf.WriteString(m.SQLAddress)
	buf.WriteString(")/pipeline(")
	buf.WriteString(m.PipelineServiceAddress)
	buf.WriteString(")/lock(")
	buf.WriteString(m.LockServiceAddress)
	buf.WriteString(")/[")
	for k, v := range m.Labels {
		buf.WriteString(k)
		buf.WriteString("=")
		buf.WriteString(v.String())
		buf.WriteString(" ")
	}
	buf.WriteString("]")
	return buf.String()
}

func (m DNService) DebugString() string {
	var buf bytes.Buffer
	buf.WriteString(m.ServiceID)
	buf.WriteString("/txn(")
	buf.WriteString(m.TxnServiceAddress)
	buf.WriteString(")/[")
	n := len(m.Shards)
	for idx, shard := range m.Shards {
		buf.WriteString(shard.DebugString())
		if idx < n-1 {
			buf.WriteString(", ")
		}
	}
	buf.WriteString("]/")
	for k, v := range m.Labels {
		buf.WriteString(k)
		buf.WriteString("=")
		buf.WriteString(v.String())
		buf.WriteString(" ")
	}
	buf.WriteString("]")
	return buf.String()
}
