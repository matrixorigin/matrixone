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
)

// IsEmpty return true if is a empty DNShard
func (m DNShard) IsEmpty() bool {
	return m.ShardID == 0
}

// DebugString returns debug string
func (m DNShard) DebugString() string {
	var buffer bytes.Buffer

	buffer.WriteString("shard-id: ")
	buffer.WriteString(fmt.Sprintf("%d", m.ShardID))
	buffer.WriteString(", ")

	buffer.WriteString("replica-id: ")
	buffer.WriteString(fmt.Sprintf("%d", m.ReplicaID))
	buffer.WriteString(", ")

	buffer.WriteString("address: ")
	buffer.WriteString(m.Address)
	return buffer.String()
}
