// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// Equal complete keys are equal on every identity-key subset. Router hash
// collisions merely co-locate rows; membership uses the existing full-key
// hash state and retains its collision behavior.
func setMembershipIdentityKeys(node *plan.Node) []int32 {
	if len(node.PhysicalEqualityKeyList) > 0 && len(node.PhysicalEqualityKeyList) != len(node.ProjectList) {
		return nil
	}
	var keys []int32
	for i, visible := range node.ProjectList {
		if visible == nil || visible.GetCol() == nil || visible.GetCol().RelPos != 0 || visible.GetCol().ColPos != int32(i) {
			return nil
		}
		key := visible
		if len(node.PhysicalEqualityKeyList) > 0 {
			key = node.PhysicalEqualityKeyList[i]
		}
		if key == nil || key.GetCol() == nil || key.GetCol().RelPos != 0 || key.GetCol().ColPos != int32(i) {
			continue
		}
		visibleType, keyType := visible.Typ, key.Typ
		visibleType.NotNullable, keyType.NotNullable = false, false
		if !proto.Equal(&visibleType, &keyType) {
			continue
		}
		switch types.T(key.Typ.Id) {
		case types.T_bool, types.T_int8, types.T_int16, types.T_int32, types.T_int64,
			types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
			types.T_decimal64, types.T_decimal128, types.T_date, types.T_datetime,
			types.T_timestamp, types.T_time:
			keys = append(keys, int32(i))
		}
	}
	return keys
}
