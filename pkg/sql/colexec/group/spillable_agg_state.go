// Copyright 2025 Matrix Origin
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

package group

import (
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type SpillableAggState struct {
	GroupVectors  []*vector.Vector
	PartialStates []any
	GroupCount    int
}

func (s *SpillableAggState) Serialize() ([]byte, error) {
	data := map[string]interface{}{
		"group_count":    s.GroupCount,
		"partial_states": s.PartialStates,
		"group_vectors":  make([]map[string]interface{}, len(s.GroupVectors)),
	}

	for i, vec := range s.GroupVectors {
		if vec != nil {
			vecData := map[string]interface{}{
				"type":     vec.GetType().String(),
				"length":   vec.Length(),
				"is_const": vec.IsConst(),
			}
			data["group_vectors"].([]map[string]interface{})[i] = vecData
		}
	}

	return json.Marshal(data)
}

func (s *SpillableAggState) Deserialize(data []byte) error {
	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}

	if count, ok := parsed["group_count"].(float64); ok {
		s.GroupCount = int(count)
	}

	if states, ok := parsed["partial_states"].([]interface{}); ok {
		s.PartialStates = states
	}

	return nil
}

func (s *SpillableAggState) EstimateSize() int64 {
	size := int64(0)
	for _, vec := range s.GroupVectors {
		if vec != nil {
			size += int64(vec.Size())
		}
	}
	size += int64(len(s.PartialStates) * 64)
	return size
}

func (s *SpillableAggState) Free(mp *mpool.MPool) {
	for _, vec := range s.GroupVectors {
		if vec != nil {
			vec.Free(mp)
		}
	}
	s.GroupVectors = nil
	s.PartialStates = nil
}
