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
	"bytes"
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type SpillableAggState struct {
	GroupVectors       []*vector.Vector
	MarshaledAggStates [][]byte
	GroupCount         int
}

func (s *SpillableAggState) Serialize() ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	if err := binary.Write(buf, binary.LittleEndian, int32(s.GroupCount)); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, int32(len(s.GroupVectors))); err != nil {
		return nil, err
	}
	for _, vec := range s.GroupVectors {
		if vec == nil {
			if err := binary.Write(buf, binary.LittleEndian, int32(0)); err != nil {
				return nil, err
			}
			continue
		}

		vecBytes, err := vec.MarshalBinary()
		if err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, int32(len(vecBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(vecBytes); err != nil {
			return nil, err
		}
	}

	if err := binary.Write(buf, binary.LittleEndian, int32(len(s.MarshaledAggStates))); err != nil {
		return nil, err
	}
	for _, aggState := range s.MarshaledAggStates {
		if err := binary.Write(buf, binary.LittleEndian, int32(len(aggState))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(aggState); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (s *SpillableAggState) Deserialize(data []byte, mp *mpool.MPool) error {
	buf := bytes.NewReader(data)

	var groupCount int32
	if err := binary.Read(buf, binary.LittleEndian, &groupCount); err != nil {
		return err
	}
	s.GroupCount = int(groupCount)

	var groupVecCount int32
	if err := binary.Read(buf, binary.LittleEndian, &groupVecCount); err != nil {
		return err
	}
	s.GroupVectors = make([]*vector.Vector, groupVecCount)
	for i := 0; i < int(groupVecCount); i++ {
		var size int32
		if err := binary.Read(buf, binary.LittleEndian, &size); err != nil {
			return err
		}
		if size == 0 {
			s.GroupVectors[i] = nil
			continue
		}

		vecBytes := make([]byte, size)
		if _, err := buf.Read(vecBytes); err != nil {
			return err
		}

		vec := vector.NewVec(types.T_any.ToType())
		if err := vec.UnmarshalBinaryWithCopy(vecBytes, mp); err != nil {
			return err
		}
		s.GroupVectors[i] = vec
	}

	var aggStateCount int32
	if err := binary.Read(buf, binary.LittleEndian, &aggStateCount); err != nil {
		return err
	}
	s.MarshaledAggStates = make([][]byte, aggStateCount)
	for i := 0; i < int(aggStateCount); i++ {
		var size int32
		if err := binary.Read(buf, binary.LittleEndian, &size); err != nil {
			return err
		}
		s.MarshaledAggStates[i] = make([]byte, size)
		if _, err := buf.Read(s.MarshaledAggStates[i]); err != nil {
			return err
		}
	}

	return nil
}

func (s *SpillableAggState) EstimateSize() int64 {
	size := int64(0)
	for _, vec := range s.GroupVectors {
		if vec != nil {
			size += int64(vec.Allocated())
		}
	}
	for _, aggState := range s.MarshaledAggStates {
		size += int64(len(aggState))
	}
	return size
}

func (s *SpillableAggState) Free(mp *mpool.MPool) {
	for _, vec := range s.GroupVectors {
		if vec != nil {
			vec.Free(mp)
		}
	}
	s.GroupVectors = nil
	s.MarshaledAggStates = nil
}
