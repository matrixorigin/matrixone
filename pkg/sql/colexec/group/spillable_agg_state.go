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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

type SpillableAggState struct {
	GroupVectors       []*vector.Vector
	GroupVectorTypes   []types.Type
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

	if err := binary.Write(buf, binary.LittleEndian, int32(len(s.GroupVectorTypes))); err != nil {
		return nil, err
	}
	for _, typ := range s.GroupVectorTypes {
		typBytes, err := typ.MarshalBinary()
		if err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, int32(len(typBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(typBytes); err != nil {
			return nil, err
		}
	}

	for i, vec := range s.GroupVectors {
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

		if i >= len(s.GroupVectorTypes) {
			s.GroupVectorTypes = append(s.GroupVectorTypes, *vec.GetType())
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

	retBytes := buf.Bytes()
	logutil.Debug("serialized spillable agg state",
		zap.String("component", "group-spill"),
		zap.Int("size", len(retBytes)),
		zap.Int("group-count", s.GroupCount))
	return retBytes, nil
}

func (s *SpillableAggState) Deserialize(data []byte, mp *mpool.MPool) error {
	logutil.Debug("deserializing spillable agg state",
		zap.String("component", "group-spill"),
		zap.Int("size", len(data)))
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

	var groupVecTypeCount int32
	if err := binary.Read(buf, binary.LittleEndian, &groupVecTypeCount); err != nil {
		return err
	}
	s.GroupVectorTypes = make([]types.Type, groupVecTypeCount)
	for i := 0; i < int(groupVecTypeCount); i++ {
		var size int32
		if err := binary.Read(buf, binary.LittleEndian, &size); err != nil {
			return err
		}
		typBytes := make([]byte, size)
		if _, err := buf.Read(typBytes); err != nil {
			return err
		}
		if err := s.GroupVectorTypes[i].UnmarshalBinary(typBytes); err != nil {
			return err
		}
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

		var vecType types.Type
		if i < len(s.GroupVectorTypes) {
			vecType = s.GroupVectorTypes[i]
		} else {
			vecType = types.T_any.ToType()
		}

		vec := vector.NewOffHeapVecWithType(vecType)
		if err := vec.UnmarshalBinaryWithCopy(vecBytes, mp); err != nil {
			vec.Free(mp)
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
	s.GroupVectorTypes = nil
	s.MarshaledAggStates = nil
}
