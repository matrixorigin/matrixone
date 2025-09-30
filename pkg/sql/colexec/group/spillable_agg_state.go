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
	logutil.Debug("SpillableAggState starting serialization",
		zap.Int("group_count", s.GroupCount),
		zap.Int("group_vectors_count", len(s.GroupVectors)),
		zap.Int("agg_states_count", len(s.MarshaledAggStates)))

	buf := bytes.NewBuffer(nil)

	if err := binary.Write(buf, binary.LittleEndian, int32(s.GroupCount)); err != nil {
		logutil.Error("SpillableAggState failed to write group count", zap.Error(err))
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, int32(len(s.GroupVectors))); err != nil {
		logutil.Error("SpillableAggState failed to write group vectors count", zap.Error(err))
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, int32(len(s.GroupVectorTypes))); err != nil {
		logutil.Error("SpillableAggState failed to write group vector types count", zap.Error(err))
		return nil, err
	}
	for i, typ := range s.GroupVectorTypes {
		typBytes, err := typ.MarshalBinary()
		if err != nil {
			logutil.Error("SpillableAggState failed to marshal vector type", zap.Int("type_index", i), zap.Error(err))
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, int32(len(typBytes))); err != nil {
			logutil.Error("SpillableAggState failed to write type bytes length", zap.Int("type_index", i), zap.Error(err))
			return nil, err
		}
		if _, err := buf.Write(typBytes); err != nil {
			logutil.Error("SpillableAggState failed to write type bytes", zap.Int("type_index", i), zap.Error(err))
			return nil, err
		}
	}

	for i, vec := range s.GroupVectors {
		if vec == nil {
			if err := binary.Write(buf, binary.LittleEndian, int32(0)); err != nil {
				logutil.Error("SpillableAggState failed to write zero length for nil vector",
					zap.Int("vec_index", i), zap.Error(err))
				return nil, err
			}
			continue
		}

		vecBytes, err := vec.MarshalBinary()
		if err != nil {
			logutil.Error("SpillableAggState failed to marshal vector",
				zap.Int("vec_index", i), zap.Error(err))
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, int32(len(vecBytes))); err != nil {
			logutil.Error("SpillableAggState failed to write vector bytes length",
				zap.Int("vec_index", i), zap.Error(err))
			return nil, err
		}
		if _, err := buf.Write(vecBytes); err != nil {
			logutil.Error("SpillableAggState failed to write vector bytes",
				zap.Int("vec_index", i), zap.Error(err))
			return nil, err
		}
	}

	if err := binary.Write(buf, binary.LittleEndian, int32(len(s.MarshaledAggStates))); err != nil {
		logutil.Error("SpillableAggState failed to write agg states count", zap.Error(err))
		return nil, err
	}
	for i, aggState := range s.MarshaledAggStates {
		if err := binary.Write(buf, binary.LittleEndian, int32(len(aggState))); err != nil {
			logutil.Error("SpillableAggState failed to write agg state length",
				zap.Int("agg_index", i), zap.Error(err))
			return nil, err
		}
		if _, err := buf.Write(aggState); err != nil {
			logutil.Error("SpillableAggState failed to write agg state bytes",
				zap.Int("agg_index", i), zap.Error(err))
			return nil, err
		}
	}

	result := buf.Bytes()
	logutil.Debug("SpillableAggState completed serialization",
		zap.Int("serialized_size", len(result)))

	return result, nil
}

func (s *SpillableAggState) Deserialize(data []byte, mp *mpool.MPool) error {
	logutil.Debug("SpillableAggState starting deserialization",
		zap.Int("data_size", len(data)))

	buf := bytes.NewReader(data)

	var groupCount int32
	if err := binary.Read(buf, binary.LittleEndian, &groupCount); err != nil {
		logutil.Error("SpillableAggState failed to read group count", zap.Error(err))
		return err
	}
	s.GroupCount = int(groupCount)

	var groupVecCount int32
	if err := binary.Read(buf, binary.LittleEndian, &groupVecCount); err != nil {
		logutil.Error("SpillableAggState failed to read group vector count", zap.Error(err))
		return err
	}

	var groupVecTypeCount int32
	if err := binary.Read(buf, binary.LittleEndian, &groupVecTypeCount); err != nil {
		logutil.Error("SpillableAggState failed to read group vector type count", zap.Error(err))
		return err
	}
	s.GroupVectorTypes = make([]types.Type, groupVecTypeCount)
	for i := 0; i < int(groupVecTypeCount); i++ {
		var size int32
		if err := binary.Read(buf, binary.LittleEndian, &size); err != nil {
			logutil.Error("SpillableAggState failed to read vector type size",
				zap.Int("type_index", i), zap.Error(err))
			return err
		}
		typBytes := make([]byte, size)
		if _, err := buf.Read(typBytes); err != nil {
			logutil.Error("SpillableAggState failed to read vector type bytes",
				zap.Int("type_index", i), zap.Error(err))
			return err
		}
		if err := s.GroupVectorTypes[i].UnmarshalBinary(typBytes); err != nil {
			logutil.Error("SpillableAggState failed to unmarshal vector type",
				zap.Int("type_index", i), zap.Error(err))
			return err
		}
	}

	s.GroupVectors = make([]*vector.Vector, groupVecCount)
	for i := 0; i < int(groupVecCount); i++ {
		var size int32
		if err := binary.Read(buf, binary.LittleEndian, &size); err != nil {
			logutil.Error("SpillableAggState failed to read vector size",
				zap.Int("vec_index", i), zap.Error(err))
			return err
		}
		if size == 0 {
			s.GroupVectors[i] = nil
			continue
		}

		vecBytes := make([]byte, size)
		if _, err := buf.Read(vecBytes); err != nil {
			logutil.Error("SpillableAggState failed to read vector bytes",
				zap.Int("vec_index", i), zap.Error(err))
			return err
		}

		vecType := types.T_any.ToType()
		if i < len(s.GroupVectorTypes) {
			vecType = s.GroupVectorTypes[i]
		}

		vec := vector.NewOffHeapVecWithType(vecType)
		if err := vec.UnmarshalBinaryWithCopy(vecBytes, mp); err != nil {
			logutil.Error("SpillableAggState failed to unmarshal vector",
				zap.Int("vec_index", i), zap.Error(err))
			vec.Free(mp)
			return err
		}
		s.GroupVectors[i] = vec
	}

	var aggStateCount int32
	if err := binary.Read(buf, binary.LittleEndian, &aggStateCount); err != nil {
		logutil.Error("SpillableAggState failed to read agg state count", zap.Error(err))
		return err
	}
	s.MarshaledAggStates = make([][]byte, aggStateCount)
	for i := 0; i < int(aggStateCount); i++ {
		var size int32
		if err := binary.Read(buf, binary.LittleEndian, &size); err != nil {
			logutil.Error("SpillableAggState failed to read agg state size",
				zap.Int("agg_index", i), zap.Error(err))
			return err
		}
		s.MarshaledAggStates[i] = make([]byte, size)
		if _, err := buf.Read(s.MarshaledAggStates[i]); err != nil {
			logutil.Error("SpillableAggState failed to read agg state bytes",
				zap.Int("agg_index", i), zap.Error(err))
			return err
		}
	}

	logutil.Debug("SpillableAggState completed deserialization",
		zap.Int("group_count", s.GroupCount),
		zap.Int("group_vectors_count", len(s.GroupVectors)),
		zap.Int("agg_states_count", len(s.MarshaledAggStates)))

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
