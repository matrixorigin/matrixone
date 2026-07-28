// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// group_concat is a special string aggregation function.
type groupConcatExec struct {
	aggExec
	distinct     bool
	distinctHash distinctHash
	separator    []byte
	maxLen       uint64
}

var groupConcatConfigMagic = []byte{0xff, 'G', 'C', 1}

const groupConcatConfigHeaderSize = 12

// EncodeGroupConcatConfig serializes the statement-specific GROUP_CONCAT
// settings into the aggregate config that is also sent to remote CNs.
func EncodeGroupConcatConfig(separator string, maxLen uint64) []byte {
	config := make([]byte, groupConcatConfigHeaderSize+len(separator))
	copy(config, groupConcatConfigMagic)
	binary.LittleEndian.PutUint64(config[len(groupConcatConfigMagic):], maxLen)
	copy(config[groupConcatConfigHeaderSize:], separator)
	return config
}

// RefreshGroupConcatConfigMaxLen preserves the statement's separator while
// replacing the session-scoped limit for a reused prepared execution.
func RefreshGroupConcatConfigMaxLen(config []byte, maxLen uint64) []byte {
	separator := config
	if len(config) >= groupConcatConfigHeaderSize &&
		bytes.Equal(config[:len(groupConcatConfigMagic)], groupConcatConfigMagic) {
		separator = config[groupConcatConfigHeaderSize:]
	}
	return EncodeGroupConcatConfig(string(separator), maxLen)
}

func GroupConcatReturnType(args []types.Type) types.Type {
	for _, p := range args {
		if p.Oid == types.T_binary || p.Oid == types.T_varbinary || p.Oid == types.T_blob {
			return types.T_blob.ToType()
		}
	}
	return types.T_text.ToType()
}

func newGroupConcatExec(mg *mpool.MPool, info multiAggInfo, separator string) AggFuncExec {
	exec := &groupConcatExec{
		distinct:     info.distinct,
		distinctHash: newDistinctHash(mg),
		separator:    []byte(separator),
		maxLen:       math.MaxUint64,
	}
	exec.mp = mg
	exec.aggInfo = aggInfo{
		aggId:      info.aggID,
		isDistinct: false,
		argTypes:   info.argTypes,
		retType:    info.retType,
		emptyNull:  info.emptyNull,
		saveArg:    true,
		opaqueArg:  true,
	}
	return exec
}

func (exec *groupConcatExec) IsDistinct() bool {
	return exec.distinct
}

func (exec *groupConcatExec) GroupGrow(more int) error {
	if exec.distinct {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}
	return exec.aggExec.GroupGrow(more)
}

func (exec *groupConcatExec) PreAllocateGroups(more int) error {
	if exec.distinct {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}
	return exec.aggExec.PreAllocateGroups(more)
}

func isValidGroupConcatUnit(value []byte) error {
	if len(value) > math.MaxUint16 {
		return moerr.NewInternalErrorNoCtx("group_concat: the length of the value is too long")
	}
	return nil
}

func (exec *groupConcatExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *groupConcatExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *groupConcatExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if exec.distinct {
		for i, grp := range groups {
			if grp == GroupNotMatched {
				continue
			}
			row := offset + i
			payload, err := encodeGroupConcatPayload(vectors, row, exec.argTypes)
			if err != nil {
				return err
			}
			if payload == nil {
				continue
			}
			need, err := exec.distinctHash.fill(int(grp-1), vectors, row)
			if err != nil {
				return err
			}
			if !need {
				continue
			}
			x, y := exec.getXY(grp - 1)
			if err := exec.state[x].fillArg(exec.mp, y, payload, false); err != nil {
				return err
			}
		}
		return nil
	}

	payloads := make([][]byte, len(groups))
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		payload, err := encodeGroupConcatPayload(vectors, offset+i, exec.argTypes)
		if err != nil {
			return err
		}
		payloads[i] = payload
	}
	return exec.batchFillOpaqueArgs(offset, groups, payloads, exec.IsDistinct())
}

func (exec *groupConcatExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *groupConcatExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*groupConcatExec)
	if exec.distinct {
		if err := exec.distinctHash.merge(&other.distinctHash); err != nil {
			return err
		}
	}
	return exec.batchMergeArgs(&other.aggExec, offset, groups, false)
}

func (exec *groupConcatExec) SetExtraInformation(partialResult any, _ int) error {
	config, ok := partialResult.([]byte)
	if !ok {
		return moerr.NewInternalErrorNoCtx("group_concat: invalid config type")
	}
	if len(config) >= groupConcatConfigHeaderSize &&
		bytes.Equal(config[:len(groupConcatConfigMagic)], groupConcatConfigMagic) {
		exec.maxLen = binary.LittleEndian.Uint64(config[len(groupConcatConfigMagic):groupConcatConfigHeaderSize])
		exec.separator = config[groupConcatConfigHeaderSize:]
		return nil
	}

	// Keep accepting the legacy separator-only config for intermediate
	// results and callers that construct aggregate executors directly.
	exec.separator = config
	return nil
}

func (exec *groupConcatExec) Flush() (_ []*vector.Vector, retErr error) {
	vecs := make([]*vector.Vector, len(exec.state))
	defer func() {
		if retErr != nil {
			for _, v := range vecs {
				if v != nil {
					v.Free(exec.mp)
				}
			}
		}
	}()
	for i, st := range exec.state {
		vecs[i] = vector.NewOffHeapVecWithType(exec.retType)
		if err := vecs[i].PreExtend(int(st.length), exec.mp); err != nil {
			return nil, err
		}
		for j := 0; j < int(st.length); j++ {
			if st.argCnt[j] == 0 {
				vector.AppendNull(vecs[i], exec.mp)
				continue
			}
			initialCapacity := uint64(64)
			if exec.maxLen < initialCapacity {
				initialCapacity = exec.maxLen
			}
			buf := make([]byte, 0, int(initialCapacity))
			first := true
			binaryResult := exec.retType.Oid == types.T_blob
			truncated := false
			if err := st.iter(uint16(j), func(k []byte) error {
				if truncated {
					return nil
				}
				payload := aggPayloadFromKey(&exec.aggInfo, k)
				if !first {
					buf, truncated = appendGroupConcatBytes(buf, exec.separator, exec.maxLen, binaryResult)
					if truncated {
						return nil
					}
				}
				first = false
				return payloadFieldIterator(payload, len(exec.argTypes), func(i int, isNull bool, data []byte) error {
					if isNull || truncated || uint64(len(buf)) >= exec.maxLen {
						return nil
					}
					var err error
					buf, err = appendGroupConcatData(buf, exec.argTypes[i], data)
					if uint64(len(buf)) > exec.maxLen {
						buf = truncateGroupConcatBytes(buf, exec.maxLen, binaryResult)
						truncated = true
					}
					return err
				})
			}); err != nil {
				return nil, err
			}
			if err := vector.AppendBytes(vecs[i], buf, false, exec.mp); err != nil {
				return nil, err
			}
		}
	}
	return vecs, nil
}

func appendGroupConcatBytes(dst, src []byte, maxLen uint64, binaryResult bool) ([]byte, bool) {
	if uint64(len(dst)) >= maxLen {
		return dst, len(src) > 0
	}
	remaining := maxLen - uint64(len(dst))
	truncated := uint64(len(src)) > remaining
	if uint64(len(src)) > remaining {
		src = src[:int(remaining)]
	}
	dst = append(dst, src...)
	if truncated {
		dst = truncateGroupConcatBytes(dst, maxLen, binaryResult)
	}
	return dst, truncated
}

func truncateGroupConcatBytes(value []byte, maxLen uint64, binaryResult bool) []byte {
	if uint64(len(value)) > maxLen {
		value = value[:int(maxLen)]
	}
	if !binaryResult {
		for len(value) > 0 && !utf8.Valid(value) {
			value = value[:len(value)-1]
		}
	}
	return value
}

func (exec *groupConcatExec) Size() int64 {
	var size int64
	for _, st := range exec.state {
		size += int64(len(st.argbuf))
		size += int64(cap(st.argCnt)) * 4
	}
	return size + int64(cap(exec.separator)) + exec.distinctHash.Size()
}

func (exec *groupConcatExec) Free() {
	exec.distinctHash.free()
	exec.aggExec.Free()
}

var GroupConcatUnsupportedTypes = []types.T{
	types.T_tuple,
}

func IsGroupConcatSupported(t types.Type) bool {
	for _, unsupported := range GroupConcatUnsupportedTypes {
		if t.Oid == unsupported {
			return false
		}
	}
	return true
}
