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
	"sort"

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
	concatArgCnt int
	orderArgCnt  int
	orderDesc    []bool
}

const groupConcatOrderConfigMagic = "\x00GCORDER1"

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
	concatArgTypes := exec.concatTypes()
	concatVectors := vectors[:len(concatArgTypes)]
	if exec.distinct {
		for i, grp := range groups {
			if grp == GroupNotMatched {
				continue
			}
			row := offset + i
			payload, err := exec.encodePayload(vectors, row)
			if err != nil {
				return err
			}
			if payload == nil {
				continue
			}
			need, err := exec.distinctHash.fill(int(grp-1), concatVectors, row)
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
		payload, err := exec.encodePayload(vectors, offset+i)
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
	config := partialResult.([]byte)
	if !bytes.HasPrefix(config, []byte(groupConcatOrderConfigMagic)) {
		exec.separator = config
		return nil
	}

	concatArgCnt, orderDesc, separator, err := decodeGroupConcatOrderConfig(config)
	if err != nil {
		return err
	}
	if concatArgCnt < 1 || concatArgCnt+len(orderDesc) != len(exec.argTypes) {
		return moerr.NewInternalErrorNoCtx("invalid group_concat order config")
	}
	exec.concatArgCnt = concatArgCnt
	exec.orderArgCnt = len(orderDesc)
	exec.orderDesc = orderDesc
	exec.separator = separator
	exec.retType = GroupConcatReturnType(exec.concatTypes())
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
			buf, err := exec.flushGroup(st, uint16(j))
			if err != nil {
				return nil, err
			}
			if err := vector.AppendBytes(vecs[i], buf, false, exec.mp); err != nil {
				return nil, err
			}
		}
	}
	return vecs, nil
}

func (exec *groupConcatExec) concatTypes() []types.Type {
	if exec.concatArgCnt > 0 {
		return exec.argTypes[:exec.concatArgCnt]
	}
	return exec.argTypes
}

func (exec *groupConcatExec) orderTypes() []types.Type {
	if exec.orderArgCnt == 0 {
		return nil
	}
	return exec.argTypes[exec.concatArgCnt : exec.concatArgCnt+exec.orderArgCnt]
}

func (exec *groupConcatExec) encodePayload(vectors []*vector.Vector, row int) ([]byte, error) {
	concatArgTypes := exec.concatTypes()
	concatPayload, err := encodeGroupConcatPayload(vectors[:len(concatArgTypes)], row, concatArgTypes)
	if err != nil || concatPayload == nil || exec.orderArgCnt == 0 {
		return concatPayload, err
	}
	orderPayload, err := encodeGroupConcatPayloadWithNulls(
		vectors[exec.concatArgCnt:exec.concatArgCnt+exec.orderArgCnt],
		row,
		exec.orderTypes(),
	)
	if err != nil {
		return nil, err
	}
	return encodeGroupConcatOrderedPayload(concatPayload, orderPayload), nil
}

func encodeGroupConcatOrderedPayload(concatPayload, orderPayload []byte) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(len(concatPayload)))
	payload := make([]byte, 0, 4+len(concatPayload)+len(orderPayload))
	payload = append(payload, buf[:]...)
	payload = append(payload, concatPayload...)
	payload = append(payload, orderPayload...)
	return payload
}

func splitGroupConcatOrderedPayload(payload []byte) ([]byte, []byte, error) {
	if len(payload) < 4 {
		return nil, nil, moerr.NewInternalErrorNoCtx("invalid group_concat ordered payload")
	}
	concatLen := int(binary.BigEndian.Uint32(payload[:4]))
	if concatLen > len(payload)-4 {
		return nil, nil, moerr.NewInternalErrorNoCtx("invalid group_concat ordered payload")
	}
	return payload[4 : 4+concatLen], payload[4+concatLen:], nil
}

type groupConcatOrderedEntry struct {
	concatPayload []byte
	orderPayload  []byte
}

func (exec *groupConcatExec) flushGroup(st aggState, group uint16) ([]byte, error) {
	if exec.orderArgCnt == 0 {
		return exec.flushGroupInInputOrder(st, group)
	}

	entries := make([]groupConcatOrderedEntry, 0, st.argCnt[group])
	if err := st.iter(group, func(k []byte) error {
		payload := aggPayloadFromKey(&exec.aggInfo, k)
		concatPayload, orderPayload, err := splitGroupConcatOrderedPayload(payload)
		if err != nil {
			return err
		}
		entries = append(entries, groupConcatOrderedEntry{
			concatPayload: concatPayload,
			orderPayload:  orderPayload,
		})
		return nil
	}); err != nil {
		return nil, err
	}

	var sortErr error
	sort.SliceStable(entries, func(i, j int) bool {
		if sortErr != nil {
			return false
		}
		cmp, err := exec.compareOrderPayload(entries[i].orderPayload, entries[j].orderPayload)
		if err != nil {
			sortErr = err
			return false
		}
		return cmp < 0
	})
	if sortErr != nil {
		return nil, sortErr
	}

	buf := make([]byte, 0, 64)
	for i, entry := range entries {
		if i > 0 {
			buf = append(buf, exec.separator...)
		}
		var err error
		buf, err = exec.appendConcatPayload(buf, entry.concatPayload)
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func (exec *groupConcatExec) flushGroupInInputOrder(st aggState, group uint16) ([]byte, error) {
	buf := make([]byte, 0, 64)
	first := true
	if err := st.iter(group, func(k []byte) error {
		payload := aggPayloadFromKey(&exec.aggInfo, k)
		if !first {
			buf = append(buf, exec.separator...)
		}
		first = false
		var err error
		buf, err = exec.appendConcatPayload(buf, payload)
		return err
	}); err != nil {
		return nil, err
	}
	return buf, nil
}

func (exec *groupConcatExec) appendConcatPayload(buf []byte, payload []byte) ([]byte, error) {
	concatArgTypes := exec.concatTypes()
	err := payloadFieldIterator(payload, len(concatArgTypes), func(i int, isNull bool, data []byte) error {
		if isNull {
			return nil
		}
		var err error
		buf, err = appendGroupConcatData(buf, concatArgTypes[i], data)
		return err
	})
	return buf, err
}

type groupConcatPayloadField struct {
	isNull bool
	data   []byte
}

func decodeGroupConcatPayloadFields(payload []byte, fieldCount int) ([]groupConcatPayloadField, error) {
	fields := make([]groupConcatPayloadField, 0, fieldCount)
	err := payloadFieldIterator(payload, fieldCount, func(_ int, isNull bool, data []byte) error {
		fields = append(fields, groupConcatPayloadField{
			isNull: isNull,
			data:   data,
		})
		return nil
	})
	return fields, err
}

func (exec *groupConcatExec) compareOrderPayload(left, right []byte) (int, error) {
	orderTypes := exec.orderTypes()
	leftFields, err := decodeGroupConcatPayloadFields(left, len(orderTypes))
	if err != nil {
		return 0, err
	}
	rightFields, err := decodeGroupConcatPayloadFields(right, len(orderTypes))
	if err != nil {
		return 0, err
	}

	for i, typ := range orderTypes {
		var leftValue any
		if !leftFields[i].isNull {
			leftValue = types.DecodeValue(leftFields[i].data, typ.Oid)
		}
		var rightValue any
		if !rightFields[i].isNull {
			rightValue = types.DecodeValue(rightFields[i].data, typ.Oid)
		}
		cmp := compareGroupConcatOrderValue(leftValue, rightValue)
		if exec.orderDesc[i] {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	return 0, nil
}

func compareGroupConcatOrderValue(left, right any) int {
	if left == nil || right == nil {
		return types.CompareValue(left, right)
	}
	switch l := left.(type) {
	case int8:
		r := right.(int8)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case int16:
		r := right.(int16)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case int32:
		r := right.(int32)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case int64:
		r := right.(int64)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case uint8:
		r := right.(uint8)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case uint16:
		r := right.(uint16)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case uint32:
		r := right.(uint32)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case uint64:
		r := right.(uint64)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case float32:
		r := right.(float32)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case float64:
		r := right.(float64)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case types.Date:
		r := right.(types.Date)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case types.Time:
		r := right.(types.Time)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case types.Timestamp:
		r := right.(types.Timestamp)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case types.Datetime:
		r := right.(types.Datetime)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case types.MoYear:
		r := right.(types.MoYear)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case types.Enum:
		r := right.(types.Enum)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	default:
		return types.CompareValue(left, right)
	}
}

func decodeGroupConcatOrderConfig(config []byte) (int, []bool, []byte, error) {
	if len(config) < len(groupConcatOrderConfigMagic)+12 {
		return 0, nil, nil, moerr.NewInternalErrorNoCtx("invalid group_concat order config")
	}
	pos := len(groupConcatOrderConfigMagic)
	concatArgCnt := int(binary.BigEndian.Uint32(config[pos : pos+4]))
	pos += 4
	orderArgCnt := int(binary.BigEndian.Uint32(config[pos : pos+4]))
	pos += 4
	if orderArgCnt < 1 || len(config) < pos+orderArgCnt+4 {
		return 0, nil, nil, moerr.NewInternalErrorNoCtx("invalid group_concat order config")
	}
	orderDesc := make([]bool, orderArgCnt)
	for i, flag := range config[pos : pos+orderArgCnt] {
		switch flag {
		case 0:
			orderDesc[i] = false
		case 1:
			orderDesc[i] = true
		default:
			return 0, nil, nil, moerr.NewInternalErrorNoCtx("invalid group_concat order config")
		}
	}
	pos += orderArgCnt
	separatorLen := int(binary.BigEndian.Uint32(config[pos : pos+4]))
	pos += 4
	if separatorLen != len(config)-pos {
		return 0, nil, nil, moerr.NewInternalErrorNoCtx("invalid group_concat order config")
	}
	return concatArgCnt, orderDesc, config[pos:], nil
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
