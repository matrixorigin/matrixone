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
	"io"
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	mosort "github.com/matrixorigin/matrixone/pkg/sort"
)

// group_concat is a special string aggregation function.
type groupConcatExec struct {
	aggExec
	distinct        bool
	distinctHash    distinctHash
	separator       []byte
	concatArgCnt    int
	orderArgCnt     int
	orderDesc       []bool
	orderNullsLast  []bool
	orderedDistinct []map[string][]byte
}

const (
	groupConcatOrderConfigVersion = byte(1)

	groupConcatOrderAsc        = byte(1)
	groupConcatOrderDesc       = byte(2)
	groupConcatOrderNullsFirst = byte(4)
	groupConcatOrderNullsLast  = byte(8)
	groupConcatOrderFlagMask   = groupConcatOrderAsc |
		groupConcatOrderDesc |
		groupConcatOrderNullsFirst |
		groupConcatOrderNullsLast
)

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
		concatArgCnt: len(info.argTypes),
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
	if exec.distinct && exec.orderArgCnt == 0 {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}
	if err := exec.aggExec.GroupGrow(more); err != nil {
		return err
	}
	if exec.distinct && exec.orderArgCnt > 0 {
		for len(exec.orderedDistinct) < exec.GetNumGroups() {
			exec.orderedDistinct = append(exec.orderedDistinct, nil)
		}
	}
	return nil
}

func (exec *groupConcatExec) PreAllocateGroups(more int) error {
	if exec.distinct && exec.orderArgCnt == 0 {
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
	if len(vectors) != len(exec.argTypes) {
		return moerr.NewInternalErrorNoCtxf(
			"invalid group_concat argument count: got %d, expected %d",
			len(vectors),
			len(exec.argTypes),
		)
	}

	if exec.distinct && exec.orderArgCnt == 0 {
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
	if exec.distinct && exec.orderArgCnt > 0 {
		return exec.fillOrderedDistinct(offset, groups, vectors)
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
	return exec.batchFillOpaqueArgs(offset, groups, payloads, false)
}

type groupConcatDistinctCandidate struct {
	group   int
	key     string
	payload []byte
}

func (exec *groupConcatExec) fillOrderedDistinct(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	candidates := make([]groupConcatDistinctCandidate, 0, len(groups)*2)
	touched := make(map[string]struct{}, len(groups))
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		payload, err := exec.encodePayload(vectors, offset+i)
		if err != nil {
			return err
		}
		if payload == nil {
			continue
		}
		concatPayload, _, err := splitGroupConcatOrderedPayload(payload)
		if err != nil {
			return err
		}
		group := int(grp - 1)
		key := string(concatPayload)
		touchKey := string(binary.BigEndian.AppendUint64(nil, uint64(group))) + key
		if _, ok := touched[touchKey]; !ok {
			touched[touchKey] = struct{}{}
			if existing := exec.orderedDistinct[group][key]; existing != nil {
				candidates = append(candidates, groupConcatDistinctCandidate{
					group: group, key: key, payload: existing,
				})
			}
		}
		candidates = append(candidates, groupConcatDistinctCandidate{
			group: group, key: key, payload: payload,
		})
	}
	return exec.selectOrderedDistinctCandidates(candidates)
}

func (exec *groupConcatExec) selectOrderedDistinctCandidates(
	candidates []groupConcatDistinctCandidate,
) error {
	if len(candidates) == 0 {
		return nil
	}
	entries := make([]groupConcatOrderedEntry, len(candidates))
	for i := range candidates {
		concatPayload, orderPayload, err := splitGroupConcatOrderedPayload(candidates[i].payload)
		if err != nil {
			return err
		}
		entries[i] = groupConcatOrderedEntry{
			concatPayload: concatPayload,
			orderPayload:  orderPayload,
		}
	}
	orderVectors, err := exec.restoreOrderVectors(entries)
	if err != nil {
		return err
	}
	defer func() {
		for _, vec := range orderVectors {
			vec.Free(exec.mp)
		}
	}()
	selectors := make([]int64, len(candidates))
	for i := range selectors {
		selectors[i] = int64(i)
	}
	mosort.SortByVectors(selectors, orderVectors, exec.orderDesc, exec.orderNullsLast)
	selected := make(map[string]struct{}, len(candidates))
	for _, selector := range selectors {
		candidate := candidates[selector]
		selectionKey := string(binary.BigEndian.AppendUint64(nil, uint64(candidate.group))) + candidate.key
		if _, ok := selected[selectionKey]; ok {
			continue
		}
		selected[selectionKey] = struct{}{}
		if exec.orderedDistinct[candidate.group] == nil {
			exec.orderedDistinct[candidate.group] = make(map[string][]byte)
		}
		exec.orderedDistinct[candidate.group][candidate.key] = candidate.payload
	}
	return nil
}

func (exec *groupConcatExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *groupConcatExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*groupConcatExec)
	if exec.distinct && exec.orderArgCnt == 0 {
		if err := exec.distinctHash.merge(&other.distinctHash); err != nil {
			return err
		}
	}
	if exec.distinct && exec.orderArgCnt > 0 {
		candidates := make([]groupConcatDistinctCandidate, 0)
		for i, grp := range groups {
			if grp == GroupNotMatched {
				continue
			}
			sourceGroup := offset + i
			targetGroup := int(grp - 1)
			for key, payload := range other.orderedDistinct[sourceGroup] {
				if existing := exec.orderedDistinct[targetGroup][key]; existing != nil {
					candidates = append(candidates, groupConcatDistinctCandidate{
						group: targetGroup, key: key, payload: existing,
					})
				}
				candidates = append(candidates, groupConcatDistinctCandidate{
					group: targetGroup, key: key, payload: payload,
				})
			}
		}
		return exec.selectOrderedDistinctCandidates(candidates)
	}
	return exec.batchMergeArgs(&other.aggExec, offset, groups, false)
}

func (exec *groupConcatExec) SetExtraInformation(partialResult any, _ int) error {
	if config, ok := partialResult.([]byte); ok {
		exec.concatArgCnt = len(exec.argTypes)
		exec.orderArgCnt = 0
		exec.orderDesc = nil
		exec.orderNullsLast = nil
		exec.separator = config
		exec.retType = GroupConcatReturnType(exec.argTypes)
		return nil
	}
	typedConfig, ok := partialResult.(AggregateConfig)
	if !ok || typedConfig.Type != plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER {
		return moerr.NewInternalErrorNoCtx("invalid group_concat config type")
	}

	concatArgCnt, orderDesc, orderNullsLast, separator, err := decodeGroupConcatOrderConfig(typedConfig.Data)
	if err != nil {
		return err
	}
	if concatArgCnt < 1 || len(orderDesc) < 1 ||
		concatArgCnt+len(orderDesc) != len(exec.argTypes) {
		return moerr.NewInternalErrorNoCtx("invalid group_concat order config")
	}
	exec.concatArgCnt = concatArgCnt
	exec.orderArgCnt = len(orderDesc)
	exec.orderDesc = orderDesc
	exec.orderNullsLast = orderNullsLast
	exec.separator = separator
	exec.retType = GroupConcatReturnType(exec.concatTypes())
	if exec.distinct {
		for len(exec.orderedDistinct) < exec.GetNumGroups() {
			exec.orderedDistinct = append(exec.orderedDistinct, nil)
		}
	}
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
			globalGroup := i*AggBatchSize + j
			empty := st.argCnt[j] == 0
			if exec.distinct && exec.orderArgCnt > 0 {
				empty = len(exec.orderedDistinct[globalGroup]) == 0
			}
			if empty {
				if err := vector.AppendNull(vecs[i], exec.mp); err != nil {
					return nil, err
				}
				continue
			}
			var buf []byte
			var err error
			if exec.distinct && exec.orderArgCnt > 0 {
				buf, err = exec.flushOrderedDistinctGroup(exec.orderedDistinct[globalGroup])
			} else {
				buf, err = exec.flushGroup(st, uint16(j))
			}
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

func (exec *groupConcatExec) flushOrderedDistinctGroup(
	values map[string][]byte,
) ([]byte, error) {
	entries := make([]groupConcatOrderedEntry, 0, len(values))
	for _, payload := range values {
		concatPayload, orderPayload, err := splitGroupConcatOrderedPayload(payload)
		if err != nil {
			return nil, err
		}
		entries = append(entries, groupConcatOrderedEntry{
			concatPayload: concatPayload,
			orderPayload:  orderPayload,
		})
	}
	return exec.flushOrderedEntries(entries, false)
}

func (exec *groupConcatExec) concatTypes() []types.Type {
	return exec.argTypes[:exec.concatArgCnt]
}

func (exec *groupConcatExec) orderTypes() []types.Type {
	return exec.argTypes[exec.concatArgCnt : exec.concatArgCnt+exec.orderArgCnt]
}

func (exec *groupConcatExec) encodePayload(vectors []*vector.Vector, row int) ([]byte, error) {
	concatPayload, err := encodeGroupConcatPayload(
		vectors[:exec.concatArgCnt],
		row,
		exec.concatTypes(),
	)
	if err != nil || concatPayload == nil || exec.orderArgCnt == 0 {
		return concatPayload, err
	}

	orderPayload, err := encodeGroupConcatPayloadWithNulls(
		vectors[exec.concatArgCnt:],
		row,
		exec.orderTypes(),
	)
	if err != nil {
		return nil, err
	}
	return encodeGroupConcatOrderedPayload(concatPayload, orderPayload), nil
}

func encodeGroupConcatOrderedPayload(concatPayload, orderPayload []byte) []byte {
	payload := make([]byte, 4, 4+len(concatPayload)+len(orderPayload))
	binary.BigEndian.PutUint32(payload, uint32(len(concatPayload)))
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
	if err := st.iter(group, func(key []byte) error {
		payload := aggPayloadFromKey(&exec.aggInfo, key)
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

	return exec.flushOrderedEntries(entries, exec.distinct)
}

func (exec *groupConcatExec) flushOrderedEntries(
	entries []groupConcatOrderedEntry,
	deduplicate bool,
) ([]byte, error) {
	orderVectors, err := exec.restoreOrderVectors(entries)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, vec := range orderVectors {
			vec.Free(exec.mp)
		}
	}()

	selectors := make([]int64, len(entries))
	for i := range selectors {
		selectors[i] = int64(i)
	}
	mosort.SortByVectors(selectors, orderVectors, exec.orderDesc, exec.orderNullsLast)

	buf := make([]byte, 0, 64)
	first := true
	var seen map[string]struct{}
	if deduplicate {
		seen = make(map[string]struct{}, len(entries))
	}
	for _, selector := range selectors {
		entry := entries[selector]
		if deduplicate {
			key := string(entry.concatPayload)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
		}
		if !first {
			buf = append(buf, exec.separator...)
		}
		first = false
		buf, err = exec.appendConcatPayload(buf, entry.concatPayload)
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func (exec *groupConcatExec) restoreOrderVectors(
	entries []groupConcatOrderedEntry,
) ([]*vector.Vector, error) {
	orderTypes := exec.orderTypes()
	orderVectors := make([]*vector.Vector, len(orderTypes))
	freeOnError := func() {
		for _, vec := range orderVectors {
			if vec != nil {
				vec.Free(exec.mp)
			}
		}
	}

	for i, typ := range orderTypes {
		orderVectors[i] = vector.NewVec(typ)
		if err := orderVectors[i].PreExtend(len(entries), exec.mp); err != nil {
			freeOnError()
			return nil, err
		}
		orderVectors[i].SetLength(len(entries))
	}

	for row := range entries {
		err := payloadFieldIterator(
			entries[row].orderPayload,
			len(orderTypes),
			func(column int, isNull bool, data []byte) error {
				vec := orderVectors[column]
				if isNull {
					vec.GetNulls().Add(uint64(row))
					return nil
				}
				typ := orderTypes[column]
				if !typ.IsVarlen() && len(data) != typ.TypeSize() {
					return moerr.NewInternalErrorNoCtx("invalid group_concat order payload field size")
				}
				return vec.SetRawBytesAt(row, data, exec.mp)
			},
		)
		if err != nil {
			freeOnError()
			return nil, err
		}
	}
	return orderVectors, nil
}

func (exec *groupConcatExec) flushGroupInInputOrder(st aggState, group uint16) ([]byte, error) {
	buf := make([]byte, 0, 64)
	first := true
	if err := st.iter(group, func(key []byte) error {
		payload := aggPayloadFromKey(&exec.aggInfo, key)
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

func (exec *groupConcatExec) appendConcatPayload(buf, payload []byte) ([]byte, error) {
	err := payloadFieldIterator(
		payload,
		exec.concatArgCnt,
		func(i int, isNull bool, data []byte) error {
			if isNull {
				return nil
			}
			var err error
			buf, err = appendGroupConcatData(buf, exec.argTypes[i], data)
			return err
		},
	)
	return buf, err
}

func decodeGroupConcatOrderConfig(
	config []byte,
) (concatArgCnt int, orderDesc, orderNullsLast []bool, separator []byte, err error) {
	const uint32Size = 4
	const minimumSize = 1 + 3*uint32Size
	if len(config) < minimumSize || config[0] != groupConcatOrderConfigVersion {
		err = moerr.NewInternalErrorNoCtx("invalid group_concat order config")
		return
	}

	pos := 1
	concatArgCnt = int(binary.BigEndian.Uint32(config[pos : pos+uint32Size]))
	pos += uint32Size
	orderArgCnt := int(binary.BigEndian.Uint32(config[pos : pos+uint32Size]))
	pos += uint32Size
	if orderArgCnt > len(config)-pos-uint32Size {
		err = moerr.NewInternalErrorNoCtx("invalid group_concat order config")
		return
	}

	orderDesc = make([]bool, orderArgCnt)
	orderNullsLast = make([]bool, orderArgCnt)
	for i, flag := range config[pos : pos+orderArgCnt] {
		if flag&^groupConcatOrderFlagMask != 0 ||
			flag&groupConcatOrderAsc != 0 && flag&groupConcatOrderDesc != 0 ||
			flag&groupConcatOrderNullsFirst != 0 && flag&groupConcatOrderNullsLast != 0 {
			err = moerr.NewInternalErrorNoCtx("invalid group_concat order flag")
			return
		}
		orderDesc[i] = flag&groupConcatOrderDesc != 0
		switch {
		case flag&groupConcatOrderNullsFirst != 0:
			orderNullsLast[i] = false
		case flag&groupConcatOrderNullsLast != 0:
			orderNullsLast[i] = true
		default:
			orderNullsLast[i] = orderDesc[i]
		}
	}
	pos += orderArgCnt

	separatorSize := int(binary.BigEndian.Uint32(config[pos : pos+uint32Size]))
	pos += uint32Size
	if separatorSize != len(config)-pos {
		err = moerr.NewInternalErrorNoCtx("invalid group_concat order config")
		return
	}
	separator = config[pos:]
	return
}

func (exec *groupConcatExec) Size() int64 {
	var size int64
	for _, st := range exec.state {
		size += int64(len(st.argbuf))
		size += int64(cap(st.argCnt)) * 4
	}
	size += int64(cap(exec.separator))
	size += int64(cap(exec.orderDesc))
	size += int64(cap(exec.orderNullsLast))
	for _, group := range exec.orderedDistinct {
		for key, payload := range group {
			size += int64(len(key) + len(payload))
		}
	}
	return size + exec.distinctHash.Size()
}

func (exec *groupConcatExec) Free() {
	exec.orderedDistinct = nil
	exec.distinctHash.free()
	exec.aggExec.Free()
}

func (exec *groupConcatExec) orderedDistinctState() (*aggExec, error) {
	state := &aggExec{
		mp:        exec.mp,
		aggInfo:   exec.aggInfo,
		chunkSize: exec.chunkSize,
	}
	if err := state.GroupGrow(exec.GetNumGroups()); err != nil {
		state.Free()
		return nil, err
	}
	for group, values := range exec.orderedDistinct {
		x, y := state.getXY(uint64(group))
		for _, payload := range values {
			if err := state.state[x].fillArg(state.mp, y, payload, false); err != nil {
				state.Free()
				return nil, err
			}
		}
	}
	return state, nil
}

func (exec *groupConcatExec) SaveIntermediateResult(
	cnt int64,
	flags [][]uint8,
	buf *bytes.Buffer,
) error {
	if !exec.distinct || exec.orderArgCnt == 0 {
		return exec.aggExec.SaveIntermediateResult(cnt, flags, buf)
	}
	state, err := exec.orderedDistinctState()
	if err != nil {
		return err
	}
	defer state.Free()
	return state.SaveIntermediateResult(cnt, flags, buf)
}

func (exec *groupConcatExec) SaveIntermediateResultOfChunk(
	chunk int,
	buf *bytes.Buffer,
) error {
	if !exec.distinct || exec.orderArgCnt == 0 {
		return exec.aggExec.SaveIntermediateResultOfChunk(chunk, buf)
	}
	state, err := exec.orderedDistinctState()
	if err != nil {
		return err
	}
	defer state.Free()
	return state.SaveIntermediateResultOfChunk(chunk, buf)
}

func (exec *groupConcatExec) UnmarshalFromReader(reader io.Reader, mp *mpool.MPool) error {
	if err := exec.aggExec.UnmarshalFromReader(reader, mp); err != nil {
		return err
	}
	if !exec.distinct || exec.orderArgCnt == 0 {
		return nil
	}
	groupCount := exec.GetNumGroups()
	candidates := make([]groupConcatDistinctCandidate, 0)
	for chunk, st := range exec.state {
		for group := 0; group < int(st.length); group++ {
			globalGroup := chunk*AggBatchSize + group
			err := st.iter(uint16(group), func(key []byte) error {
				payload := bytes.Clone(aggPayloadFromKey(&exec.aggInfo, key))
				concatPayload, _, err := splitGroupConcatOrderedPayload(payload)
				if err != nil {
					return err
				}
				candidates = append(candidates, groupConcatDistinctCandidate{
					group:   globalGroup,
					key:     string(concatPayload),
					payload: payload,
				})
				return nil
			})
			if err != nil {
				return err
			}
		}
	}
	exec.aggExec.Free()
	exec.state = nil
	exec.orderedDistinct = make([]map[string][]byte, groupCount)
	if err := exec.aggExec.GroupGrow(groupCount); err != nil {
		return err
	}
	return exec.selectOrderedDistinctCandidates(candidates)
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
