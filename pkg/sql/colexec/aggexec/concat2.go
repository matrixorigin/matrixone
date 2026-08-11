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
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"io"
	"math"
	"os"
	"slices"
	"unicode/utf8"

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
	distinct         bool
	distinctHash     distinctHash
	separator        []byte
	concatArgCnt     int
	orderArgCnt      int
	orderArgIndexes  []uint32
	orderDesc        []bool
	orderNullsLast   []bool
	orderedDistinct  []map[string][]byte
	h0SpillLimit     int64
	h0SpillContext   context.Context
	h0SpillFile      func() (*os.File, error)
	h0SpillReport    func(int64, int64, int64)
	h0SpillData      *os.File
	orderedSpillRuns [][]groupConcatSpillRun
	maxLen           uint64
}

var (
	groupConcatConfigMagic        = []byte{0xff, 'G', 'C', 1}
	groupConcatOrderedConfigMagic = []byte{0xff, 'G', 'C', 'O', 1}
)

const (
	groupConcatConfigHeaderSize        = 12
	groupConcatOrderedConfigHeaderSize = 13
	groupConcatOrderConfigVersion      = byte(2)
	groupConcatMaxH0RunSize            = int64(8 << 20)
	groupConcatMinRunSize              = int64(64 << 10)
	groupConcatMergeFanIn              = 32

	groupConcatOrderAsc        = byte(1)
	groupConcatOrderDesc       = byte(2)
	groupConcatOrderNullsFirst = byte(4)
	groupConcatOrderNullsLast  = byte(8)
	groupConcatOrderFlagMask   = groupConcatOrderAsc |
		groupConcatOrderDesc |
		groupConcatOrderNullsFirst |
		groupConcatOrderNullsLast
)

func EncodeGroupConcatConfig(separator string, maxLen uint64) []byte {
	config := make([]byte, groupConcatConfigHeaderSize+len(separator))
	copy(config, groupConcatConfigMagic)
	binary.LittleEndian.PutUint64(config[len(groupConcatConfigMagic):], maxLen)
	copy(config[groupConcatConfigHeaderSize:], separator)
	return config
}

func EncodeGroupConcatOrderedConfig(config []byte, maxLen uint64) []byte {
	runtimeConfig := make([]byte, groupConcatOrderedConfigHeaderSize+len(config))
	copy(runtimeConfig, groupConcatOrderedConfigMagic)
	binary.LittleEndian.PutUint64(runtimeConfig[len(groupConcatOrderedConfigMagic):], maxLen)
	copy(runtimeConfig[groupConcatOrderedConfigHeaderSize:], config)
	return runtimeConfig
}

func RefreshGroupConcatConfigMaxLen(config []byte, maxLen uint64) []byte {
	if len(config) >= groupConcatOrderedConfigHeaderSize &&
		bytes.Equal(config[:len(groupConcatOrderedConfigMagic)], groupConcatOrderedConfigMagic) {
		return EncodeGroupConcatOrderedConfig(config[groupConcatOrderedConfigHeaderSize:], maxLen)
	}
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
	result := types.T_text.ToType()
	result.Charset = types.MergeStringCharset(args, result.Charset)
	return result
}

func newGroupConcatExec(mg *mpool.MPool, info multiAggInfo, separator string) AggFuncExec {
	exec := &groupConcatExec{
		distinct:     info.distinct,
		distinctHash: newDistinctHash(mg),
		separator:    []byte(separator),
		concatArgCnt: len(info.argTypes),
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
	if exec.orderArgCnt > 0 {
		for len(exec.orderedSpillRuns) < exec.GetNumGroups() {
			exec.orderedSpillRuns = append(exec.orderedSpillRuns, nil)
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
	if exec.orderArgCnt > 0 && exec.h0SpillLimit > 0 {
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
			x, y := exec.getXY(grp - 1)
			if err = exec.state[x].fillArg(exec.mp, y, payload, false); err != nil {
				return err
			}
			if err = exec.maybeSpillOrdered(); err != nil {
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
	return exec.batchFillOpaqueArgs(offset, groups, payloads, false)
}

func (exec *groupConcatExec) maybeSpillOrdered() error {
	if exec.h0SpillLimit > 0 && exec.activeOrderedMemorySize() >= exec.h0SpillLimit {
		return exec.spillOrderedState(exec.h0SpillContext)
	}
	return nil
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
	const candidateChunkSize = 256
	candidates := make([]groupConcatDistinctCandidate, 0, candidateChunkSize*2)
	touched := make(map[string]struct{}, candidateChunkSize)
	candidateBytes := 0
	flushCandidates := func() error {
		if err := exec.selectOrderedDistinctCandidates(candidates); err != nil {
			return err
		}
		if err := exec.maybeSpillOrdered(); err != nil {
			return err
		}
		candidates = candidates[:0]
		clear(touched)
		candidateBytes = 0
		return nil
	}
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
		candidateBytes += len(key) + len(payload)
		if len(candidates) >= candidateChunkSize || candidateBytes >= 1<<20 {
			if err = flushCandidates(); err != nil {
				return err
			}
		}
	}
	return flushCandidates()
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
	orderVectors, err := exec.restoreOrderVectors(context.Background(), entries)
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
		if err := exec.selectOrderedDistinctCandidates(candidates); err != nil {
			return err
		}
		return exec.maybeSpillOrdered()
	}
	if err := exec.batchMergeArgs(&other.aggExec, offset, groups, false); err != nil {
		return err
	}
	return exec.maybeSpillOrdered()
}

func (exec *groupConcatExec) SetExtraInformation(partialResult any, _ int) error {
	if config, ok := partialResult.([]byte); ok {
		exec.concatArgCnt = len(exec.argTypes)
		exec.orderArgCnt = 0
		exec.orderArgIndexes = nil
		exec.orderDesc = nil
		exec.orderNullsLast = nil
		if len(config) >= groupConcatConfigHeaderSize &&
			bytes.Equal(config[:len(groupConcatConfigMagic)], groupConcatConfigMagic) {
			exec.maxLen = binary.LittleEndian.Uint64(config[len(groupConcatConfigMagic):groupConcatConfigHeaderSize])
			exec.separator = config[groupConcatConfigHeaderSize:]
		} else {
			exec.separator = config
		}
		exec.retType = GroupConcatReturnType(exec.argTypes)
		return nil
	}
	typedConfig, ok := partialResult.(AggregateConfig)
	if !ok || typedConfig.Type != plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER {
		return moerr.NewInternalErrorNoCtx("invalid group_concat config type")
	}

	config := typedConfig.Data
	if len(config) >= groupConcatOrderedConfigHeaderSize &&
		bytes.Equal(config[:len(groupConcatOrderedConfigMagic)], groupConcatOrderedConfigMagic) {
		exec.maxLen = binary.LittleEndian.Uint64(
			config[len(groupConcatOrderedConfigMagic):groupConcatOrderedConfigHeaderSize],
		)
		config = config[groupConcatOrderedConfigHeaderSize:]
	}
	concatArgCnt, orderArgIndexes, orderDesc, orderNullsLast, separator, err :=
		decodeGroupConcatOrderConfig(config)
	if err != nil {
		return err
	}
	if concatArgCnt < 1 || len(orderDesc) < 1 || len(orderArgIndexes) != len(orderDesc) {
		return moerr.NewInternalErrorNoCtx("invalid group_concat order config")
	}
	for _, index := range orderArgIndexes {
		if int(index) >= len(exec.argTypes) {
			return moerr.NewInternalErrorNoCtx("invalid group_concat order argument index")
		}
	}
	exec.concatArgCnt = concatArgCnt
	exec.orderArgCnt = len(orderDesc)
	exec.orderArgIndexes = orderArgIndexes
	exec.orderDesc = orderDesc
	exec.orderNullsLast = orderNullsLast
	exec.separator = separator
	exec.retType = GroupConcatReturnType(exec.concatTypes())
	if exec.distinct {
		for len(exec.orderedDistinct) < exec.GetNumGroups() {
			exec.orderedDistinct = append(exec.orderedDistinct, nil)
		}
	}
	for len(exec.orderedSpillRuns) < exec.GetNumGroups() {
		exec.orderedSpillRuns = append(exec.orderedSpillRuns, nil)
	}
	return nil
}

func (exec *groupConcatExec) Flush() (_ []*vector.Vector, retErr error) {
	return exec.FlushWithContext(context.Background())
}

func (exec *groupConcatExec) FlushWithContext(ctx context.Context) (_ []*vector.Vector, retErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if exec.hasOrderedSpillRuns() {
		if err := exec.spillOrderedState(ctx); err != nil {
			return nil, err
		}
	}
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
			if len(exec.orderedSpillRuns) > globalGroup &&
				len(exec.orderedSpillRuns[globalGroup]) > 0 {
				empty = false
			}
			if empty {
				if err := vector.AppendNull(vecs[i], exec.mp); err != nil {
					return nil, err
				}
				continue
			}
			var buf []byte
			var err error
			if len(exec.orderedSpillRuns) > globalGroup &&
				len(exec.orderedSpillRuns[globalGroup]) > 0 {
				buf, err = exec.flushSpilledGroup(ctx, globalGroup)
			} else if exec.distinct && exec.orderArgCnt > 0 {
				buf, err = exec.flushOrderedDistinctGroup(ctx, exec.orderedDistinct[globalGroup])
			} else {
				buf, err = exec.flushGroup(ctx, st, uint16(j))
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

func (exec *groupConcatExec) hasOrderedSpillRuns() bool {
	for _, runs := range exec.orderedSpillRuns {
		if len(runs) > 0 {
			return true
		}
	}
	return false
}

// ConfigureGroupConcatH0Spill enables bounded ordered-state runs for a final
// ordered aggregation. The historical name is retained for compatibility.
func ConfigureGroupConcatH0Spill(
	agg AggFuncExec,
	limit int64,
	ctx context.Context,
	createFile func() (*os.File, error),
	report func(int64, int64, int64),
) {
	if exec, ok := agg.(*groupConcatExec); ok && exec.orderArgCnt > 0 {
		if ctx == nil {
			ctx = context.Background()
		}
		if limit > groupConcatMaxH0RunSize {
			limit = groupConcatMaxH0RunSize
		}
		if limit < groupConcatMinRunSize {
			limit = groupConcatMinRunSize
		}
		exec.h0SpillLimit = limit
		exec.h0SpillContext = ctx
		exec.h0SpillFile = createFile
		exec.h0SpillReport = report
	}
}

// FlushWithContext lets context-aware aggregates stop expensive finalization.
func FlushWithContext(ctx context.Context, agg AggFuncExec) ([]*vector.Vector, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if contextual, ok := agg.(interface {
		FlushWithContext(context.Context) ([]*vector.Vector, error)
	}); ok {
		return contextual.FlushWithContext(ctx)
	}
	return agg.Flush()
}

func (exec *groupConcatExec) flushOrderedDistinctGroup(
	ctx context.Context,
	values map[string][]byte,
) ([]byte, error) {
	entries := make([]groupConcatOrderedEntry, 0, len(values))
	for _, payload := range values {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		concatPayload, orderPayload, err := splitGroupConcatOrderedPayload(payload)
		if err != nil {
			return nil, err
		}
		entries = append(entries, groupConcatOrderedEntry{
			concatPayload: concatPayload,
			orderPayload:  orderPayload,
		})
	}
	return exec.flushOrderedEntries(ctx, entries, false)
}

func (exec *groupConcatExec) flushSpilledGroup(
	ctx context.Context,
	group int,
) ([]byte, error) {
	if err := exec.compactSpillRuns(ctx, group); err != nil {
		return nil, err
	}
	runs := exec.orderedSpillRuns[group]
	for i := range runs {
		runs[i].pos = runs[i].start
	}
	if len(runs) == 1 {
		buf := make([]byte, 0, 64)
		first := true
		var seen map[string]struct{}
		if exec.distinct {
			seen = make(map[string]struct{})
		}
		for {
			if err := context.Cause(ctx); err != nil {
				return nil, err
			}
			entry, err := readGroupConcatRunEntry(exec.h0SpillData, &runs[0])
			if err != nil {
				return nil, err
			}
			if entry == nil {
				break
			}
			if exec.distinct {
				key := string(entry.concatPayload)
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
			}
			if !first {
				var truncated bool
				buf, truncated = appendGroupConcatBytes(
					buf, exec.separator, exec.maxLen, exec.retType.Oid == types.T_blob,
				)
				if truncated {
					break
				}
			}
			first = false
			var truncated bool
			if buf, truncated, err = exec.appendConcatPayload(buf, entry.concatPayload); err != nil {
				return nil, err
			}
			if truncated {
				break
			}
		}
		return buf, nil
	}
	heads := make([]*groupConcatOrderedEntry, len(runs))
	active := make([]groupConcatOrderedEntry, len(runs))
	for i := range runs {
		entry, err := readGroupConcatRunEntry(exec.h0SpillData, &runs[i])
		if err != nil {
			return nil, err
		}
		heads[i] = entry
		active[i] = *entry
	}
	headVectors, err := exec.restoreOrderVectors(ctx, active)
	if err != nil {
		return nil, err
	}
	defer func() { freeVectors(headVectors, exec.mp) }()
	runHeap := &groupConcatRunHeap{
		runs:      make([]int, len(heads)),
		vectors:   headVectors,
		desc:      exec.orderDesc,
		nullsLast: exec.orderNullsLast,
	}
	for i := range runHeap.runs {
		runHeap.runs[i] = i
	}
	heap.Init(runHeap)

	buf := make([]byte, 0, 64)
	first := true
	var seen map[string]struct{}
	if exec.distinct {
		seen = make(map[string]struct{})
	}
	var replacedOrderBytes int
	for runHeap.Len() > 0 {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		run := heap.Pop(runHeap).(int)
		entry := heads[run]
		if exec.distinct {
			key := string(entry.concatPayload)
			if _, ok := seen[key]; ok {
				heads[run], err = readGroupConcatRunEntry(exec.h0SpillData, &runs[run])
				if err != nil {
					return nil, err
				}
				if heads[run] != nil {
					active[run] = *heads[run]
					if err = exec.setOrderVectorRow(headVectors, run, heads[run].orderPayload); err != nil {
						return nil, err
					}
					replacedOrderBytes += len(heads[run].orderPayload)
					heap.Push(runHeap, run)
				}
				if replacedOrderBytes >= int(groupConcatMaxH0RunSize) {
					refreshed, refreshErr := exec.restoreOrderVectors(ctx, active)
					if refreshErr != nil {
						return nil, refreshErr
					}
					freeVectors(headVectors, exec.mp)
					headVectors = refreshed
					runHeap.vectors = headVectors
					heap.Init(runHeap)
					replacedOrderBytes = 0
				}
				continue
			}
			seen[key] = struct{}{}
		}
		if !first {
			var truncated bool
			buf, truncated = appendGroupConcatBytes(
				buf, exec.separator, exec.maxLen, exec.retType.Oid == types.T_blob,
			)
			if truncated {
				break
			}
		}
		first = false
		var truncated bool
		if buf, truncated, err = exec.appendConcatPayload(buf, entry.concatPayload); err != nil {
			return nil, err
		}
		if truncated {
			break
		}
		heads[run], err = readGroupConcatRunEntry(exec.h0SpillData, &runs[run])
		if err != nil {
			return nil, err
		}
		if heads[run] != nil {
			active[run] = *heads[run]
			if err = exec.setOrderVectorRow(headVectors, run, heads[run].orderPayload); err != nil {
				return nil, err
			}
			replacedOrderBytes += len(heads[run].orderPayload)
			heap.Push(runHeap, run)
		}
		if replacedOrderBytes >= int(groupConcatMaxH0RunSize) {
			refreshed, refreshErr := exec.restoreOrderVectors(ctx, active)
			if refreshErr != nil {
				return nil, refreshErr
			}
			freeVectors(headVectors, exec.mp)
			headVectors = refreshed
			runHeap.vectors = headVectors
			heap.Init(runHeap)
			replacedOrderBytes = 0
		}
	}

	return buf, nil
}

func (exec *groupConcatExec) compactSpillRuns(ctx context.Context, group int) error {
	runs := exec.orderedSpillRuns[group]
	for len(runs) > groupConcatMergeFanIn {
		compacted := make([]groupConcatSpillRun, 0,
			(len(runs)+groupConcatMergeFanIn-1)/groupConcatMergeFanIn)
		for start := 0; start < len(runs); start += groupConcatMergeFanIn {
			end := min(start+groupConcatMergeFanIn, len(runs))
			if end-start == 1 {
				compacted = append(compacted, runs[start])
				continue
			}
			merged, err := exec.mergeSpillRuns(ctx, runs[start:end])
			if err != nil {
				return err
			}
			compacted = append(compacted, merged)
		}
		runs = compacted
	}
	exec.orderedSpillRuns[group] = runs
	return nil
}

func (exec *groupConcatExec) compactSpillRunsIncrementally(
	ctx context.Context,
	group int,
) error {
	runs := exec.orderedSpillRuns[group]
	for len(runs) >= groupConcatMergeFanIn {
		start := len(runs) - groupConcatMergeFanIn
		level := runs[start].level
		sameLevel := true
		for i := start + 1; i < len(runs); i++ {
			if runs[i].level != level {
				sameLevel = false
				break
			}
		}
		if !sameLevel {
			break
		}
		merged, err := exec.mergeSpillRuns(ctx, runs[start:])
		if err != nil {
			return err
		}
		merged.level = level + 1
		runs = append(runs[:start], merged)
	}
	exec.orderedSpillRuns[group] = runs
	return nil
}

func (exec *groupConcatExec) mergeSpillRuns(
	ctx context.Context,
	source []groupConcatSpillRun,
) (groupConcatSpillRun, error) {
	runs := slices.Clone(source)
	heads := make([]*groupConcatOrderedEntry, len(runs))
	active := make([]groupConcatOrderedEntry, len(runs))
	for i := range runs {
		runs[i].pos = runs[i].start
		entry, err := readGroupConcatRunEntry(exec.h0SpillData, &runs[i])
		if err != nil {
			return groupConcatSpillRun{}, err
		}
		heads[i] = entry
		active[i] = *entry
	}
	headVectors, err := exec.restoreOrderVectors(ctx, active)
	if err != nil {
		return groupConcatSpillRun{}, err
	}
	defer func() { freeVectors(headVectors, exec.mp) }()
	runHeap := &groupConcatRunHeap{
		runs:      make([]int, len(runs)),
		vectors:   headVectors,
		desc:      exec.orderDesc,
		nullsLast: exec.orderNullsLast,
	}
	for i := range runHeap.runs {
		runHeap.runs[i] = i
	}
	heap.Init(runHeap)

	start, err := exec.h0SpillData.Seek(0, io.SeekEnd)
	if err != nil {
		return groupConcatSpillRun{}, err
	}
	writer := bufio.NewWriterSize(exec.h0SpillData, 64*1024)
	var size [4]byte
	var rows int64
	var replacedOrderBytes int
	for runHeap.Len() > 0 {
		if err = context.Cause(ctx); err != nil {
			return groupConcatSpillRun{}, err
		}
		run := heap.Pop(runHeap).(int)
		entry := heads[run]
		payload := encodeGroupConcatOrderedPayload(entry.concatPayload, entry.orderPayload)
		binary.BigEndian.PutUint32(size[:], uint32(len(payload)))
		if _, err = writer.Write(size[:]); err != nil {
			return groupConcatSpillRun{}, err
		}
		if _, err = writer.Write(payload); err != nil {
			return groupConcatSpillRun{}, err
		}
		rows++
		heads[run], err = readGroupConcatRunEntry(exec.h0SpillData, &runs[run])
		if err != nil {
			return groupConcatSpillRun{}, err
		}
		if heads[run] != nil {
			active[run] = *heads[run]
			if err = exec.setOrderVectorRow(headVectors, run, heads[run].orderPayload); err != nil {
				return groupConcatSpillRun{}, err
			}
			replacedOrderBytes += len(heads[run].orderPayload)
			heap.Push(runHeap, run)
		}
		if replacedOrderBytes >= int(groupConcatMaxH0RunSize) {
			refreshed, refreshErr := exec.restoreOrderVectors(ctx, active)
			if refreshErr != nil {
				return groupConcatSpillRun{}, refreshErr
			}
			freeVectors(headVectors, exec.mp)
			headVectors = refreshed
			runHeap.vectors = headVectors
			heap.Init(runHeap)
			replacedOrderBytes = 0
		}
	}
	if err = writer.Flush(); err != nil {
		return groupConcatSpillRun{}, err
	}
	end, err := exec.h0SpillData.Seek(0, io.SeekCurrent)
	if err != nil {
		return groupConcatSpillRun{}, err
	}
	if exec.h0SpillReport != nil {
		exec.h0SpillReport(end-start, rows, exec.Size())
	}
	return groupConcatSpillRun{start: start, end: end, pos: start}, nil
}

func readGroupConcatRunEntry(
	file *os.File,
	run *groupConcatSpillRun,
) (*groupConcatOrderedEntry, error) {
	if run.pos >= run.end {
		return nil, nil
	}
	var size [4]byte
	if n, err := file.ReadAt(size[:], run.pos); err != nil {
		if n != 0 {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	run.pos += int64(len(size))
	payloadSize := int(binary.BigEndian.Uint32(size[:]))
	if payloadSize < 0 || int64(payloadSize) > run.end-run.pos {
		return nil, moerr.NewInternalErrorNoCtx("invalid group_concat spill run")
	}
	payload := make([]byte, payloadSize)
	if _, err := file.ReadAt(payload, run.pos); err != nil {
		return nil, err
	}
	run.pos += int64(payloadSize)
	concatPayload, orderPayload, err := splitGroupConcatOrderedPayload(payload)
	if err != nil {
		return nil, err
	}
	return &groupConcatOrderedEntry{
		concatPayload: concatPayload,
		orderPayload:  orderPayload,
	}, nil
}

func (exec *groupConcatExec) concatTypes() []types.Type {
	return exec.argTypes[:exec.concatArgCnt]
}

func (exec *groupConcatExec) orderTypes() []types.Type {
	result := make([]types.Type, len(exec.orderArgIndexes))
	for i, index := range exec.orderArgIndexes {
		result[i] = exec.argTypes[index]
	}
	return result
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

	orderVectors := make([]*vector.Vector, len(exec.orderArgIndexes))
	for i, index := range exec.orderArgIndexes {
		orderVectors[i] = vectors[index]
	}
	orderPayload, err := encodeGroupConcatPayloadWithNulls(orderVectors, row, exec.orderTypes())
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

type groupConcatSpillRun struct {
	start int64
	end   int64
	pos   int64
	level int64
}

type groupConcatRunHeap struct {
	runs      []int
	vectors   []*vector.Vector
	desc      []bool
	nullsLast []bool
}

func (h groupConcatRunHeap) Len() int { return len(h.runs) }
func (h groupConcatRunHeap) Less(i, j int) bool {
	left, right := int64(h.runs[i]), int64(h.runs[j])
	pair := [2]int64{left, right}
	mosort.SortByVectors(pair[:], h.vectors, h.desc, h.nullsLast)
	if pair[0] != left {
		return false
	}
	reversed := [2]int64{right, left}
	mosort.SortByVectors(reversed[:], h.vectors, h.desc, h.nullsLast)
	if reversed[0] == right {
		return left < right
	}
	return true
}
func (h groupConcatRunHeap) Swap(i, j int) { h.runs[i], h.runs[j] = h.runs[j], h.runs[i] }
func (h *groupConcatRunHeap) Push(value any) {
	h.runs = append(h.runs, value.(int))
}
func (h *groupConcatRunHeap) Pop() any {
	last := len(h.runs) - 1
	value := h.runs[last]
	h.runs = h.runs[:last]
	return value
}

func (exec *groupConcatExec) spillOrderedState(ctx context.Context) error {
	if exec.h0SpillFile == nil {
		return nil
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	groupCount := exec.GetNumGroups()
	for group := 0; group < groupCount; group++ {
		var entries []groupConcatOrderedEntry
		if exec.distinct {
			values := exec.orderedDistinct[group]
			entries = make([]groupConcatOrderedEntry, 0, len(values))
			for _, payload := range values {
				concatPayload, orderPayload, err := splitGroupConcatOrderedPayload(payload)
				if err != nil {
					return err
				}
				entries = append(entries, groupConcatOrderedEntry{
					concatPayload: concatPayload,
					orderPayload:  orderPayload,
				})
			}
		} else {
			x, y := exec.getXY(uint64(group))
			var err error
			entries, err = exec.orderedEntries(ctx, exec.state[x], y)
			if err != nil {
				return err
			}
		}
		if len(entries) > 0 {
			if err := exec.writeOrderedRun(ctx, group, entries); err != nil {
				return err
			}
			if err := exec.compactSpillRunsIncrementally(ctx, group); err != nil {
				return err
			}
		}
	}
	for i := range exec.state {
		exec.state[i].free(exec.mp)
	}
	exec.state = nil
	if exec.distinct {
		exec.orderedDistinct = make([]map[string][]byte, groupCount)
	}
	return exec.aggExec.GroupGrow(groupCount)
}

func (exec *groupConcatExec) writeOrderedRun(
	ctx context.Context,
	group int,
	entries []groupConcatOrderedEntry,
) error {
	selectors, vectors, err := exec.sortOrderedEntries(ctx, entries)
	if err != nil {
		return err
	}
	defer freeVectors(vectors, exec.mp)

	if exec.h0SpillData == nil {
		exec.h0SpillData, err = exec.h0SpillFile()
		if err != nil {
			return err
		}
	}
	file := exec.h0SpillData
	start, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	writer := bufio.NewWriterSize(file, 64*1024)
	var size [4]byte
	for _, selector := range selectors {
		if err = context.Cause(ctx); err != nil {
			return err
		}
		entry := entries[selector]
		payload := encodeGroupConcatOrderedPayload(entry.concatPayload, entry.orderPayload)
		binary.BigEndian.PutUint32(size[:], uint32(len(payload)))
		if _, err = writer.Write(size[:]); err != nil {
			return err
		}
		if _, err = writer.Write(payload); err != nil {
			return err
		}
	}
	if err = writer.Flush(); err != nil {
		return err
	}
	end, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	exec.orderedSpillRuns[group] = append(exec.orderedSpillRuns[group], groupConcatSpillRun{
		start: start,
		end:   end,
		pos:   start,
	})
	if exec.h0SpillReport != nil {
		exec.h0SpillReport(end-start, int64(len(entries)), exec.Size())
	}
	return nil
}

func (exec *groupConcatExec) orderedEntries(
	ctx context.Context,
	st aggState,
	group uint16,
) ([]groupConcatOrderedEntry, error) {
	entries := make([]groupConcatOrderedEntry, 0, st.argCnt[group])
	err := st.iter(group, func(key []byte) error {
		if err := context.Cause(ctx); err != nil {
			return err
		}
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
	})
	return entries, err
}

func (exec *groupConcatExec) flushGroup(
	ctx context.Context,
	st aggState,
	group uint16,
) ([]byte, error) {
	if exec.orderArgCnt == 0 {
		return exec.flushGroupInInputOrder(st, group)
	}

	entries, err := exec.orderedEntries(ctx, st, group)
	if err != nil {
		return nil, err
	}

	return exec.flushOrderedEntries(ctx, entries, exec.distinct)
}

func (exec *groupConcatExec) flushOrderedEntries(
	ctx context.Context,
	entries []groupConcatOrderedEntry,
	deduplicate bool,
) ([]byte, error) {
	selectors, orderVectors, err := exec.sortOrderedEntries(ctx, entries)
	if err != nil {
		return nil, err
	}
	defer freeVectors(orderVectors, exec.mp)

	buf := make([]byte, 0, 64)
	first := true
	var seen map[string]struct{}
	if deduplicate {
		seen = make(map[string]struct{}, len(entries))
	}
	for _, selector := range selectors {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		entry := entries[selector]
		if deduplicate {
			key := string(entry.concatPayload)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
		}
		if !first {
			var truncated bool
			buf, truncated = appendGroupConcatBytes(
				buf, exec.separator, exec.maxLen, exec.retType.Oid == types.T_blob,
			)
			if truncated {
				break
			}
		}
		first = false
		var truncated bool
		buf, truncated, err = exec.appendConcatPayload(buf, entry.concatPayload)
		if err != nil {
			return nil, err
		}
		if truncated {
			break
		}
	}
	return buf, nil
}

func (exec *groupConcatExec) sortOrderedEntries(
	ctx context.Context,
	entries []groupConcatOrderedEntry,
) ([]int64, []*vector.Vector, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, nil, err
	}
	orderVectors, err := exec.restoreOrderVectors(ctx, entries)
	if err != nil {
		return nil, nil, err
	}
	selectors := make([]int64, len(entries))
	for i := range selectors {
		if i&1023 == 0 {
			if err := context.Cause(ctx); err != nil {
				freeVectors(orderVectors, exec.mp)
				return nil, nil, err
			}
		}
		selectors[i] = int64(i)
	}
	mosort.SortByVectors(selectors, orderVectors, exec.orderDesc, exec.orderNullsLast)
	return selectors, orderVectors, nil
}

func freeVectors(vectors []*vector.Vector, mp *mpool.MPool) {
	for _, vec := range vectors {
		vec.Free(mp)
	}
}

func (exec *groupConcatExec) restoreOrderVectors(
	ctx context.Context,
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
		if row&1023 == 0 {
			if err := context.Cause(ctx); err != nil {
				freeOnError()
				return nil, err
			}
		}
		if err := exec.setOrderVectorRow(orderVectors, row, entries[row].orderPayload); err != nil {
			freeOnError()
			return nil, err
		}
	}
	return orderVectors, nil
}

func (exec *groupConcatExec) setOrderVectorRow(
	orderVectors []*vector.Vector,
	row int,
	payload []byte,
) error {
	orderTypes := exec.orderTypes()
	return payloadFieldIterator(
		payload,
		len(orderTypes),
		func(column int, isNull bool, data []byte) error {
			vec := orderVectors[column]
			vec.GetNulls().Del(uint64(row))
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
}

func (exec *groupConcatExec) flushGroupInInputOrder(st aggState, group uint16) ([]byte, error) {
	buf := make([]byte, 0, 64)
	first := true
	truncated := false
	if err := st.iter(group, func(key []byte) error {
		if truncated {
			return nil
		}
		payload := aggPayloadFromKey(&exec.aggInfo, key)
		if !first {
			buf, truncated = appendGroupConcatBytes(
				buf, exec.separator, exec.maxLen, exec.retType.Oid == types.T_blob,
			)
			if truncated {
				return nil
			}
		}
		first = false
		var (
			err              error
			payloadTruncated bool
		)
		buf, payloadTruncated, err = exec.appendConcatPayload(buf, payload)
		if err != nil {
			return err
		}
		if payloadTruncated {
			truncated = true
			return nil
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return buf, nil
}

func (exec *groupConcatExec) appendConcatPayload(
	buf, payload []byte,
) ([]byte, bool, error) {
	truncated := false
	err := payloadFieldIterator(
		payload,
		exec.concatArgCnt,
		func(i int, isNull bool, data []byte) error {
			if isNull || truncated {
				return nil
			}
			var err error
			buf, err = appendGroupConcatData(buf, exec.argTypes[i], data)
			if uint64(len(buf)) > exec.maxLen {
				truncated = true
				buf = truncateGroupConcatBytes(
					buf,
					exec.maxLen,
					exec.retType.Oid == types.T_blob,
				)
			}
			return err
		},
	)
	return buf, truncated, err
}

func appendGroupConcatBytes(dst, src []byte, maxLen uint64, binaryResult bool) ([]byte, bool) {
	if uint64(len(dst)) >= maxLen {
		return dst, len(src) > 0
	}
	remaining := maxLen - uint64(len(dst))
	truncated := uint64(len(src)) > remaining
	if truncated {
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

func decodeGroupConcatOrderConfig(
	config []byte,
) (
	concatArgCnt int,
	orderArgIndexes []uint32,
	orderDesc, orderNullsLast []bool,
	separator []byte,
	err error,
) {
	const uint32Size = 4
	const minimumSize = 1 + 3*uint32Size
	if len(config) < minimumSize || (config[0] != 1 && config[0] != groupConcatOrderConfigVersion) {
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
	orderArgIndexes = make([]uint32, orderArgCnt)
	if config[0] == 1 {
		for i := range orderArgIndexes {
			orderArgIndexes[i] = uint32(concatArgCnt + i)
		}
	} else {
		if orderArgCnt > (len(config)-pos-uint32Size)/uint32Size {
			err = moerr.NewInternalErrorNoCtx("invalid group_concat order config")
			return
		}
		for i := range orderArgIndexes {
			orderArgIndexes[i] = binary.BigEndian.Uint32(config[pos : pos+uint32Size])
			pos += uint32Size
		}
	}

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
	size := exec.activeOrderedMemorySize() + exec.fixedAndSpilledMemorySize() + exec.distinctHash.Size()
	for _, st := range exec.state {
		size += int64(cap(st.argCnt)) * 4
	}
	return size
}

// activeOrderedMemorySize reports state which has not been written to an
// ordered spill run yet. Only this state participates in the spill watermark:
// retained run descriptors remain part of Size(), but must not force every
// subsequent input row into its own run.
func (exec *groupConcatExec) activeOrderedMemorySize() int64 {
	var size int64
	for _, st := range exec.state {
		size += int64(len(st.argbuf))
	}
	for _, group := range exec.orderedDistinct {
		// Account for the Go map bucket and string/slice headers in addition to
		// the payload bytes. These allocations live outside the operator mpool.
		size += int64(len(group)) * 64
		for key, payload := range group {
			size += int64(len(key) + len(payload))
		}
	}
	return size
}

// AdditionalMemorySize reports retained Go-heap state which is invisible to
// the operator mpool and therefore must be added to EXPLAIN memory accounting.
func (exec *groupConcatExec) AdditionalMemorySize() int64 {
	return exec.activeOrderedDistinctMemorySize() + exec.fixedAndSpilledMemorySize()
}

func (exec *groupConcatExec) activeOrderedDistinctMemorySize() int64 {
	var size int64
	for _, group := range exec.orderedDistinct {
		// Account for the Go map bucket and string/slice headers in addition to
		// the payload bytes. These allocations live outside the operator mpool.
		size += int64(len(group)) * 64
		for key, payload := range group {
			size += int64(len(key) + len(payload))
		}
	}
	return size
}

func (exec *groupConcatExec) fixedAndSpilledMemorySize() int64 {
	var size int64
	size += int64(cap(exec.separator))
	size += int64(cap(exec.orderDesc))
	size += int64(cap(exec.orderNullsLast))
	size += int64(cap(exec.orderArgIndexes)) * 4
	for _, runs := range exec.orderedSpillRuns {
		size += int64(cap(runs)) * int64(4*8)
	}
	return size
}

func (exec *groupConcatExec) Free() {
	if exec.h0SpillData != nil {
		_ = exec.h0SpillData.Close()
	}
	exec.h0SpillData = nil
	exec.orderedSpillRuns = nil
	exec.h0SpillLimit = 0
	exec.h0SpillContext = nil
	exec.h0SpillFile = nil
	exec.h0SpillReport = nil
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
	if exec.hasOrderedSpillRuns() {
		return moerr.NewInternalErrorNoCtx("spilled final group_concat cannot be serialized as a partial result")
	}
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
	if exec.hasOrderedSpillRuns() {
		return moerr.NewInternalErrorNoCtx("spilled final group_concat cannot be serialized as a partial result")
	}
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
