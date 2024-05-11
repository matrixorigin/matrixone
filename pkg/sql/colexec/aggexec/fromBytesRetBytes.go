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
    "github.com/matrixorigin/matrixone/pkg/common/mpool"
    "github.com/matrixorigin/matrixone/pkg/container/vector"
)

// singleAggFuncExecNew4 is the agg executor for single-column aggregation function
// with fixed input and output types were both var-len types.
type singleAggFuncExecNew4 struct {
    singleAggInfo
    singleAggExecExtraInformation
    distinctHash

    arg sBytesArg
    ret aggFuncBytesResult

    execContext *AggContext

    fill SingleAggFill4NewVersion
    fills SingleAggFills4NewVersion
    merge SingleAggMerge4NewVersion
    flush SingleAggFlush4NewVersion
}

func (exec *singleAggFuncExecNew4) marshal() ([]byte, error) {
    d := exec.singleAggInfo.getEncoded()
    r, err := exec.ret.marshal()
    if err != nil {
        return nil, err
    }
    encoded := &EncodedAgg{
        Info:   d,
        Result: r,
        Groups: exec.execContext.getGroupContextEncodings(),
    }
    return encoded.Marshal()
}

func (exec *singleAggFuncExecNew4) unmarshal(mp *mpool.MPool, result []byte, groups [][]byte) error {
    exec.execContext.decodeGroupContexts(groups, exec.singleAggInfo.retType, exec.singleAggInfo.argType)
    return exec.ret.unmarshal(result)
}

func (exec *singleAggFuncExecNew4) init(
    mg AggMemoryManager,
    info singleAggInfo,
    impl aggImplementation) {

    if info.IsDistinct() {
        exec.distinctHash = newDistinctHash(mg.Mp(), false)
    }

    if resultInitMethod := impl.logic.init; resultInitMethod != nil {
        v := resultInitMethod.(SingleAggInitResultVar)(info.retType, info.argType)
        exec.ret = initBytesAggFuncResult2(mg, info.retType, info.emptyNull, v)
    } else {
        exec.ret = initBytesAggFuncResult(mg, info.retType, info.emptyNull)
    }

    exec.singleAggInfo = info
    exec.singleAggExecExtraInformation = emptyExtraInfo
    exec.execContext = newAggContextFromImpl(impl.ctx, info.retType, info.argType)

    if flushMethod := impl.logic.flush; flushMethod != nil {
        exec.flush = flushMethod.(SingleAggFlush4NewVersion)
    }

    exec.fill = impl.logic.fill.(SingleAggFill4NewVersion)
    exec.fills = impl.logic.fills.(SingleAggFills4NewVersion)
    exec.merge = impl.logic.merge.(SingleAggMerge4NewVersion)
}

func (exec *singleAggFuncExecNew4) GroupGrow(more int) error {
    if err := exec.ret.grows(more); err != nil {
        return err
    }
    // deal with distinct hash.
    if exec.IsDistinct() {
        if err := exec.distinctHash.grows(more); err != nil {
            return err
        }
    }
    // deal with execContext.
    exec.execContext.growsGroupContext(more, exec.singleAggInfo.retType, exec.singleAggInfo.argType)
    return nil
}

func (exec *singleAggFuncExecNew4) PreAllocateGroups(more int) error {
    exec.execContext.preAllocate(more)
    return exec.ret.preAllocate(more)
}

func (exec *singleAggFuncExecNew4) Fill(
    group int, row int, vectors []*vector.Vector) error {
    if vectors[0].IsNull(uint64(row)) {
        return nil
    }

    if vectors[0].IsConst() {
        row = 0
    }

    if exec.IsDistinct() {
        if need, err := exec.distinctHash.fill(group, vectors, row); !need || err != nil {
            return err
        }
    }

    exec.ret.groupToSet = group
    exec.ret.setGroupNotEmpty(group)

    return exec.fill(
        exec.execContext.getGroupContext(group),
        exec.execContext.getCommonContext(),
        vectors[0].GetBytesAt(row),
        exec.ret.aggGet, exec.ret.aggSet)
}
