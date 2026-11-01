// Copyright 2026 Matrix Origin
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

import "github.com/matrixorigin/matrixone/pkg/common/moerr"

// windowPrefixState marks fixed-size states that can be copied to an empty
// output group without arithmetic, consuming the source, or replaying DISTINCT
// arguments. MergePreservesSource alone does not guarantee these properties.
type windowPrefixState interface {
	windowPrefixState() *aggExec
}

func (exec *countStarExec) windowPrefixState() *aggExec         { return &exec.aggExec }
func (exec *countColumnExec) windowPrefixState() *aggExec       { return &exec.aggExec }
func (exec *minMaxExecFixed[T]) windowPrefixState() *aggExec    { return &exec.aggExec }
func (exec *sumAvgExec[T, A]) windowPrefixState() *aggExec      { return &exec.aggExec }
func (exec *sumAvgDecExec[A, S]) windowPrefixState() *aggExec   { return &exec.aggExec }
func (exec *sumDecimal64FastExec) windowPrefixState() *aggExec  { return &exec.aggExec }
func (exec *sumDecimal128FastExec) windowPrefixState() *aggExec { return &exec.aggExec }

func SupportsWindowPrefixSnapshot(exec AggFuncExec) bool {
	_, ok := exec.(windowPrefixState)
	return ok && !exec.IsDistinct()
}

// SnapshotWindowPrefix copies a source group into an empty destination group.
// The caller creates both executors with the same aggregate configuration.
func SnapshotWindowPrefix(dst, src AggFuncExec, dstGroup, srcGroup int) error {
	d, ok := dst.(windowPrefixState)
	if !ok || !SupportsWindowPrefixSnapshot(dst) || !SupportsWindowPrefixSnapshot(src) || dst.AggID() != src.AggID() {
		return moerr.NewInternalErrorNoCtx("aggregate does not support window prefix snapshots")
	}
	s := src.(windowPrefixState)
	dState, sState := d.windowPrefixState(), s.windowPrefixState()
	dx, dy := dState.getXY(uint64(dstGroup))
	sx, sy := sState.getXY(uint64(srcGroup))
	for i, source := range sState.state[sx].vecs {
		target := dState.state[dx].vecs[i]
		if source.GetType().IsVarlen() {
			return moerr.NewInternalErrorNoCtx("variable-length window prefix state")
		}
		if source.IsNull(uint64(sy)) {
			target.SetNull(uint64(dy))
			continue
		}
		if err := target.SetRawBytesAtFromAndUnsetNull(int(dy), source, int(sy), dState.mp); err != nil {
			return err
		}
	}
	return nil
}

// FullPartitionWindowAggregate identifies aggregates without per-output-row
// side effects when every output row sees the same complete frame.
func FullPartitionWindowAggregate(id int64) bool {
	switch id {
	case AggIdOfCountStar, AggIdOfCountColumn, AggIdOfSum, AggIdOfAvg, AggIdOfMin, AggIdOfMax:
		return true
	}
	return false
}
