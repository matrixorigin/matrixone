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

package function

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func moAssertSnapshotAfter(
	inputs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selected *FunctionSelectList,
) error {
	args := vector.GenerateFunctionStrParameter(inputs[0])
	out := vector.MustFunctionResult[bool](result)
	var snapshot timestamp.Timestamp
	for row := uint64(0); row < uint64(length); row++ {
		if selected != nil && (selected.IgnoreAllRow() || (!selected.ShouldEvalAllRow() && selected.Contains(row))) {
			if err := out.Append(false, true); err != nil {
				return err
			}
			continue
		}
		if snapshot.IsEmpty() {
			var err error
			snapshot, err = localExplicitSnapshot(proc)
			if err != nil {
				return err
			}
		}
		value, null := args.GetStrValue(row)
		if null {
			return moerr.NewInvalidInput(proc.Ctx, "mo_assert_snapshot_after requires a non-NULL HLC watermark")
		}
		if err := assertSnapshotAfter(proc.Ctx, snapshot, string(value)); err != nil {
			return err
		}
		if err := out.Append(true, false); err != nil {
			return err
		}
	}
	return nil
}

func localExplicitSnapshot(proc *process.Process) (timestamp.Timestamp, error) {
	if remote, _ := proc.Ctx.Value(defines.RemoteRunContext{}).(bool); remote {
		return timestamp.Timestamp{}, moerr.NewNotSupported(proc.Ctx, "mo_assert_snapshot_after must run on the transaction coordinator")
	}
	if op := proc.GetTxnOperator(); op != nil {
		opts := op.TxnOptions()
		if opts.UserTxn() && opts.GetByBegin() && opts.GetCN() != "" && opts.GetCN() == proc.GetService() {
			meta := op.Txn()
			if meta.Status == txn.TxnStatus_Active && !meta.SnapshotTS.IsEmpty() {
				return meta.SnapshotTS, nil
			}
		}
	}
	return timestamp.Timestamp{}, moerr.NewInvalidInput(proc.Ctx, "mo_assert_snapshot_after requires an explicit transaction started with BEGIN")
}

func assertSnapshotAfter(ctx context.Context, snapshot timestamp.Timestamp, value string) error {
	// Nonnegative physical time (at most 19 digits), delimiter and uint32 logical
	// time (at most 10 digits). Keep malformed values out of error messages.
	if len(value) >= 3 && len(value) <= 30 {
		watermark, err := timestamp.ParseTimestamp(value)
		if err == nil && watermark.PhysicalTime >= 0 && watermark.DebugString() == value {
			if snapshot.Greater(watermark) {
				return nil
			}
			return moerr.NewSnapshotNotAfter(ctx)
		}
	}
	return moerr.NewInvalidInput(ctx, "mo_assert_snapshot_after requires a canonical physical-logical HLC watermark")
}
