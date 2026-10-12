// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/jsonvalue"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type jsonTableTestWarnings struct {
	total    uint64
	codes    []uint16
	messages []string
}

func (w *jsonTableTestWarnings) AppendWarningBatch(total uint64, codes []uint16, messages []string) {
	w.total += total
	w.codes = append(w.codes, codes...)
	w.messages = append(w.messages, messages...)
}

func newJSONTableTestFunction(t *testing.T, source string, typ types.Type, onError string) (*TableFunction, *process.Process, *jsonTableTestWarnings) {
	t.Helper()
	proc := testutil.NewProcess(t)
	sink := &jsonTableTestWarnings{}
	proc.WarningSink = sink
	params, err := json.Marshal(jsonvalue.TableSpec{Version: 1, RootPath: "$[*]", Columns: []jsonvalue.TableColumn{{
		Name: "v", Kind: "path", Path: "$", Type: typ,
		OnEmpty: jsonvalue.TableResponse{Action: "null"}, OnError: jsonvalue.TableResponse{Action: onError},
	}}})
	require.NoError(t, err)
	tf := &TableFunction{FuncName: "json_table", Params: params, Attrs: []string{"v"},
		Rets: []*plan.ColDef{{Name: "v", Typ: plan.Type{Id: int32(typ.Oid), Width: typ.Width, Scale: typ.Scale}}},
		Args: []*plan.Expr{plan2.MakePlan2StringConstExprWithType(source)}}
	t.Cleanup(func() {
		tf.Free(proc, false, nil)
		require.Zero(t, proc.Mp().CurrNB(), "owned result/executor memory must be released")
	})
	require.NoError(t, tf.Prepare(proc))
	return tf, proc, sink
}

func TestJSONTableWarningAndResourceLifecycle(t *testing.T) {
	t.Run("cross_batch_once", func(t *testing.T) {
		source := `[` + strings.Repeat(`1.25,`, colexec.DefaultBatchSize) + `1.25]`
		tf, proc, sink := newJSONTableTestFunction(t, source, types.New(types.T_decimal64, 3, 1), "null")
		out, err := tf.Call(proc)
		require.NoError(t, err)
		require.Equal(t, colexec.DefaultBatchSize, out.Batch.RowCount())
		require.Equal(t, uint64(1), sink.total)
		out, err = tf.Call(proc)
		require.NoError(t, err)
		require.Equal(t, 1, out.Batch.RowCount())
		require.Equal(t, uint64(1), sink.total)
	})
	t.Run("early_stop_reset_reuse_free", func(t *testing.T) {
		tf, proc, sink := newJSONTableTestFunction(t, `[1.25,2.75]`, types.New(types.T_decimal64, 3, 1), "null")
		out, err := tf.Call(proc)
		require.NoError(t, err)
		require.Equal(t, 2, out.Batch.RowCount())
		require.Equal(t, []types.Decimal64{13, 28}, vector.MustFixedColWithTypeCheck[types.Decimal64](out.Batch.Vecs[0]))
		require.Equal(t, uint64(1), sink.total, "successful output is published before end, including early LIMIT")
		require.Equal(t, []uint16{1265}, sink.codes)
		require.Equal(t, []string{"Data truncated for column 'v' at row 1"}, sink.messages)
		tf.Reset(proc, false, nil)
		sink.total = 0 // models the next attempt's fresh sink, not row-level reset
		out, err = tf.Call(proc)
		require.NoError(t, err)
		require.Equal(t, 2, out.Batch.RowCount())
		require.Equal(t, uint64(1), sink.total)
		require.NoError(t, tf.ApplyEnd(proc))
		require.NoError(t, tf.ApplyEnd(proc))
		require.Equal(t, uint64(1), sink.total, "end cannot publish the same warning twice")
		tf.Free(proc, false, nil)
		tf.Free(proc, false, nil)
	})
	t.Run("partial_batch_error_discards_pending", func(t *testing.T) {
		tf, proc, sink := newJSONTableTestFunction(t, `[1.25,{}]`, types.New(types.T_decimal64, 3, 1), "error")
		_, err := tf.Call(proc)
		require.Error(t, err)
		require.Zero(t, sink.total)
		state := tf.ctr.state.(*jsonTableState)
		require.Empty(t, state.frames)
		require.Zero(t, state.batch.RowCount())
		require.Zero(t, state.warnings.Total)
		tf.Reset(proc, true, err)
	})
	t.Run("cancel_then_fresh_attempt", func(t *testing.T) {
		tf, proc, sink := newJSONTableTestFunction(t, `[1,2]`, types.T_int32.ToType(), "null")
		oldCtx := proc.Ctx
		ctx, cancel := context.WithCancel(oldCtx)
		proc.Ctx = ctx
		cancel()
		_, err := tf.Call(proc)
		require.Error(t, err)
		require.Zero(t, sink.total)
		tf.Reset(proc, true, err)
		proc.Ctx = oldCtx
		out, err := tf.Call(proc)
		require.NoError(t, err)
		require.Equal(t, []int32{1, 2}, vector.MustFixedColWithTypeCheck[int32](out.Batch.Vecs[0]))
	})
	t.Run("unsupported_lossy_conversion_is_not_on_error", func(t *testing.T) {
		for _, tc := range []struct {
			source string
			typ    types.Type
		}{
			{`["ab"]`, types.New(types.T_varchar, 1, 0)},
			{`[1.25]`, types.T_int32.ToType()},
		} {
			tf, proc, sink := newJSONTableTestFunction(t, tc.source, tc.typ, "null")
			_, err := tf.Call(proc)
			require.ErrorContains(t, err, "requires the scalar compatibility gate")
			require.Zero(t, sink.total)
		}
	})
}
