// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func viewColumnsOutputFixture(t *testing.T, columns []*planpb.ColDef) (*viewColumnsState, *TableFunction, *process.Process) {
	t.Helper()
	proc := testutil.NewProcess(t)
	tf := &TableFunction{Attrs: []string{"attnum", "attname", "atttyp", "att_default", "attnotnull", "attr_enum", "att_is_auto_increment", "attr_has_generated", "attr_generated", "att_is_hidden", "att_comment", "att_constraint_type"}}
	tf.ctr.retSchema = []types.Type{types.T_int32.ToType(), types.T_varchar.ToType(), types.T_varbinary.ToType(), types.T_varbinary.ToType(), types.T_int8.ToType(), types.T_varchar.ToType(), types.T_int8.ToType(), types.T_int8.ToType(), types.T_varbinary.ToType(), types.T_int8.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()}
	s := &viewColumnsState{columns: columns, derived: true}
	t.Cleanup(func() {
		s.free(tf, proc, false, nil)
		require.Zero(t, proc.Mp().CurrNB())
	})
	return s, tf, proc
}

func TestViewColumnsOutputRowPagesAndCleanup(t *testing.T) {
	cols := make([]*planpb.ColDef, viewColumnsBatchRows+1)
	for i := range cols {
		cols[i] = &planpb.ColDef{Name: fmt.Sprintf("column_%d", i), Typ: planpb.Type{Id: int32(types.T_int32), NotNullable: true}}
	}
	s, tf, proc := viewColumnsOutputFixture(t, cols)
	seen := 0
	for _, count := range []int{viewColumnsBatchRows, 1} {
		result, err := s.call(tf, proc)
		require.NoError(t, err)
		require.Equal(t, vm.ExecNext, result.Status)
		require.Equal(t, count, result.Batch.RowCount())
		ordinals := vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[0])
		for i := range ordinals {
			require.Equal(t, int32(seen+i+1), ordinals[i])
			require.Equal(t, cols[seen+i].Name, result.Batch.Vecs[1].GetStringAt(i))
		}
		var typ types.Type
		require.NoError(t, types.Decode(result.Batch.Vecs[2].GetBytesAt(0), &typ))
		require.Equal(t, types.T_int32, typ.Oid)
		var def planpb.Default
		require.NoError(t, types.Decode(result.Batch.Vecs[3].GetBytesAt(0), &def))
		require.False(t, def.NullAbility)
		require.Equal(t, int8(1), vector.GetFixedAtNoTypeCheck[int8](result.Batch.Vecs[4], 0))
		require.True(t, result.Batch.Vecs[8].IsNull(0))
		seen += count
	}
	require.Nil(t, s.columns)
	result, err := s.call(tf, proc)
	require.NoError(t, err)
	require.Equal(t, vm.CancelResult, result)
	s.reset(tf, proc)
	s.columns = cols[:1]
	result, err = s.call(tf, proc)
	require.NoError(t, err)
	require.Equal(t, int32(1), vector.GetFixedAtNoTypeCheck[int32](result.Batch.Vecs[0], 0))
	s.free(tf, proc, false, nil)
	s.free(tf, proc, false, nil)
}

func TestViewColumnsOutputBytePagesAndLargeRow(t *testing.T) {
	for _, length := range []int{viewColumnsBatchBytes / 2, viewColumnsBatchBytes + 1} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			column := &planpb.ColDef{Name: "v", Typ: planpb.Type{Id: int32(types.T_varchar)}, Default: &planpb.Default{OriginString: strings.Repeat("x", length)}}
			s, tf, proc := viewColumnsOutputFixture(t, []*planpb.ColDef{column, column})
			for i := 0; i < 2; i++ {
				result, err := s.call(tf, proc)
				require.NoError(t, err)
				require.Equal(t, 1, result.Batch.RowCount())
				require.LessOrEqual(t, result.Batch.Size(), viewColumnsBatchHardBytes)
			}
		})
	}
}

func TestViewColumnsOutputFailuresReleasePartialState(t *testing.T) {
	for _, mode := range []string{"cancel", "nil column", "oversized column", "unknown projection", "early end", "reset"} {
		t.Run(mode, func(t *testing.T) {
			column := &planpb.ColDef{Name: "v", Typ: planpb.Type{Id: int32(types.T_int32)}}
			s, tf, proc := viewColumnsOutputFixture(t, []*planpb.ColDef{column})
			switch mode {
			case "cancel":
				ctx, cancel := context.WithCancel(proc.Ctx)
				proc.Ctx = ctx
				cancel()
			case "nil column":
				s.columns[0] = nil
			case "oversized column":
				column.Name = strings.Repeat("x", viewColumnsBatchHardBytes+1)
			case "unknown projection":
				tf.Attrs[len(tf.Attrs)-1] = "unknown"
			case "early end":
				require.NoError(t, s.end(tf, proc))
			case "reset":
				s.reset(tf, proc)
			}
			result, err := s.call(tf, proc)
			if mode == "early end" || mode == "reset" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, vm.CancelResult, result)
			require.Nil(t, s.columns)
			if s.batch != nil {
				require.Zero(t, s.batch.RowCount())
			}
		})
	}
}

func TestViewColumnsDerivedPlanCannotFallBackWithoutOwner(t *testing.T) {
	s, tf, proc := viewColumnsOutputFixture(t, nil)
	database, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("db"), 1, proc.Mp())
	require.NoError(t, err)
	defer database.Free(proc.Mp())
	name, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("v"), 1, proc.Mp())
	require.NoError(t, err)
	defer name.Free(proc.Mp())
	id, err := vector.NewConstFixed(types.T_uint64.ToType(), uint64(7), 1, proc.Mp())
	require.NoError(t, err)
	defer id.Free(proc.Mp())
	tf.ctr.argVecs = []*vector.Vector{database, name, id}
	err = s.start(tf, proc, 0, nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
	require.Nil(t, s.columns)
}

func TestViewColumnsLegacyBatchCompatibility(t *testing.T) {
	columns := make([]*planpb.ColDef, viewColumnsBatchRows+1)
	for i := range columns {
		columns[i] = &planpb.ColDef{Name: "v", Typ: planpb.Type{Id: int32(types.T_int32)}}
	}
	s, tf, proc := viewColumnsOutputFixture(t, columns)
	s.derived = false
	result, err := s.call(tf, proc)
	require.NoError(t, err)
	require.Equal(t, len(columns), result.Batch.RowCount())
	for _, name := range []string{plan.ViewColumnsFunctionName, plan.SubscriptionViewColumnsFunctionName} {
		tf.FuncName = name
		tf.Args = make([]*planpb.Expr, 3)
		require.True(t, tf.ApplyPreserveBatch())
		tf.Args = tf.Args[:1]
		require.False(t, tf.ApplyPreserveBatch())
	}
	tf.FuncName = "generate_series"
	tf.Args = make([]*planpb.Expr, 3)
	require.False(t, tf.ApplyPreserveBatch())
}

func TestSkippableViewDescriptionError(t *testing.T) {
	ctx := t.Context()
	for _, err := range []error{
		moerr.NewNoDB(ctx),
		moerr.NewNoSuchTable(ctx, "db", "source"),
		moerr.NewBadFieldError(ctx, "missing", "field list"),
		moerr.NewBadDB(ctx, "source_db"),
	} {
		require.True(t, skippableViewDescriptionError(err), err)
	}
	for _, err := range []error{
		nil,
		moerr.NewNotSupported(ctx, "protocol gate"),
		moerr.NewInternalError(ctx, "binding budget"),
		moerr.NewParseError(ctx, "corrupt definition"),
		context.Canceled,
		context.DeadlineExceeded,
		plan.ErrViewSchemaLimit,
		plan.ErrViewSchemaChanged,
		plan.ErrViewSchemaClosed,
	} {
		require.False(t, skippableViewDescriptionError(err), err)
	}
}
