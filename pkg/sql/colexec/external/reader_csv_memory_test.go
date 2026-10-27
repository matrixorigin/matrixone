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

package external

import (
	"context"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestCSVShortRowPreservesStrictAndLocalPolicy(t *testing.T) {
	for _, mode := range []struct {
		name          string
		strict, local bool
	}{
		{"strict", true, false}, {"permissive", false, false}, {"local", true, true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			defer proc.Free()
			param := &ExternalParam{ExParamConst: ExParamConst{
				Ctx: proc.Ctx, ColumnListLen: 2, StrictSqlMode: mode.strict,
				Attrs:  []plan.ExternAttr{{ColName: "a", ColIndex: 0, ColFieldIndex: 0}, {ColName: "b", ColIndex: 1, ColFieldIndex: 1}},
				Cols:   []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_varchar)}}, {Typ: plan.Type{Id: int32(types.T_varchar)}}},
				Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.CSV, Tail: &tree.TailParameter{}}, ExParam: tree.ExParam{Local: mode.local}},
			}}
			p, err := newCSVParserFromReader(param.Extern, strings.NewReader("one\n"))
			require.NoError(t, err)
			r := &CsvReader{param: param, plh: &ParseLineHandler{csvReader: p}, reader: io.NopCloser(strings.NewReader(""))}
			defer r.Close()
			bat := batch.NewWithSize(2)
			for i := range bat.Vecs {
				bat.Vecs[i] = vector.NewVec(types.T_varchar.ToType())
			}
			defer bat.Clean(proc.Mp())
			_, err = r.makeBatchRows(proc, bat)
			if mode.strict && !mode.local {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, bat.RowCount())
			require.Equal(t, "one", string(bat.Vecs[0].GetBytesAt(0)))
			require.True(t, bat.Vecs[1].GetNulls().Contains(0))
		})
	}
}

func TestCSVRecordBoundUsesOnlyKnownUncompressedInput(t *testing.T) {
	source := ExParamConst{FileList: []string{"a.csv", "b.csv"}, FileSize: []int64{64, 128}}
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{ScanType: tree.INFILE}}
	require.Equal(t, int64(128), csvRecordBytes(&source, param))
	source.FileList[1] = "b.csv.gz"
	require.Equal(t, int64(config.LargestEntryLimit), csvRecordBytes(&source, param))
	param.Local = true
	param.ScanType = tree.INLINE
	param.Data = "one\n"
	require.Equal(t, int64(config.LargestEntryLimit), csvRecordBytes(&source, param))
	param.Local = false
	require.Equal(t, int64(4), csvRecordBytes(&source, param))
	param.CompressType = tree.GZIP
	require.Equal(t, int64(config.LargestEntryLimit), csvRecordBytes(&source, param))
}

type inputOwnerExecutor struct {
	colexec.ExpressionExecutor
	beforeFree func()
}

func (e *inputOwnerExecutor) Free() {
	e.beforeFree()
	e.ExpressionExecutor.Free()
}

func TestCSVResetDestroysProjectionBeforeReturningInputSlot(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	owner := rscthrottler.NewMemThrottler("external-reset-owner", 1, rscthrottler.WithConstLimit(1<<30))
	ws, err := owner.(rscthrottler.WorkingSetThrottler).ReserveWorkingSetCandidatesUntilFit(proc.Ctx, []rscthrottler.WorkingSetSpec{{
		UnitBytes: 1 << 20, Inputs: []rscthrottler.InputSlotSpec{{Name: "csv", Count: 1, Bytes: 1 << 20}}, IOBytes: 1,
	}})
	require.NoError(t, err)
	defer ws.Close()
	lease, err := ws.AcquireInput(proc.Ctx, 0)
	require.NoError(t, err)
	e := &External{inputLease: lease}
	defer e.Free(proc, false, nil)
	v := testutil.MakeVarcharVector([]string{strings.Repeat("x", 128)}, nil, proc.Mp())
	freed := false
	e.Projection = colexec.Projection{ProjectList: []*plan.Expr{{}}, ProjectExecutors: []colexec.ExpressionExecutor{
		&inputOwnerExecutor{ExpressionExecutor: colexec.NewFixedVectorExpressionExecutor(proc.Mp(), true, v), beforeFree: func() {
			require.Same(t, lease, e.inputLease, "projection remains an I owner until its storage is destroyed")
			freed = true
		}},
	}}
	e.Reset(proc, false, nil)
	require.True(t, freed)
	require.Nil(t, e.ProjectExecutors)
	require.Nil(t, e.inputLease)
	require.Zero(t, proc.Mp().AllocatedBytes())
	lease, err = ws.AcquireInput(proc.Ctx, 0)
	require.NoError(t, err)
	lease.Release()
}

func TestCSVTypedBudgetPreservesPendingRecord(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	param := &ExternalParam{ExParamConst: ExParamConst{
		Ctx: context.Background(), maxBatchSize: 512,
		ColumnListLen: 2,
		Attrs:         []plan.ExternAttr{{ColName: "id", ColIndex: 0, ColFieldIndex: 0}, {ColName: "v", ColIndex: 1, ColFieldIndex: 1}},
		Cols:          []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}}, {Name: "v", Typ: plan.Type{Id: int32(types.T_array_float64), Width: 32}}},
		Extern:        &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.CSV, Tail: &tree.TailParameter{}}},
	}}
	input := ""
	for _, id := range []string{"1", "2", "3"} {
		input += id + ",\"[" + strings.Repeat("0,", 31) + "0]\"\n"
	}
	p, err := newCSVParserFromReader(param.Extern, strings.NewReader(input))
	require.NoError(t, err)
	r := &CsvReader{param: param, plh: &ParseLineHandler{csvReader: p}, reader: io.NopCloser(strings.NewReader(""))}
	defer r.Close()
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	vtype := types.T_array_float64.ToType()
	vtype.Width = 32
	bat.Vecs[1] = vector.NewVec(vtype)
	defer bat.Clean(proc.Mp())
	var got []int64
	for {
		finished, readErr := r.makeBatchRows(proc, bat)
		require.NoError(t, readErr)
		for _, id := range vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0]) {
			got = append(got, id)
		}
		if !finished {
			require.LessOrEqual(t, bat.RowCount(), 1, "typed expansion must split even though raw bytes fit")
		}
		if finished {
			break
		}
		// The crossing raw record is an alias of parser-owned storage. Its
		// value must survive the downstream call and the next typed append.
		bat.CleanOnlyData()
	}
	require.Equal(t, []int64{1, 2, 3}, got)
	require.Nil(t, r.pending)
}

func TestCSVSingletonAboveBatchTarget(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	param := &ExternalParam{ExParamConst: ExParamConst{Ctx: context.Background(), maxBatchSize: 16, ColumnListLen: 1, Attrs: []plan.ExternAttr{{ColName: "v", ColIndex: 0, ColFieldIndex: 0}}, Cols: []*plan.ColDef{{Name: "v", Typ: plan.Type{Id: int32(types.T_text)}}}, Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.CSV, Tail: &tree.TailParameter{}}}}}
	p, err := newCSVParserFromReader(param.Extern, strings.NewReader(strings.Repeat("x", 128)+"\nshort\n"))
	require.NoError(t, err)
	r := &CsvReader{param: param, plh: &ParseLineHandler{csvReader: p}, reader: io.NopCloser(strings.NewReader(""))}
	defer r.Close()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_text.ToType())
	defer bat.Clean(proc.Mp())
	finished, err := r.makeBatchRows(proc, bat)
	require.NoError(t, err)
	require.False(t, finished)
	require.Equal(t, strings.Repeat("x", 128), bat.Vecs[0].GetStringAt(0))
	bat.CleanOnlyData()
	_, err = r.makeBatchRows(proc, bat)
	require.NoError(t, err)
	require.Equal(t, "short", bat.Vecs[0].GetStringAt(0))
}

// The compiler can lower UnitBytes without lowering the RPC-derived batch
// limit. Exercise the actual External.Prepare -> raw CSV -> Call path with an
// admitted input slot, rather than manually changing maxBatchSize.
func TestCSVAdmittedUnitControlsRawAndTypedBatches(t *testing.T) {
	const unit int64 = 512
	arrayType := types.T_array_float64.ToType()
	arrayType.Width = 32
	for _, tc := range []struct {
		name          string
		payloads      []string
		payloadType   types.Type
		ignored       bool
		wantOversized bool
	}{
		{name: "typed_expansion", payloads: []string{
			"[" + strings.Repeat("0,", 31) + "0]",
			"[" + strings.Repeat("0,", 31) + "0]",
			"[" + strings.Repeat("0,", 31) + "0]",
			"[" + strings.Repeat("0,", 31) + "0]",
			"[" + strings.Repeat("0,", 31) + "0]",
			"[" + strings.Repeat("0,", 31) + "0]",
		}, payloadType: arrayType},
		{name: "raw_ignored_fields", payloads: []string{
			strings.Repeat("x", 300), strings.Repeat("y", 300), strings.Repeat("z", 1500), "ab", "cd",
		}, ignored: true, wantOversized: true},
		{name: "capacity_reuse_then_area_growth", payloads: []string{
			"a", "b", "c", "d", strings.Repeat("e", 300), "f", "g", "h",
		}, payloadType: types.T_varchar.ToType()},
		{name: "typed_singleton", payloads: []string{
			strings.Repeat("x", 2048), "short", strings.Repeat("y", 200), "tiny",
		}, payloadType: types.T_text.ToType(), wantOversized: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			defer proc.Free()
			owner := rscthrottler.NewMemThrottler("csv-admitted-unit", 1, rscthrottler.WithConstLimit(1<<30))
			ws, err := owner.(rscthrottler.WorkingSetThrottler).ReserveWorkingSetCandidatesUntilFit(proc.Ctx, []rscthrottler.WorkingSetSpec{{
				UnitBytes: unit, Inputs: []rscthrottler.InputSlotSpec{{Name: "csv", Count: 1, Bytes: 1 << 20}}, IOBytes: 1,
			}})
			require.NoError(t, err)
			defer ws.Close()
			growths := 0
			proc.Base.ImportMemory = &process.ImportMemory{WorkingSet: ws, UnitBytes: ws.UnitBytes(), SourceBytes: 1 << 20, IOBytes: 1, RecordBytes: 1024,
				GrowRecord: func(ctx context.Context, need int64) (int64, error) {
					growths++
					return 4096, ws.TryGrow(ctx, rscthrottler.WorkingSetSpec{UnitBytes: unit,
						Inputs: []rscthrottler.InputSlotSpec{{Name: "csv", Count: 1, Bytes: 2 << 20}}, IOBytes: 1})
				},
			}
			proc.Base.Lim.MaxMsgSize = 128 << 20
			proc.Base.Lim.BatchSize = 64 << 20
			param := &ExternalParam{ExParamConst: ExParamConst{
				ColumnListLen: 1,
				Attrs:         []plan.ExternAttr{{ColName: "id", ColIndex: 0, ColFieldIndex: 0}},
				Cols:          []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}}},
				Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{
					Format: tree.CSV, ScanType: tree.INLINE, Tail: &tree.TailParameter{},
				}},
			}, ExParam: ExParam{Fileparam: &ExFileparam{}}}
			if !tc.ignored {
				param.ColumnListLen = 2
				param.Attrs = append(param.Attrs, plan.ExternAttr{ColName: "v", ColIndex: 1, ColFieldIndex: 1})
				param.Cols = append(param.Cols, &plan.ColDef{Name: "v", Typ: plan.Type{Id: int32(tc.payloadType.Oid), Width: tc.payloadType.Width}})
			}
			var input strings.Builder
			for i, payload := range tc.payloads {
				input.WriteString(strconv.Itoa(i + 1))
				input.WriteString(",\"")
				input.WriteString(payload)
				input.WriteString("\"\n")
			}
			param.Extern.Data = input.String()
			e := &External{Es: param}
			defer e.Free(proc, false, nil)
			require.NoError(t, e.Prepare(proc))
			require.NotNil(t, e.inputLease, "the real source owner must hold the admitted input slot")
			require.Greater(t, param.maxBatchSize, uint64(unit), "RPC-derived limit must remain large to test ticket wiring")
			var got []int64
			var sawOversized bool
			for calls := 0; ; calls++ {
				require.Less(t, calls, len(tc.payloads)+3, "raw source must make progress through every pending record")
				result, err := e.Call(proc)
				require.NoError(t, err)
				if result.Status == vm.ExecStop {
					break
				}
				bat := result.Batch
				require.NotNil(t, bat)
				var raw int64
				for row, id := range vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0]) {
					got = append(got, id)
					require.GreaterOrEqual(t, id, int64(1))
					require.LessOrEqual(t, id, int64(len(tc.payloads)))
					payload := tc.payloads[id-1]
					raw += int64(len(strconv.FormatInt(id, 10)) + len(payload))
					if tc.ignored {
						continue
					}
					if tc.payloadType.Oid == types.T_array_float64 {
						require.Equal(t, make([]float64, 32), vector.GetArrayAt[float64](bat.Vecs[1], row))
					} else {
						require.Equal(t, payload, bat.Vecs[1].GetStringAt(row), "pending raw record must remain intact across calls")
					}
				}
				if raw > unit || int64(bat.Allocated()) > unit {
					sawOversized = true
					require.Equal(t, 1, bat.RowCount(), "only a legal singleton may exceed the admitted raw/typed target")
				}
			}
			expected := make([]int64, len(tc.payloads))
			for i := range expected {
				expected[i] = int64(i + 1)
			}
			require.Equal(t, expected, got)
			require.Equal(t, tc.wantOversized, sawOversized)
			require.Equal(t, tc.wantOversized, growths > 0, "reader must grow admission for a wider raw record")
			e.Free(proc, false, nil)
			require.Zero(t, proc.Mp().AllocatedBytes())
			lease, err := ws.AcquireInput(proc.Ctx, 0)
			require.NoError(t, err)
			lease.Release()
		})
	}
}
