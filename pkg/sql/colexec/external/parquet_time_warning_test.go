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

package external

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// Exercise native ParquetReader admission, rather than an isolated mapper:
// at ten bytes the second eight-byte TIME is retried in the next batch.
func TestParquetTimeWarningsFollowAcceptedRows(t *testing.T) {
	values := []string{"2562047788:00:00", "-2562047788:00:00"}
	for _, dictionary := range []bool{false, true} {
		for _, budget := range []uint64{10, 16, 0} {
			t.Run(fmt.Sprintf("dictionary%v/budget%d", dictionary, budget), func(t *testing.T) {
				data := writeBatchBudgetStringParquet(t, values)
				if dictionary {
					data = writeBatchBudgetDictionaryStringParquet(t, values)
				}
				reader, proc := newParquetTimeWarningReader(t, data, budget,
					[]*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_time)}, NotNull: true}},
					[]plan.ExternAttr{{ColName: "c", ColIndex: 0}})
				defer reader.Close()
				sink := &timeAssignmentWarningSink{}
				proc.WarningSink = sink
				var got []types.Time
				for finished := false; !finished; {
					bat := batchBudgetVectorBatch(types.T_time)
					func() {
						defer bat.Clean(proc.Mp())
						var err error
						finished, err = reader.ReadBatch(t.Context(), bat, proc, nil)
						require.NoError(t, err)
						require.Positive(t, bat.RowCount())
						got = append(got, vector.MustFixedColWithTypeCheck[types.Time](bat.Vecs[0])...)
					}()
				}
				require.Equal(t, []types.Time{3020399000000, -3020399000000}, got)
				require.Equal(t, []uint16{1264, 1264}, sink.codes)
				require.Same(t, sink, proc.WarningSink)
				require.Zero(t, process.WarningDiagnosticBudgetForProcess(proc).Used())
			})
		}
	}
}

func TestParquetTimeWarningsOnPrefixRemap(t *testing.T) {
	values := []string{"839:00:00", "-839:00:00", "840:00:00", "-840:00:00"}
	for _, dictionary := range []bool{false, true} {
		for _, ignore := range []bool{false, true} {
			for _, tc := range []struct {
				name      string
				retention int
				budget    uint64
				retained  int
			}{
				{"count only", 0, process.WarningDiagnosticMaxBytes, 0},
				{"one record", 1, process.WarningDiagnosticMaxBytes, 1},
				{"all records", 4, process.WarningDiagnosticMaxBytes, 4},
				{"one record budget", 4, process.WarningDiagnosticRecordBytes("Out of range value for column 'time' at row 1"), 1},
				{"empty budget", 4, 0, 0},
			} {
				t.Run(fmt.Sprintf("dictionary%v/ignore%v/%s", dictionary, ignore, tc.name), func(t *testing.T) {
					data := parquetTimeWarningPair(t, values, []string{"", "", "", ""}, dictionary)
					// The empty TEXT's vector slot makes decoded rows larger than
					// source rows. The initial estimate maps three more rows, then
					// admits/remaps two and retries the last in the next batch.
					budget := uint64(3*(8+types.VarlenaSize) + 1)
					reader, proc := newParquetTimeWarningPairReader(t, data, budget, types.T_text)
					defer reader.Close()
					proc.GetStmtProfile().SetStatementRuntimeProfile("Load Data", tree.QueryTypeDML, ignore)
					if ignore {
						proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return "STRICT_TRANS_TABLES", nil })
					}
					sink := &parquetTimeWarningSink{limit: tc.retention, budget: process.NewWarningDiagnosticBudget(tc.budget)}
					defer sink.reset()
					proc.WarningSink = sink
					mapping := reader.h.mappers[0].mapper
					var attempts []int64
					reader.h.mappers[0].mapper = func(mp *columnMapper, page parquet.Page, p *process.Process, v *vector.Vector) error {
						attempts = append(attempts, page.NumRows())
						return mapping(mp, page, p, v)
					}
					var got []types.Time
					var batches []int
					for finished := false; !finished; {
						bat := parquetTimeWarningPairBatch(types.T_text)
						func() {
							defer bat.Clean(proc.Mp())
							var err error
							finished, err = reader.ReadBatch(t.Context(), bat, proc, nil)
							require.NoError(t, err)
							require.Positive(t, bat.RowCount())
							batches = append(batches, bat.RowCount())
							got = append(got, vector.MustFixedColWithTypeCheck[types.Time](bat.Vecs[0])...)
						}()
					}
					require.Equal(t, []int64{1, 3, 2, 1, 1}, attempts, "exercise prefix remap and zero-admission retry")
					require.Equal(t, []int{3, 1}, batches)
					require.Equal(t, []types.Time{3020399000000, -3020399000000, 3020399000000, -3020399000000}, got)
					require.Equal(t, uint64(len(values)), sink.total)
					require.Len(t, sink.codes, tc.retained)
					for _, code := range sink.codes {
						require.Equal(t, uint16(1264), code)
					}
					require.Same(t, sink, proc.WarningSink)
					require.Equal(t, sink.charged, sink.budget.Used(), "only published diagnostics own budget")
					sink.reset()
					require.Zero(t, sink.budget.Used())
				})
			}
		}
	}
}

func TestParquetTimeWarningAdmissionErrors(t *testing.T) {
	for _, failure := range []string{"later column", "strict", "canceled", "panic"} {
		t.Run(failure, func(t *testing.T) {
			data := parquetTimeWarningPair(t, []string{"839:00:00"}, []string{"bad"}, false)
			reader, proc := newParquetTimeWarningPairReader(t, data, 0, types.T_int32)
			defer reader.Close()
			sink := &parquetTimeWarningSink{limit: 4, budget: process.NewWarningDiagnosticBudget(process.WarningDiagnosticMaxBytes)}
			defer sink.reset()
			proc.WarningSink = sink
			bat := parquetTimeWarningPairBatch(types.T_int32)
			defer bat.Clean(proc.Mp())
			sentinel := errors.New("cancel candidate after TIME conversion")
			if failure == "strict" {
				proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return "STRICT_TRANS_TABLES", nil })
			} else if failure == "canceled" || failure == "panic" {
				ctx, cancel := context.WithCancelCause(proc.Ctx)
				defer cancel(nil)
				proc.Ctx = ctx
				reader.h.mappers[1].mapper = func(_ *columnMapper, _ parquet.Page, p *process.Process, _ *vector.Vector) error {
					require.Equal(t, ctx, p.Ctx)
					if failure == "panic" {
						panic(sentinel)
					}
					cancel(sentinel)
					return context.Cause(p.Ctx)
				}
			}
			if failure == "panic" {
				require.PanicsWithValue(t, sentinel, func() { _, _ = reader.ReadBatch(t.Context(), bat, proc, nil) })
			} else {
				finished, err := reader.ReadBatch(t.Context(), bat, proc, nil)
				require.Error(t, err)
				require.False(t, finished)
				if failure == "strict" {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "%v", err)
				} else if failure == "canceled" {
					require.ErrorIs(t, err, sentinel)
				}
				for _, vec := range bat.Vecs {
					require.Zero(t, vec.Length(), "failed candidate rolls back all columns")
				}
			}
			require.Same(t, sink, proc.WarningSink)
			require.Zero(t, sink.total)
			require.Empty(t, sink.codes)
			require.Zero(t, sink.budget.Used())
		})
	}
}

func TestParquetHybridTimeWarningsFollowAcceptedRows(t *testing.T) {
	for _, seekFailure := range []bool{false, true} {
		t.Run(fmt.Sprintf("seekFailure%v", seekFailure), func(t *testing.T) {
			values := []string{"839:00:00", "-839:00:00", "840:00:00", "-840:00:00"}
			data := parquetTimeWarningPair(t, values, []string{"", "", "", ""}, true, true)
			reader, proc := newParquetTimeWarningPairReader(t, data, uint64(3*(8+types.VarlenaSize)+1), types.T_text)
			defer reader.Close()
			require.True(t, reader.h.hasNestedCols, "exercise the real hybrid reader")
			sink := &parquetTimeWarningSink{limit: 4, budget: process.NewWarningDiagnosticBudget(process.WarningDiagnosticMaxBytes)}
			defer sink.reset()
			proc.WarningSink = sink
			sentinel := errors.New("hybrid rewind failed")
			if seekFailure {
				reader.h.rowReader = &parquetWarningSeekFailure{Rows: reader.h.rowReader, err: sentinel}
			}
			var got []types.Time
			for finished := false; !finished; {
				bat := parquetTimeWarningPairBatch(types.T_text)
				func() {
					defer bat.Clean(proc.Mp())
					var err error
					finished, err = reader.ReadBatch(t.Context(), bat, proc, nil)
					if seekFailure {
						require.ErrorContains(t, err, sentinel.Error())
						require.Equal(t, uint64(1), sink.total, "only the earlier accepted slice was published")
						finished = true
					} else {
						require.NoError(t, err)
						require.Positive(t, bat.RowCount())
						got = append(got, vector.MustFixedColWithTypeCheck[types.Time](bat.Vecs[0])...)
					}
				}()
			}
			if !seekFailure {
				require.Equal(t, []types.Time{3020399000000, -3020399000000, 3020399000000, -3020399000000}, got)
				require.Equal(t, uint64(len(values)), sink.total)
				require.Equal(t, []uint16{1264, 1264, 1264, 1264}, sink.codes)
			}
			require.Same(t, sink, proc.WarningSink)
			require.Equal(t, sink.charged, sink.budget.Used())
			sink.reset()
			require.Zero(t, sink.budget.Used())
		})
	}
}

type parquetWarningSeekFailure struct {
	parquet.Rows
	err error
}

func (r *parquetWarningSeekFailure) SeekToRow(row int64) error {
	if row > 0 {
		return r.err
	}
	return r.Rows.SeekToRow(row)
}

// This destination models session ownership: count-only warnings remain exact,
// and an accepted owned batch transfers its charge without a second reserve.
type parquetTimeWarningSink struct {
	limit    int
	budget   *process.WarningDiagnosticBudget
	total    uint64
	codes    []uint16
	messages []string
	charged  uint64
}

func (s *parquetTimeWarningSink) GetWarningRetentionLimit() int { return s.limit }
func (s *parquetTimeWarningSink) GetWarningDiagnosticBudget() *process.WarningDiagnosticBudget {
	return s.budget
}
func (s *parquetTimeWarningSink) AppendWarningDiagnostic(code uint16, message string) {
	s.total++
	if len(s.codes) >= s.limit {
		return
	}
	message = process.BoundWarningMessage(message, process.WarningDiagnosticMaxMessageBytes)
	charge := process.WarningDiagnosticRecordBytes(message)
	if s.budget.Reserve(charge) {
		s.codes = append(s.codes, code)
		s.messages = append(s.messages, message)
		s.charged += charge
	}
}
func (s *parquetTimeWarningSink) AppendWarningBatchOwned(total uint64, codes []uint16,
	messages []string, source *process.WarningDiagnosticBudget, charged uint64,
) bool {
	if source != s.budget {
		return false
	}
	s.total += total
	keep := min(len(codes), len(messages), s.limit-len(s.codes), int(total))
	var retained uint64
	for _, message := range messages[:keep] {
		retained += process.WarningDiagnosticRecordBytes(message)
	}
	s.codes = append(s.codes, codes[:keep]...)
	s.messages = append(s.messages, messages[:keep]...)
	s.charged += retained
	s.budget.Release(charged - retained)
	return true
}
func (s *parquetTimeWarningSink) reset() {
	s.budget.Release(s.charged)
	s.charged = 0
	s.codes, s.messages = nil, nil
}

func parquetTimeWarningPair(t *testing.T, times, payloads []string, dictionary bool, nested ...bool) []byte {
	t.Helper()
	require.Len(t, payloads, len(times))
	node := parquet.String()
	if dictionary {
		node = parquet.Encoded(node, &parquet.RLEDictionary)
	}
	payloadNode := parquet.String()
	if len(nested) > 0 && nested[0] {
		payloadNode = parquet.Group{"v": payloadNode}
	}
	return writeBatchBudgetParquet(t, parquet.NewSchema("x", parquet.Group{
		"c1": node, "c2": payloadNode,
	}), times, func(value string) parquet.Row {
		payload := payloads[0]
		payloads = payloads[1:]
		return parquet.Row{
			parquet.ByteArrayValue([]byte(value)).Level(0, 0, 0),
			parquet.ByteArrayValue([]byte(payload)).Level(0, 0, 1),
		}
	})
}

func newParquetTimeWarningPairReader(t *testing.T, data []byte, budget uint64, second types.T) (*ParquetReader, *process.Process) {
	t.Helper()
	return newParquetTimeWarningReader(t, data, budget,
		[]*plan.ColDef{
			{Typ: plan.Type{Id: int32(types.T_time)}, NotNull: true},
			{Typ: plan.Type{Id: int32(second)}, NotNull: true},
		}, []plan.ExternAttr{{ColName: "c1", ColIndex: 0}, {ColName: "c2", ColIndex: 1}})
}

func parquetTimeWarningPairBatch(second types.T) *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_time.ToType())
	bat.Vecs[1] = vector.NewVec(second.ToType())
	return bat
}

func newParquetTimeWarningReader(t *testing.T, data []byte, budget uint64,
	cols []*plan.ColDef, attrs []plan.ExternAttr,
) (*ParquetReader, *process.Process) {
	t.Helper()
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx: t.Context(), maxBatchSize: budget, Cols: cols, Attrs: attrs,
			Extern: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{ScanType: tree.INLINE, Format: tree.PARQUET, Data: string(data)},
				ExParam:      tree.ExParam{ExternType: int32(plan.ExternType_LOAD)},
			},
			FileSize: []int64{int64(len(data))},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1}},
	}
	proc := testutil.NewProc(t)
	proc.Ctx = t.Context()
	proc.SetStmtProfile(&process.StmtProfile{})
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return "", nil })
	reader := NewParquetReader(param, proc)
	empty, err := reader.Open(param, proc)
	require.NoError(t, err)
	require.False(t, empty)
	return reader, proc
}
