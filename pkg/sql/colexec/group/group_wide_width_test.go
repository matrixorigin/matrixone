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

package group

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type declaredGroupKey struct {
	typ         types.T
	width       int32
	notNullable bool
}

// Only five actual tiny rows are allocated. Large widths are declarations,
// exactly as for LONGTEXT; the test must never allocate declared-width data.
func wideWidthInput(t *testing.T, proc *process.Process, keys []declaredGroupKey) (*batch.Batch, map[string]int64) {
	t.Helper()
	input := batch.NewWithSize(len(keys))
	for column, key := range keys {
		input.Vecs[column] = vector.NewVec(types.New(key.typ, key.width, 0))
	}
	owned := true
	defer func() {
		if owned {
			input.Clean(proc.Mp())
		}
	}()
	want := make(map[string]int64)
	for row, value := range []int64{1, 1, 2, 3, 3} {
		logical := make([]any, len(keys))
		for column, key := range keys {
			isNull := !key.notNullable && row >= 3
			var err error
			switch key.typ {
			case types.T_text, types.T_varchar:
				text := string(rune('a' + value - 1))
				err = vector.AppendBytes(input.Vecs[column], []byte(text), isNull, proc.Mp())
				logical[column] = text
			case types.T_int32:
				err = vector.AppendFixed(input.Vecs[column], int32(value), isNull, proc.Mp())
				logical[column] = value
			case types.T_int16:
				err = vector.AppendFixed(input.Vecs[column], int16(value), isNull, proc.Mp())
				logical[column] = value
			case types.T_int8:
				err = vector.AppendFixed(input.Vecs[column], int8(value), isNull, proc.Mp())
				logical[column] = value
			default:
				t.Fatalf("unsupported tiny fixture key: %v", key.typ)
			}
			require.NoError(t, err)
			if isNull {
				logical[column] = nil
			}
		}
		encoded, err := json.Marshal(logical)
		require.NoError(t, err)
		want[string(encoded)]++
	}
	input.SetRowCount(5)
	owned = false
	return input, want
}

func wideWidthCounts(t *testing.T, outputs []*batch.Batch, keys []declaredGroupKey) map[string]int64 {
	t.Helper()
	got := make(map[string]int64)
	for _, output := range outputs {
		require.Len(t, output.Vecs, len(keys)+1)
		counts := vector.MustFixedColNoTypeCheck[int64](output.Vecs[len(keys)])
		for row := 0; row < output.RowCount(); row++ {
			logical := make([]any, len(keys))
			for column, key := range keys {
				vec := output.Vecs[column]
				if vec.IsNull(uint64(row)) {
					continue
				}
				switch key.typ {
				case types.T_text, types.T_varchar:
					logical[column] = string(vec.GetBytesAt(row))
				case types.T_int32:
					logical[column] = int64(vector.MustFixedColNoTypeCheck[int32](vec)[row])
				case types.T_int16:
					logical[column] = int64(vector.MustFixedColNoTypeCheck[int16](vec)[row])
				case types.T_int8:
					logical[column] = int64(vector.MustFixedColNoTypeCheck[int8](vec)[row])
				}
			}
			encoded, err := json.Marshal(logical)
			require.NoError(t, err)
			_, duplicate := got[string(encoded)]
			require.False(t, duplicate, "one final row required per complete group key")
			got[string(encoded)] = counts[row]
		}
	}
	return got
}

func TestGroupHashWideDeclaredWidths(t *testing.T) {
	hugeNullable := declaredGroupKey{typ: types.T_text, width: math.MaxInt32}
	hugeNotNull := declaredGroupKey{typ: types.T_text, width: math.MaxInt32, notNullable: true}
	for _, test := range []struct {
		name       string
		keys       []declaredGroupKey
		remoteV77  bool
		wantType   int32
		wantWidth  int32
		wantLegacy bool
	}{
		{name: "two-nullable-longtext", keys: []declaredGroupKey{hugeNullable, hugeNullable}, wantType: HStr, wantWidth: math.MaxInt32},
		{name: "two-not-null-longtext", keys: []declaredGroupKey{hugeNotNull, hugeNotNull}, wantType: HStr, wantWidth: math.MaxInt32},
		{name: "one-nullable-longtext", keys: []declaredGroupKey{hugeNullable}, wantType: HStr, wantWidth: math.MaxInt32},
		{name: "real-no-group-by", wantType: H0, wantWidth: 0},
		{name: "fixed-eight-byte-boundary", keys: []declaredGroupKey{{typ: types.T_int32, notNullable: true}, {typ: types.T_int32, notNullable: true}}, wantType: H8, wantWidth: 8},
		{name: "global-nullability-eleven", keys: []declaredGroupKey{{typ: types.T_int16, notNullable: true}, {typ: types.T_int16, notNullable: true}, {typ: types.T_int16, notNullable: true}, {typ: types.T_int8}}, wantType: HStr, wantWidth: 11},
		{name: "legacy-huge-nullable-varchar", keys: []declaredGroupKey{{typ: types.T_varchar, width: math.MaxInt32}}, remoteV77: true, wantType: HStr, wantWidth: math.MaxInt32},
		{name: "legacy-huge-not-null-composite", keys: []declaredGroupKey{hugeNotNull, hugeNotNull}, remoteV77: true, wantType: HStr, wantWidth: math.MaxInt32},
		{name: "legacy-one-huge-plus-narrow", keys: []declaredGroupKey{hugeNotNull, {typ: types.T_int8, notNullable: true}}, remoteV77: true, wantType: HStr, wantWidth: math.MaxInt32},
		{name: "legacy-small-varchar", keys: []declaredGroupKey{{typ: types.T_varchar, width: 2}}, remoteV77: true, wantType: H8, wantWidth: 3, wantLegacy: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			version := int64(defines.MORPCVersion78)
			if test.remoteV77 {
				proc.Ctx = context.WithValue(proc.Ctx, defines.RemoteRunContext{}, true)
				version = defines.MORPCVersion77
			}
			setPrepareParamKindProtocolVersion(t, proc, version)
			groupBy := make([]*plan.Expr, len(test.keys))
			for column, key := range test.keys {
				groupBy[column] = colExpr(int32(column), key.typ)
				groupBy[column].Typ.Width = key.width
				groupBy[column].Typ.NotNullable = key.notNullable
			}
			g := newGroupOp(proc, groupBy, []aggexec.AggFuncExecExpression{countStarLiteralAgg()})
			var child *colexec.MockOperator
			defer func() {
				g.Free(proc, false, nil)
				if child != nil {
					child.Free(proc, false, nil)
				}
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			}()
			for execution := 0; execution < 2; execution++ {
				input, want := wideWidthInput(t, proc, test.keys)
				child = colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
				g.Children = nil
				g.AppendChild(child)
				require.NoError(t, g.Prepare(proc))
				require.Equal(t, test.wantType, g.ctr.mtyp)
				require.Equal(t, test.wantWidth, g.ctr.keyWidth)
				require.Equal(t, test.wantLegacy, g.ctr.legacyH8CharSemantics)
				require.Equal(t, want, wideWidthCounts(t, collectBatches(t, g, proc), test.keys))
				g.Reset(proc, false, nil)
				child.Free(proc, false, nil)
				child = nil
				require.Zero(t, proc.Mp().CurrNB(), "Reset plus input-owner cleanup must release each execution")
			}
		})
	}
}

func TestGroupWideDeclaredWidthsPartialMerge(t *testing.T) {
	for _, version := range []int64{defines.MORPCVersion77, defines.MORPCVersion78} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.Ctx = context.WithValue(proc.Ctx, defines.RemoteRunContext{}, true)
			setPrepareParamKindProtocolVersion(t, proc, version)
			keys := []declaredGroupKey{{typ: types.T_text, width: math.MaxInt32}, {typ: types.T_text, width: math.MaxInt32}}
			groupBy := []*plan.Expr{colExpr(0, types.T_text), colExpr(1, types.T_text)}
			for _, key := range groupBy {
				key.Typ.Width = math.MaxInt32
			}
			aggs := []aggexec.AggFuncExecExpression{countStarLiteralAgg()}
			partial := newGroupOp(proc, groupBy, aggs)
			partial.NeedEval = false
			var child, mergeChild *colexec.MockOperator
			var merge *MergeGroup
			var copies []*batch.Batch
			defer func() {
				if merge != nil {
					merge.Free(proc, false, nil)
				}
				if mergeChild != nil {
					mergeChild.Free(proc, false, nil)
				}
				partial.Free(proc, false, nil)
				if child != nil {
					child.Free(proc, false, nil)
				}
				for _, copied := range copies {
					copied.Clean(proc.Mp())
				}
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			}()
			want := make(map[string]int64)
			for execution := 0; execution < 2; execution++ {
				input, expected := wideWidthInput(t, proc, keys)
				for key, count := range expected {
					want[key] += count
				}
				child = colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
				partial.Children = nil
				partial.AppendChild(child)
				require.NoError(t, partial.Prepare(proc))
				require.Equal(t, int32(HStr), partial.ctr.mtyp)
				outputs := collectBatches(t, partial, proc)
				require.NotEmpty(t, outputs)
				for _, output := range outputs {
					copied := cloneBatch(t, proc, output)
					copies = append(copies, copied)
					require.GreaterOrEqual(t, len(copied.ExtraBuf), 4)
					require.Equal(t, int32(HStr), types.DecodeInt32(copied.ExtraBuf[:4]))
				}
				partial.Reset(proc, false, nil)
				child.Free(proc, false, nil)
				child = nil
			}
			merge = newMergeGroupOp(aggs)
			mergeChild = colexec.NewMockOperator().WithBatchs(copies)
			copies = nil // The mock child now owns every cloned partial.
			merge.AppendChild(mergeChild)
			require.NoError(t, merge.Prepare(proc))
			outputs := collectBatches(t, merge, proc)
			require.Equal(t, int32(HStr), merge.ctr.mtyp)
			require.Equal(t, want, wideWidthCounts(t, outputs, keys))
			merge.Reset(proc, false, nil)
		})
	}
}
