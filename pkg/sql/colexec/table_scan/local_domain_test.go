// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_scan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type localDomainTestReader struct {
	engine.Reader
	values []int64
	nullAt int
	reads  int
}

func (r *localDomainTestReader) Read(_ context.Context, _ []string, _ *plan.Expr, mp *mpool.MPool, out *batch.Batch) (bool, error) {
	r.reads++
	if r.reads > 1 || len(r.values) == 0 {
		return true, nil
	}
	for i, value := range r.values {
		if err := vector.AppendFixed(out.Vecs[0], value, i == r.nullAt, mp); err != nil {
			return false, err
		}
	}
	out.SetRowCount(len(r.values))
	return false, nil
}

func (*localDomainTestReader) Close() error { return nil }

func TestLocalDomainScanSourceAndProbe(t *testing.T) {
	for _, tc := range []struct {
		name  string
		empty bool
		ndv   float64
		pass  bool
	}{
		{name: "complete domain filters before output", ndv: 10000},
		{name: "empty domain avoids probe read", empty: true, ndv: 10000},
		{name: "unprofitable probe keeps all rows", ndv: 20, pass: true},
		{name: "unknown probe NDV keeps all rows", pass: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.SetMessageBoard(message.NewMessageBoard())
			t.Cleanup(proc.Free)
			budget := process.MustNewExecutionResourceBudget(16<<20, 16<<20)
			g, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			t.Cleanup(g.Close)
			require.NoError(t, proc.SetExecutionResourceBudgetForTesting(g))
			domain, _ := runtimefilter.NewLocalDomain(types.T_int64, 1000, 1, 1, 1000000)
			t.Cleanup(domain.Close)
			typ := plan.Type{Id: int32(types.T_int64)}
			makeScan := func(id int) *TableScan {
				s := NewArgument().WithTypes([]plan.Type{typ})
				s.Attrs = []string{"id"}
				s.SetAnalyzeControl(id, true)
				s.SetMaxParallel(1)
				t.Cleanup(func() {
					s.Free(proc, false, nil)
					s.closeReader()
					s.Release()
				})
				return s
			}
			source := makeScan(0)
			sourceReader := &localDomainTestReader{values: []int64{1, 2}, nullAt: -1}
			if tc.empty {
				sourceReader.values = nil
			}
			source.Reader = sourceReader
			source.LocalDomainBuilds = []runtimefilter.LocalDomainBinding{{Domain: domain, Column: "id"}}
			probe := makeScan(1)
			probeReader := &localDomainTestReader{values: []int64{1, 2, 99, 0}, nullAt: 3}
			probe.Reader = probeReader
			probe.LocalDomainProbes = []runtimefilter.LocalDomainBinding{{Domain: domain, Column: "id", ProbeNDV: tc.ndv}}
			require.NoError(t, source.Prepare(proc))
			require.NoError(t, probe.Prepare(proc))
			for {
				result, err := source.Call(proc)
				require.NoError(t, err)
				if result.Batch == nil {
					break
				}
			}
			result, err := probe.Call(proc)
			require.NoError(t, err)
			if tc.empty {
				require.Nil(t, result.Batch)
				require.Zero(t, probeReader.reads)
			} else if tc.pass {
				require.Equal(t, 4, result.Batch.RowCount())
				require.True(t, result.Batch.Vecs[0].IsNull(3), "PASS must not perform even NULL rejection")
			} else {
				require.NotNil(t, result.Batch)
				values := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[0])
				require.Contains(t, values, int64(1))
				require.Contains(t, values, int64(2))
				require.NotContains(t, values, int64(0), "ordinary equality cannot match a NULL probe")
				require.Equal(t, int64(4), probe.ctr.domainProbes[0].tested)
				require.Equal(t, int64(4-len(values)), probe.ctr.domainProbes[0].rejected)
			}
			source.Reset(proc, false, nil)
			probe.Reset(proc, false, nil)
			domain.Close()
			require.Zero(t, g.Snapshot().Used)
		})
	}
}

func TestLocalDomainProbeConstantAndShape(t *testing.T) {
	for _, name := range []string{"constant", "constant NULL", "wrong type", "short flat vector"} {
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.SetMessageBoard(message.NewMessageBoard())
			t.Cleanup(proc.Free)
			domain, _ := runtimefilter.NewLocalDomain(types.T_int64, 1000, 1, 1, 1000000)
			t.Cleanup(domain.Close)
			source, err := domain.Begin(proc, 0, 1)
			require.NoError(t, err)
			keys := testutil.MakeInt64Vector([]int64{7}, nil, proc.Mp())
			t.Cleanup(func() { keys.Free(proc.Mp()) })
			require.NoError(t, source.AddVector(proc.Ctx, keys, 1))
			source.Complete(false)
			bloom, empty, err := domain.Wait(proc.Ctx)
			require.NoError(t, err)
			require.False(t, empty)
			var probe *vector.Vector
			switch name {
			case "constant":
				probe, err = vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, proc.Mp())
			case "constant NULL":
				probe = vector.NewConstNull(types.T_int64.ToType(), 1, proc.Mp())
			case "wrong type":
				probe = testutil.MakeInt32Vector([]int32{7, 7, 7}, nil, proc.Mp())
			default:
				probe = testutil.MakeInt64Vector([]int64{7}, nil, proc.Mp())
			}
			require.NoError(t, err)
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = probe
			bat.SetRowCount(3)
			t.Cleanup(func() { bat.Clean(proc.Mp()) })
			scan := &TableScan{ctr: container{buf: bat, domainRows: make([]int64, 0, 3),
				domainProbes: []localDomainProbeState{{domain: domain, bloom: bloom, mask: make([]uint8, 3)}}}}
			require.NoError(t, scan.applyLocalDomains(proc))
			if name == "constant NULL" {
				require.Zero(t, bat.RowCount())
			} else {
				require.Equal(t, 3, bat.RowCount())
			}
			state := scan.ctr.domainProbes[0]
			if name == "wrong type" || name == "short flat vector" {
				require.True(t, state.pass)
				require.Equal(t, int64(3), state.bypassed)
			} else {
				require.Equal(t, int64(3), state.tested)
			}
		})
	}
}
