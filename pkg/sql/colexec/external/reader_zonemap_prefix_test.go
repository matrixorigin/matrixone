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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type prefixReadFailFS struct {
	fileservice.FileService
	err error
}

func (fs *prefixReadFailFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if fs.err != nil {
		return fs.err
	}
	return fs.FileService.Read(ctx, v)
}

func TestZonemapReaderPrefixScratchLifecycle(t *testing.T) {
	proc, other := testutil.NewProcess(t), testutil.NewProcess(t)
	defer proc.Free()
	defer other.Free()
	ctx := proc.Ctx
	fs := &prefixReadFailFS{FileService: testutil.NewSharedFS()}
	defer fs.Close(ctx)
	writer := ioutil.ConstructWriter(0, []uint16{0}, -1, false, false, fs)
	for _, values := range [][]string{{"abc", "az"}, {"zebra"}, {"abacus"}} {
		func() {
			bat := batch.NewWithSize(1)
			defer bat.Clean(proc.Mp())
			bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			for _, value := range values {
				require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(value), false, proc.Mp()))
			}
			bat.SetRowCount(len(values))
			_, err := writer.WriteBatch(bat)
			require.NoError(t, err)
		}()
	}
	_, _, err := writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	name := stats.ObjectName().String()
	paramFor := func(prefix string) *ExternalParam {
		v := vector.NewVec(types.T_varchar.ToType())
		defer v.Free(proc.Mp())
		require.NoError(t, vector.AppendBytes(v, []byte(prefix), false, proc.Mp()))
		data, err := v.MarshalBinary()
		require.NoError(t, err)
		expr := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "prefix_in", Obj: function.PrefixInFunctionEncodedID},
			Args: []*plan.Expr{
				{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
				{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Data: data}}},
			},
		}}}
		return &ExternalParam{
			ExParamConst: ExParamConst{
				Attrs:  []plan.ExternAttr{{ColName: "k", ColIndex: 0, ColFieldIndex: 0}},
				Cols:   []*plan.ColDef{{Name: "k", Typ: plan.Type{Id: int32(types.T_varchar)}}},
				Extern: &tree.ExternParam{ExParam: tree.ExParam{FileService: fs}},
			},
			ExParam: ExParam{
				Fileparam: &ExFileparam{Filepath: name},
				Filter:    &FilterParam{FilterExpr: expr, AuxIdCnt: plan2.AssignAuxIdForExpr(expr, 0), columnMap: map[int]int{0: 0}, zonemappable: true},
			},
		}
	}
	param := paramFor("a")
	reader := NewZonemapReader(param, proc)
	defer reader.Close()
	analyzer := process.NewAnalyzer(0, false, false, "prefix-reader")
	readOne := func(ctx context.Context, p *process.Process) (bool, error) {
		out := batch.NewWithSize(1)
		defer out.Clean(p.Mp())
		out.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		return reader.ReadBatch(ctx, out, p, analyzer)
	}
	readAll := func(p *process.Process) []string {
		out := batch.NewWithSize(1)
		out.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		defer out.Clean(p.Mp())
		var got []string
		var prepared *vector.Vector
		for {
			done, err := reader.ReadBatch(p.Ctx, out, p, analyzer)
			require.NoError(t, err)
			for i := range out.RowCount() {
				got = append(got, string(out.Vecs[0].GetBytesAt(i)))
			}
			rhs := reader.vecs[reader.param.Filter.FilterExpr.GetF().Args[1].AuxId]
			require.NotNil(t, rhs)
			if prepared != nil {
				require.Same(t, prepared, rhs, "all blocks in one open share preparation")
			}
			prepared = rhs
			out.CleanOnlyData()
			if done {
				break
			}
		}
		return got
	}
	_, err = reader.Open(param, proc)
	require.NoError(t, err)
	require.Equal(t, []string{"abc", "az", "abacus"}, readAll(proc))
	require.NoError(t, reader.Close())
	require.NoError(t, reader.Close())
	require.Nil(t, reader.vecs)
	require.Nil(t, reader.mp)
	require.Zero(t, proc.Mp().CurrNB())

	// Reopen with the same AuxIds but a new predicate, then change the pool.
	_, err = reader.Open(paramFor("z"), proc)
	require.NoError(t, err)
	require.Equal(t, []string{"zebra"}, readAll(other))
	require.Same(t, other.Mp(), reader.mp)
	require.NoError(t, reader.Close())
	require.Zero(t, proc.Mp().CurrNB())
	require.Zero(t, other.Mp().CurrNB())

	// A pool change after preparation frees the old generation with its owner.
	_, err = reader.Open(paramFor("a"), proc)
	require.NoError(t, err)
	_, err = readOne(ctx, proc)
	require.NoError(t, err)
	old := reader.vecs[reader.param.Filter.FilterExpr.GetF().Args[1].AuxId]
	require.Equal(t, []string{"abacus"}, readAll(other))
	require.NotSame(t, old, reader.vecs[reader.param.Filter.FilterExpr.GetF().Args[1].AuxId])
	require.NoError(t, reader.Close())
	require.Zero(t, proc.Mp().CurrNB())
	require.Zero(t, other.Mp().CurrNB())

	for _, canceled := range []bool{false, true} {
		_, err = reader.Open(paramFor("a"), proc)
		require.NoError(t, err)
		_, err = readOne(ctx, proc)
		require.NoError(t, err)
		callCtx, cancel := context.WithCancel(ctx)
		want := errors.New("prefix reader injected IO failure")
		if canceled {
			cancel()
			want = context.Canceled
		} else {
			fs.err = want
		}
		_, err = readOne(callCtx, proc)
		cancel()
		require.ErrorIs(t, err, want)
		require.NoError(t, reader.Close())
		require.Zero(t, proc.Mp().CurrNB())
		fs.err = nil
	}

	// Early close after a successful block must also release this generation.
	_, err = reader.Open(paramFor(""), proc)
	require.NoError(t, err)
	_, err = readOne(ctx, proc)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Zero(t, proc.Mp().CurrNB())
}
