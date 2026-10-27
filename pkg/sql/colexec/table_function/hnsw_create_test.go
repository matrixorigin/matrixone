// Copyright 2022 Matrix Origin
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

package table_function

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// hnswSource is the source table an hnsw_create test reads: the rows its streamed SELECT
// returns, with the type of their vector column.
type hnswSource struct {
	typ  types.T
	dim  int32
	rows [][]float64
	// streamErr fails the streamed SELECT after its rows.
	streamErr error
}

type hnswCreateRun struct {
	tf    *TableFunction
	proc  *process.Process
	sqls  []string
	state *hnswCreateState
}

func hnswCfg(src hnswSource, dict bool) string {
	cfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__meta", IndexTable: "__index",
		PKey: "id", KeyPart: "v", KeyPartType: int32(src.typ), KeyPartWidth: src.dim}
	if dict {
		cfg.DictTable = "__dict"
	}
	b, _ := json.Marshal(cfg)
	return string(b)
}

func (src hnswSource) batch(t *testing.T, mp *mpool.MPool) *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.New(src.typ, src.dim, 0))
	for i, row := range src.rows {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(i), false, mp))
		var b []byte
		switch src.typ {
		case types.T_array_float64:
			b = types.ArrayToBytes(row)
		case types.T_array_int8:
			v := make([]int8, len(row))
			for j, x := range row {
				v[j] = int8(x)
			}
			b = types.ArrayToBytes(v)
		default:
			v := make([]float32, len(row))
			for j, x := range row {
				v[j] = float32(x)
			}
			if src.typ == types.T_array_float16 {
				b = types.ArrayToBytes(types.Float32ToFloat16Slice(v))
			} else {
				b = types.ArrayToBytes(v)
			}
		}
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], b, false, mp))
	}
	bat.SetRowCount(len(src.rows))
	return bat
}

func int64Result(t *testing.T, mp *mpool.MPool, n int64) executor.Result {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], n, false, mp))
	bat.SetRowCount(1)
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

// runHnswCreate prepares hnsw_create with params and cfg, then starts and ends it over src.
func runHnswCreate(t *testing.T, params, cfg string, src hnswSource) (*hnswCreateRun, error) {
	mp := mpool.MustNewZero()
	run := &hnswCreateRun{proc: testutil.NewProcessWithMPool(t, "", mp)}
	run.tf = &TableFunction{
		Attrs:    []string{"status"},
		Rets:     []*plan.ColDef{{Name: "status", Typ: plan.Type{Id: int32(types.T_int32)}}},
		FuncName: "hnsw_create",
		Params:   []byte(params),
		Args: []*plan.Expr{{
			Typ:  plan.Type{Id: int32(types.T_varchar), Width: 512},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: cfg}}},
		}},
	}
	query := func(sql string) (executor.Result, error) {
		run.sqls = append(run.sqls, sql)
		switch {
		case strings.HasPrefix(sql, "SELECT count(*)"):
			return int64Result(t, mp, int64(len(src.rows))), nil
		case strings.HasPrefix(sql, "SELECT SAMPLE("):
			bat := src.batch(t, mp)
			bat.Vecs = bat.Vecs[1:]
			return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
		}
		return executor.Result{Mp: mp}, nil
	}
	mockIndexTxn(t, run.proc, func(sql string, _ executor.StatementOption) (executor.Result, error) { return query(sql) })
	oldSql, oldStream := hnsw_runSql, hnsw_runStreamingSql
	t.Cleanup(func() { hnsw_runSql, hnsw_runStreamingSql = oldSql, oldStream })
	hnsw_runSql = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) { return query(sql) }
	hnsw_runStreamingSql = func(_ context.Context, _ *sqlexec.SqlProcess, sql string, ch chan executor.Result, _ chan error) (executor.Result, error) {
		run.sqls = append(run.sqls, sql)
		ch <- executor.Result{Mp: mp, Batches: []*batch.Batch{src.batch(t, mp)}}
		return executor.Result{}, src.streamErr
	}

	require.NoError(t, run.tf.Prepare(run.proc))
	run.state = run.tf.ctr.state.(*hnswCreateState)
	for i := range run.tf.ctr.executorsForArgs {
		vec, err := run.tf.ctr.executorsForArgs[i].Eval(run.proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
		require.NoError(t, err)
		run.tf.ctr.argVecs[i] = vec
	}
	t.Cleanup(func() { run.state.free(run.tf, run.proc, false, nil) })
	if err := run.state.start(run.tf, run.proc, 0, nil); err != nil {
		return run, err
	}
	res, err := run.state.call(run.tf, run.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, res.Status)
	return run, run.state.end(run.tf, run.proc)
}

func (r *hnswCreateRun) find(prefix string) []string {
	var out []string
	for _, sql := range r.sqls {
		if strings.HasPrefix(sql, prefix) {
			out = append(out, sql)
		}
	}
	return out
}

func hnswRows(n, dim int) [][]float64 {
	rows := make([][]float64, n)
	for i := range rows {
		rows[i] = make([]float64, dim)
		for d := range rows[i] {
			rows[i][d] = float64((i*7+d*3)%11) - 5
		}
	}
	return rows
}

const hnswL2 = `{"op_type": "vector_l2_ops"}`

// hnsw_create reads the source itself: it counts the vectors, streams every one into the
// build, then clears the hidden tables and persists the models.
func TestHnswCreateBuildsFromSource(t *testing.T) {
	for _, typ := range []types.T{types.T_array_float32, types.T_array_float64, types.T_array_float16} {
		t.Run(typ.String(), func(t *testing.T) {
			src := hnswSource{typ: typ, dim: 3, rows: hnswRows(20, 3)}
			run, err := runHnswCreate(t, hnswL2, hnswCfg(src, false), src)
			require.NoError(t, err)
			require.Equal(t, []string{"SELECT count(*) FROM `db`.`src` WHERE `v` IS NOT NULL"}, run.find("SELECT count"))
			require.Equal(t, []string{"SELECT `id`, `v` FROM `db`.`src` WHERE `v` IS NOT NULL"}, run.find("SELECT `id`"))
			require.Empty(t, run.find("SELECT SAMPLE"), "a float index trains no range")
			require.Equal(t, []string{
				"DELETE FROM `db`.`__meta` WHERE TRUE",
				"DELETE FROM `db`.`__index` WHERE TRUE",
			}, run.find("DELETE"))
			require.Len(t, run.find("INSERT INTO `db`.`__meta`"), 1)

			var rows int64
			for _, m := range run.models() {
				rows += m.len
			}
			require.EqualValues(t, 20, rows)
		})
	}
}

// max_index_capacity splits the build into models, the last sized to the rows left.
func TestHnswCreateCapacity(t *testing.T) {
	src := hnswSource{typ: types.T_array_float32, dim: 3, rows: hnswRows(5, 3)}
	run, err := runHnswCreate(t, `{"op_type": "vector_l2_ops", "max_index_capacity": "2"}`, hnswCfg(src, false), src)
	require.NoError(t, err)
	var caps []uint
	for _, m := range run.models() {
		caps = append(caps, m.maxCapacity)
	}
	require.Equal(t, []uint{2, 2, 1}, caps)
}

// An int8 index on a float column trains its range from a sample and writes it to its dict
// table; on a vecint8 column it writes the column type's range without sampling.
func TestHnswCreateDict(t *testing.T) {
	src := hnswSource{typ: types.T_array_float32, dim: 3, rows: hnswRows(20, 3)}
	run, err := runHnswCreate(t, `{"op_type": "vector_l2_ops", "quantization": "int8"}`, hnswCfg(src, true), src)
	require.NoError(t, err)
	require.Len(t, run.find("SELECT SAMPLE(`v`, 100.000000 PERCENT) FROM `db`.`src`"), 1)
	require.Contains(t, run.find("DELETE"), "DELETE FROM `db`.`__dict` WHERE TRUE")
	dict := run.find("INSERT INTO `db`.`__dict`")
	require.Len(t, dict, 1)
	require.Contains(t, dict[0], "('quantize_min', '-5'), ('quantize_max', '5')")
	require.True(t, run.state.bounds.Set)
	for _, m := range run.models() {
		require.Equal(t, run.state.bounds, m.bounds)
	}

	src = hnswSource{typ: types.T_array_int8, dim: 3, rows: hnswRows(20, 3)}
	run, err = runHnswCreate(t, `{"op_type": "vector_l2_ops", "quantization": "int8"}`, hnswCfg(src, true), src)
	require.NoError(t, err)
	require.Empty(t, run.find("SELECT SAMPLE"))
	dict = run.find("INSERT INTO `db`.`__dict`")
	require.Len(t, dict, 1)
	require.Contains(t, dict[0], "('quantize_min', '-128'), ('quantize_max', '127')")
}

// An empty source still clears every hidden table and builds nothing.
func TestHnswCreateEmptySource(t *testing.T) {
	src := hnswSource{typ: types.T_array_float32, dim: 3}
	run, err := runHnswCreate(t, `{"op_type": "vector_l2_ops", "quantization": "int8"}`, hnswCfg(src, true), src)
	require.NoError(t, err)
	require.Empty(t, run.find("SELECT `id`"), "nothing to read")
	require.Empty(t, run.find("SELECT SAMPLE"))
	require.Equal(t, []string{
		"DELETE FROM `db`.`__meta` WHERE TRUE",
		"DELETE FROM `db`.`__index` WHERE TRUE",
		"DELETE FROM `db`.`__dict` WHERE TRUE",
	}, run.find("DELETE"))
	require.Empty(t, run.find("INSERT"), "an index built empty trains its range on its first sync")
}

func TestHnswCreateStreamError(t *testing.T) {
	src := hnswSource{typ: types.T_array_float32, dim: 3, rows: hnswRows(4, 3), streamErr: moerr.NewInternalErrorNoCtx("scan failed")}
	_, err := runHnswCreate(t, hnswL2, hnswCfg(src, false), src)
	require.ErrorContains(t, err, "scan failed")
}

func TestHnswCreateBadInput(t *testing.T) {
	src := hnswSource{typ: types.T_array_float32, dim: 3, rows: hnswRows(4, 3)}
	for _, params := range []string{"{",
		`{"op_type": "vector_cos_ops"}`,
		`{"op_type": "vector_l2_ops", "m":"notnumber"}`,
		`{"op_type": "vector_l2_ops", "ef_construction":"notnumber"}`,
		`{"op_type": "vector_l2_ops", "ef_search":"notnumber"}`,
		`{"op_type": "vector_l2_ops", "quantization":"bad"}`,
	} {
		_, err := runHnswCreate(t, params, hnswCfg(src, false), src)
		require.Error(t, err, params)
	}

	noColumns, _ := json.Marshal(vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__meta", IndexTable: "__index"})
	for _, cfg := range []string{"", "{", string(noColumns)} {
		_, err := runHnswCreate(t, hnswL2, cfg, src)
		require.Error(t, err, cfg)
	}

	// a vector of the wrong dimension
	wrong := hnswSource{typ: types.T_array_float32, dim: 3, rows: [][]float64{{1, 2}}}
	_, err := runHnswCreate(t, hnswL2, hnswCfg(src, false), wrong)
	require.Error(t, err)
}

// Without table names end() runs no SQL rather than an invalid "DELETE FROM ".
func TestHnswCreateEndNoConfigSkips(t *testing.T) {
	var called bool
	old := hnsw_runSql
	defer func() { hnsw_runSql = old }()
	hnsw_runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		called = true
		return executor.Result{}, nil
	}
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	st := &hnswCreateState{}
	require.Nil(t, st.end(&TableFunction{}, proc))
	require.False(t, called, "no SQL must run without a valid table config")
}

type hnswModelInfo struct {
	len         int64
	maxCapacity uint
	bounds      hnsw.QuantizeBounds
}

// models reports the models a run built.
func (r *hnswCreateRun) models() []hnswModelInfo {
	switch b := r.state.build.(type) {
	case typedHnswBuilder[float32]:
		return modelInfos(b.GetIndexes())
	case typedHnswBuilder[float64]:
		return modelInfos(b.GetIndexes())
	case typedHnswBuilder[types.Float16]:
		return modelInfos(b.GetIndexes())
	case typedHnswBuilder[types.BF16]:
		return modelInfos(b.GetIndexes())
	case typedHnswBuilder[int8]:
		return modelInfos(b.GetIndexes())
	case typedHnswBuilder[uint8]:
		return modelInfos(b.GetIndexes())
	}
	return nil
}

func modelInfos[T types.ArrayElement](models []*hnsw.HnswModel[T]) []hnswModelInfo {
	out := make([]hnswModelInfo, 0, len(models))
	for _, m := range models {
		out = append(out, hnswModelInfo{m.Len.Load(), m.MaxCapacity, m.Bounds})
	}
	return out
}
