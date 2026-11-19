// Copyright 2022 Matrix Origin
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
	"strconv"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw"
	hnswrt "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/runtime"
	vimemory "github.com/matrixorigin/matrixone/pkg/vectorindex/memory"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	usearch "github.com/unum-cloud/usearch/golang"
)

// hnswCatalogHooks is the shared (stateless) catalog-hooks instance used for
// plugin-declared type validation (see pkg/indexplugin/catalog).
var hnswCatalogHooks = hnswrt.CatalogHooks{}

var hnsw_runSql = sqlexec.RunSql
var hnsw_runStreamingSql = sqlexec.RunStreamingSql

type hnswCreateState struct {
	inited         bool
	started        bool
	policy         *executor.IndexRelationPolicy
	admittedConfig string
	build          hnswBuilder
	param          vectorindex.HnswParam
	tblcfg         vectorindex.IndexTableConfig
	idxcfg         vectorindex.IndexConfig
	// bounds is the range of an int8/uint8 index, written to its dict table by end.
	bounds hnsw.QuantizeBounds
	// ignored leaves out the rows the index cannot store; nil without an ignored table.
	ignored *ignoredBuild

	// holding one call batch, tokenizedState owns it.
	batch *batch.Batch
}

func (u *hnswCreateState) end(tf *TableFunction, proc *process.Process) error {
	if !u.started || u.policy == nil {
		return nil
	}
	sqlproc := sqlexec.NewSqlProcess(proc)
	sqlproc.IndexRelationPolicy = u.policy

	// The TVF owns clear+rebuild (the compile layer no longer pre-deletes on the sync path).
	// Without table names we cannot clear (a bare "DELETE FROM " with an empty identifier is
	// invalid), so skip rather than run malformed SQL.
	if u.tblcfg.MetadataTable == "" || u.tblcfg.IndexTable == "" {
		return nil
	}
	if u.ignored != nil {
		if err := u.ignored.allBad(); err != nil {
			return err
		}
	}

	// Clear the old index unconditionally — even when nothing was built (REBUILD to zero docs must
	// still empty the index), so the DELETEs run before the early-out on a nil builder. Cross-CN
	// cache freshness is a per-model checksum multiset (see hnsw generation.go), so the metadata
	// timestamp plays no role in freshness — plain wall-clock is fine here.
	deletes := hnsw.ClearIndexSqls(u.tblcfg)
	var sqls []string

	ts := time.Now().UnixMicro()
	provenance := metadataProvenance(proc, u.tblcfg.DbName, u.tblcfg.MetadataTable, u.policy)
	if u.build != nil {
		insertSqls, err := u.build.PreparePersistence(ts, buildSnapshotTS(proc), provenance)
		if err != nil {
			return err
		}
		sqls = append(sqls, insertSqls...)
	}
	if u.tblcfg.DictTable != "" && u.bounds.Set {
		sqls = append(sqls, hnsw.DictBoundsSql(u.tblcfg.DbName, u.tblcfg.DictTable, u.bounds))
	}
	buildTs := buildSnapshotTS(proc)

	return sqlexec.RunTxn(sqlproc, func(exec executor.TxnExecutor) error {
		stmt := sqlproc.StatementOption()
		run := func(sql string) error {
			res, err := exec.Exec(sql, stmt)
			res.Close()
			return err
		}
		for _, sql := range deletes {
			if err := run(sql); err != nil {
				return err
			}
		}
		if u.build != nil {
			if err := u.build.WriteIndexFiles(sqlproc, exec.Txn(), run); err != nil {
				return err
			}
		}
		for _, sql := range sqls {
			if err := run(sql); err != nil {
				return err
			}
		}
		if u.ignored != nil {
			return u.ignored.sqls(u.tblcfg, buildTs, run)
		}
		return nil
	})
}

func (u *hnswCreateState) reset(tf *TableFunction, proc *process.Process) {
	u.policy = nil
	u.started = false
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
}

func (u *hnswCreateState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {

	u.batch.CleanOnlyData()

	if u.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}

	// write the batch
	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil
}

func (u *hnswCreateState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}

	if u.build != nil {
		u.build.Destroy()
	}
	u.ignored.close()
}

func hnswCreatePrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &hnswCreateState{}

	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	if err != nil {
		return nil, err
	}
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))

	return st, err
}

// start builds the whole index on the first call: it counts the vectors of the source column,
// sizes the models from the host memory budget, trains the range of an int8/uint8 index from a
// sample, then reads every vector and adds it to the build. end persists the result.
func (u *hnswCreateState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) (err error) {
	defer func() {
		if err != nil {
			u.policy, u.started, u.inited = nil, false, false
			if u.build != nil {
				u.build.Destroy()
				u.build = nil
			}
			if u.ignored != nil {
				u.ignored.close()
				u.ignored = nil
			}
			if u.batch != nil {
				u.batch.Clean(proc.Mp())
				u.batch = nil
			}
		} else {
			u.started, u.inited = true, true
		}
	}()
	admit := func() error {
		cfgVec := tf.ctr.argVecs[0]
		if cfgVec.GetType().Oid != types.T_varchar || !cfgVec.IsConst() {
			return moerr.NewInvalidInput(proc.Ctx, "IndexTableConfig must be a string constant")
		}
		cfgstr := cfgVec.UnsafeGetStringAt(0)
		cfg, policy, err := admitVectorIndexMaintenance(proc, tf, "hnsw_create", cfgstr, u.admittedConfig, u.inited, hnswrt.MaintenanceOperands)
		if err != nil {
			return err
		}
		u.tblcfg, u.policy, u.admittedConfig = cfg, policy, string([]byte(cfgstr))
		return nil
	}
	if u.inited && u.policy == nil {
		if err = admit(); err != nil {
			return err
		}
	}
	if u.inited {
		u.batch.CleanOnlyData()
		return nil
	}
	u.batch = tf.createResultBatch()

	if len(tf.Params) > 0 {
		if err = sonic.Unmarshal([]byte(tf.Params), &u.param); err != nil {
			return err
		}
	}

	if len(u.param.M) > 0 {
		val, err := strconv.Atoi(u.param.M)
		if err != nil {
			return err
		}
		u.idxcfg.Usearch.Connectivity = uint(val)
	}

	metrictype, ok := metric.OpTypeToUsearchMetric[u.param.OpType]
	if !ok {
		return moerr.NewInternalError(proc.Ctx, "Invalid op_type")
	}
	u.idxcfg.OpType = u.param.OpType
	u.idxcfg.Usearch.Metric = metrictype

	if len(u.param.EfConstruction) > 0 {
		val, err := strconv.Atoi(u.param.EfConstruction)
		if err != nil {
			return err
		}
		u.idxcfg.Usearch.ExpansionAdd = uint(val)
	}

	if len(u.param.EfSearch) > 0 {
		val, err := strconv.Atoi(u.param.EfSearch)
		if err != nil {
			return err
		}
		u.idxcfg.Usearch.ExpansionSearch = uint(val)
	}

	if err = admit(); err != nil {
		return err
	}
	if u.tblcfg.PKey == "" || u.tblcfg.KeyPart == "" || u.tblcfg.KeyPartWidth <= 0 {
		return moerr.NewInternalError(proc.Ctx, "IndexTableConfig is missing the source columns")
	}

	partType := u.tblcfg.KeyPartType
	u.idxcfg.Usearch.Quantization, err = hnsw.StorageQuantization(u.param.Quantization, partType)
	if err != nil {
		return err
	}
	u.idxcfg.Usearch.Dimensions = uint(u.tblcfg.KeyPartWidth)
	u.idxcfg.Type = vectorindex.HNSW

	rows, err := fetchSrcTableRowCount(proc, hnsw_runSql, u.tblcfg.DbName, u.tblcfg.SrcTable, u.tblcfg.KeyPart, u.policy)
	if err != nil {
		return err
	}
	u.tblcfg.DataSize = rows

	// max_index_capacity: the index's own option, else the session variable; 0 sizes each
	// model from the host memory budget.
	explicit, err := indexplugin.AlgoParamInt(u.param.MaxIndexCapacity,
		proc.GetResolveVariableFunc(), "hnsw_max_index_capacity", hnswrt.DefaultMaxIndexCapacity)
	if err != nil {
		return err
	}
	if u.idxcfg.IndexCapacity, err = hnsw.PlanCapacity(explicit, u.idxcfg.Usearch); err != nil {
		return err
	}

	// An int8/uint8 index maps every vector with one range: the type range of a
	// vecint8/vecuint8 column, else trained from a random sample of the column.
	if hnsw.NeedsQuantizeBounds(u.idxcfg.Usearch.Quantization) {
		u.bounds = hnsw.BaseBounds(partType)
		if !u.bounds.Set && rows > 0 {
			sqlproc := sqlexec.NewSqlProcess(proc)
			sqlproc.IndexRelationPolicy = u.policy
			u.bounds, err = hnsw.SampleQuantizeBounds(sqlproc, u.tblcfg, int(u.tblcfg.KeyPartWidth))
			if err != nil {
				return err
			}
		}
	}
	if rows == 0 {
		return nil
	}

	u.ignored = newIgnoredBuild(u.tblcfg, vimemory.HostSpillDir(proc.GetTopContext(), proc.Base.FileService, proc.GetService()))
	uid := fmt.Sprintf("%s:%d:%d", tf.CnAddr, tf.MaxParallel, tf.ParallelID)
	sqlproc := sqlexec.NewSqlProcess(proc)
	sqlproc.IndexRelationPolicy = u.policy
	u.build, err = newHnswBuilder(sqlproc, uid, partType, u.idxcfg, u.tblcfg)
	if err == nil {
		u.build.SetQuantizeBounds(u.bounds)
		u.build.SetRowCount(rows)
	}
	if err != nil {
		return err
	}
	return u.addAll(proc)
}

// addAll reads the primary key and vector of every row with a vector and adds it to the build.
func (u *hnswCreateState) addAll(proc *process.Process) (err error) {
	sql := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s IS NOT NULL",
		sqlquote.Ident(u.tblcfg.PKey), sqlquote.Ident(u.tblcfg.KeyPart),
		sqlquote.QualifiedIdent(u.tblcfg.DbName, u.tblcfg.SrcTable), sqlquote.Ident(u.tblcfg.KeyPart))

	streamCh := make(chan executor.Result, 2)
	// A failed SELECT is sent on errCh by the executor and again by the goroutine below.
	errCh := make(chan error, 2)
	ctx, cancel := context.WithCancelCause(proc.Ctx)
	defer cancel(nil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			close(streamCh)
			wg.Done()
		}()
		sqlproc := sqlexec.NewSqlProcess(proc)
		sqlproc.IndexRelationPolicy = u.policy
		if _, err := hnsw_runStreamingSql(ctx, sqlproc, sql, streamCh, errCh); err != nil {
			errCh <- err
		}
	}()

	closed := false
	for !closed && err == nil {
		select {
		case res, ok := <-streamCh:
			if !ok {
				closed = true
				break
			}
			err = u.addResult(res)
			res.Close()
		case err = <-errCh:
		case <-proc.Ctx.Done():
			err = proc.Ctx.Err()
		}
	}
	if err != nil {
		cancel(err)
		for res := range streamCh {
			res.Close()
		}
	}
	wg.Wait()
	if err == nil {
		select {
		case err = <-errCh:
		default:
		}
	}
	return err
}

func (u *hnswCreateState) addResult(res executor.Result) error {
	for _, bat := range res.Batches {
		if bat == nil || len(bat.Vecs) < 2 {
			continue
		}
		idVec, faVec := bat.Vecs[0], bat.Vecs[1]
		if !catalogplugin.SupportsPrimaryKeyType(hnswCatalogHooks, idVec.GetType().Oid) {
			return moerr.NewInvalidInputNoCtx("hnsw primary key must be a bigint")
		}
		for i := 0; i < bat.RowCount(); i++ {
			if idVec.IsNull(uint64(i)) || faVec.IsNull(uint64(i)) {
				continue
			}
			id := vector.GetFixedAtNoTypeCheck[int64](idVec, i)
			err := u.build.addBytes(id, faVec.GetBytesAt(i))
			if u.ignored == nil {
				if err != nil {
					return err
				}
				continue
			}
			if err == nil {
				u.ignored.stored()
				continue
			}
			if err := u.ignored.leaveOut(strconv.FormatInt(id, 10), err); err != nil {
				return err
			}
		}
	}
	return nil
}

// hnswBuilder is an hnsw.HnswBuild of the element type of the indexed column.
type hnswBuilder interface {
	SetQuantizeBounds(hnsw.QuantizeBounds)
	SetRowCount(int64)
	PreparePersistence(ts int64, buildTS int64, provenance bool) ([]string, error)
	WriteIndexFiles(sqlproc *sqlexec.SqlProcess, txn client.TxnOperator, run func(sql string) error) error
	Destroy() error
	// addBytes adds a vector given in the column's encoding; an error with an unstorable code
	// tells why the index cannot store it.
	addBytes(id int64, data []byte) error
}

type typedHnswBuilder[T types.ArrayElement] struct {
	*hnsw.HnswBuild[T]
	dim  uint
	kind usearch.Quantization
}

func (b typedHnswBuilder[T]) addBytes(id int64, data []byte) error {
	vec := types.BytesToArray[T](data)
	if uint(len(vec)) != b.dim {
		return moerr.NewIndexVectorDimensionNoCtx(fmt.Sprintf("%d, the index has %d", len(vec), b.dim))
	}
	if err := hnsw.CheckStorable(vec, b.kind); err != nil {
		return err
	}
	return b.Add(id, vec)
}

func newTypedHnswBuilder[T types.ArrayElement](sqlproc *sqlexec.SqlProcess, uid string,
	idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) (hnswBuilder, error) {
	build, err := hnsw.NewHnswBuild[T](sqlproc, uid, 1, idxcfg, tblcfg)
	if err != nil {
		return nil, err
	}
	return typedHnswBuilder[T]{HnswBuild: build, dim: idxcfg.Usearch.Dimensions, kind: idxcfg.Usearch.Quantization}, nil
}

// newHnswBuilder returns a build that takes vectors of the column type partType as they are.
func newHnswBuilder(sqlproc *sqlexec.SqlProcess, uid string, partType int32,
	idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) (hnswBuilder, error) {
	// typeswitch:partial vecf8/vecf4 cannot carry a vector index (rejected at DDL)
	switch types.T(partType) {
	case types.T_array_float32:
		return newTypedHnswBuilder[float32](sqlproc, uid, idxcfg, tblcfg)
	case types.T_array_float64:
		return newTypedHnswBuilder[float64](sqlproc, uid, idxcfg, tblcfg)
	case types.T_array_float16:
		return newTypedHnswBuilder[types.Float16](sqlproc, uid, idxcfg, tblcfg)
	case types.T_array_bf16:
		return newTypedHnswBuilder[types.BF16](sqlproc, uid, idxcfg, tblcfg)
	case types.T_array_int8:
		return newTypedHnswBuilder[int8](sqlproc, uid, idxcfg, tblcfg)
	case types.T_array_uint8:
		return newTypedHnswBuilder[uint8](sqlproc, uid, idxcfg, tblcfg)
	}
	return nil, moerr.NewInvalidInputNoCtxf("hnsw does not index vector type %s", types.T(partType).String())
}
