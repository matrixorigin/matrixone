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
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec/algos/kmeans"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec/algos/kmeans/elkans"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	defaultKmeansMaxIteration   = 10
	defaultKmeansDeltaThreshold = 0.01
	defaultKmeansDistanceType   = kmeans.L2Distance
	defaultKmeansInitType       = kmeans.Random
	defaultKmeansClusterCnt     = 1
	defaultKmeansNormalize      = false

	configSeparator = ","
)

var (
	ClusterCentersSupportTypes = []types.T{
		types.T_array_float32, types.T_array_float64,
	}

	distTypeStrToEnum = map[string]kmeans.DistanceType{
		"vector_l2_ops":     kmeans.L2Distance,
		"vector_ip_ops":     kmeans.InnerProduct,
		"vector_cosine_ops": kmeans.CosineDistance,
		"vector_l1_ops":     kmeans.L1Distance,
	}

	initTypeStrToEnum = map[string]kmeans.InitType{
		"random":         kmeans.Random,
		"kmeansplusplus": kmeans.KmeansPlusPlus,
	}
)

type ivfCreateState struct {
	inited  bool
	param   vectorindex.IvfParam
	tblcfg  vectorindex.IndexTableConfig
	idxcfg  vectorindex.IndexConfig
	data    [][]float64
	nsample uint
	offset  int

	// holding one call batch, tokenizedState owns it.
	batch *batch.Batch
}

func convertToF64(ar []float32) []float64 {
	newar := make([]float64, len(ar))
	var v float32
	var i int
	for i, v = range ar {
		newar[i] = float64(v)
	}
	return newar
}

func (u *ivfCreateState) end(tf *TableFunction, proc *process.Process) error {

	var clusterer kmeans.Clusterer
	var centers [][]float64
	var err error

	os.Stderr.WriteString(fmt.Sprintf("END nsample %d\n", len(u.data)))

	if clusterer, err = elkans.NewKMeans(
		u.data, int(u.idxcfg.Ivfflat.Lists),
		defaultKmeansMaxIteration,
		defaultKmeansDeltaThreshold,
		kmeans.DistanceType(u.idxcfg.Ivfflat.Metric),
		kmeans.InitType(u.idxcfg.Ivfflat.InitType),
		u.idxcfg.Ivfflat.Normalize); err != nil {
		return err
	}
	if centers, err = clusterer.Cluster(); err != nil {
		return err
	}

	version, err := func() (int64, error) {
		sql := fmt.Sprintf("SELECT CAST(`%s` AS BIGINT) FROM `%s` WHERE `%s` = 'version'",
			catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
			u.tblcfg.MetadataTable,
			catalog.SystemSI_IVFFLAT_TblCol_Metadata_key)

		res, err := ivf_runSql(proc, sql)
		if err != nil {
			return 0, err
		}
		defer res.Close()

		if len(res.Batches) == 0 {
			return 0, moerr.NewInternalError(proc.Ctx, "version not found")
		}

		version := int64(0)
		bat := res.Batches[0]
		if bat.RowCount() == 1 {
			version = vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[0], 0)
			//logutil.Infof("NROW = %d", nrow)
		}

		return version, nil
	}()

	// insert into centroid table
	values := make([]string, 0, len(centers))
	for i, c := range centers {
		s := types.ArrayToString[float64](c)
		values = append(values, fmt.Sprintf("(%d, %d, '%s')", version, i, s))
	}

	sql := fmt.Sprintf("INSERT INTO `%s`.`%s` (`%s`, `%s`, `%s`) VALUES %s", u.tblcfg.DbName, u.tblcfg.IndexTable,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
		strings.Join(values, ","))

	//os.Stderr.WriteString(sql)

	sqls := []string{sql}
	for _, s := range sqls {
		res, err := ivf_runSql(proc, s)
		if err != nil {
			return err
		}
		res.Close()
	}

	return nil
}

func (u *ivfCreateState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
}

func (u *ivfCreateState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {

	u.batch.CleanOnlyData()

	if u.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}

	// write the batch
	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil
}

func (u *ivfCreateState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}
}

func ivfCreatePrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &ivfCreateState{}

	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))

	return st, err

}

// start calling tvf on nthRow and put the result in u.batch.  Note that current tokenize impl will
// always return one batch per nthRow.
func (u *ivfCreateState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) (err error) {

	if !u.inited {
		if len(tf.Params) > 0 {
			err = json.Unmarshal([]byte(tf.Params), &u.param)
			if err != nil {
				return err
			}
		}

		if len(u.param.Lists) > 0 {
			val, err := strconv.Atoi(u.param.Lists)
			if err != nil {
				return err
			}
			u.idxcfg.Ivfflat.Lists = uint(val)
		} else {
			return moerr.NewInternalError(proc.Ctx, "invalid lists must be > 0")
		}

		u.idxcfg.Ivfflat.InitType = uint16(kmeans.KmeansPlusPlus)

		metric, ok := distTypeStrToEnum[u.param.OpType]
		if !ok {
			return moerr.NewInternalError(proc.Ctx, "invalid optype")
		}
		u.idxcfg.Ivfflat.Metric = uint16(metric)

		// IndexTableConfig
		cfgVec := tf.ctr.argVecs[0]
		if cfgVec.GetType().Oid != types.T_varchar {
			return moerr.NewInvalidInput(proc.Ctx, "First argument (IndexTableConfig must be a string")
		}
		if !cfgVec.IsConst() {
			return moerr.NewInternalError(proc.Ctx, "IndexTableConfig must be a String constant")
		}
		cfgstr := cfgVec.UnsafeGetStringAt(0)
		if len(cfgstr) == 0 {
			return moerr.NewInternalError(proc.Ctx, "IndexTableConfig is empty")
		}
		err := json.Unmarshal([]byte(cfgstr), &u.tblcfg)
		if err != nil {
			return err
		}

		// support both vecf32 and vecf64
		f32aVec := tf.ctr.argVecs[1]
		supported := false
		for _, t := range ClusterCentersSupportTypes {
			if f32aVec.GetType().Oid == t {
				supported = true
				break
			}
		}
		if !supported {
			return moerr.NewInvalidInput(proc.Ctx, "Third argument (vector must be a vecf32 or vecf64 type")
		}
		dimension := f32aVec.GetType().Width

		// dimension
		u.idxcfg.Ivfflat.Dimensions = uint(dimension)
		u.idxcfg.Type = "ivfflat"

		u.nsample = u.idxcfg.Ivfflat.Lists * 50
		if u.nsample < 10000 {
			u.nsample = 10000
		}
		u.data = make([][]float64, 0, u.nsample)

		u.batch = tf.createResultBatch()
		u.inited = true
	}

	// reset slice
	u.offset = 0

	// cleanup the batch
	u.batch.CleanOnlyData()

	if uint(len(u.data)) >= u.nsample {
		// enough sample data
		return nil
	}

	fpaVec := tf.ctr.argVecs[1]
	if fpaVec.IsNull(uint64(nthRow)) {
		return nil
	}

	var f64a []float64
	if fpaVec.GetType().Oid == types.T_array_float32 {
		f32a := types.BytesToArray[float32](fpaVec.GetBytesAt(nthRow))
		f64a = convertToF64(f32a)
	} else {
		f64a = types.BytesToArray[float64](fpaVec.GetBytesAt(nthRow))
	}

	u.data = append(u.data, append(make([]float64, 0, len(f64a)), f64a...))

	return nil
}

var ivf_runSql = ivf_runSql_fn

// run SQL in batch mode. Result batches will stored in memory and return once all result batches received.
func ivf_runSql_fn(proc *process.Process, sql string) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	//-------------------------------------------------------
	topContext := proc.GetTopContext()
	accountId, err := defines.GetAccountId(proc.Ctx)
	if err != nil {
		return executor.Result{}, err
	}
	//-------------------------------------------------------

	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(proc.GetTxnOperator()).
		WithDatabase(proc.GetSessionInfo().Database).
		WithTimeZone(proc.GetSessionInfo().TimeZone).
		WithAccountID(accountId)
	return exec.Exec(topContext, sql, opts)
}
