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
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/kmeans"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/kmeans/elkans"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/rand"
)

const (
	defaultKmeansMaxIteration   = 500
	defaultKmeansDeltaThreshold = 0.01
	defaultKmeansDistanceType   = metric.Metric_L2Distance
	defaultKmeansInitType       = kmeans.Random
	defaultKmeansClusterCnt     = 1
	defaultKmeansNormalize      = false
)

var (
	ClusterCentersSupportTypes = []types.T{
		types.T_array_float32, types.T_array_float64,
	}

	ivf_runSql = sqlexec.RunSql
)

type ivfCreateState struct {
	inited       bool
	param        vectorindex.IvfParam
	tblcfg       vectorindex.IndexTableConfig
	idxcfg       vectorindex.IndexConfig
	data         [][]float64
	rand         *rand.Rand
	nsample      uint
	sample_ratio float64
	offset       int

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

	if !u.inited || len(u.data) == 0 {
		return nil
	}

	version, err := ivfflat.GetVersion(proc, u.tblcfg)
	if err != nil {
		return err
	}

	nworker := vectorindex.GetConcurrencyForBuild(u.tblcfg.ThreadsBuild)

	// NOTE: We use L2 distance to caculate centroid.  Ivfflat metric just for searching.
	if clusterer, err = elkans.NewKMeans(
		u.data, int(u.idxcfg.Ivfflat.Lists),
		defaultKmeansMaxIteration,
		defaultKmeansDeltaThreshold,
		metric.MetricType(u.idxcfg.Ivfflat.Metric),
		kmeans.InitType(u.idxcfg.Ivfflat.InitType),
		u.idxcfg.Ivfflat.Normalize,
		int(nworker)); err != nil {
		return err
	}
	if centers, err = clusterer.Cluster(); err != nil {
		return err
	}

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

	if arg.MaxParallel > 1 {
		return nil, moerr.NewInternalError(proc.Ctx, fmt.Sprintf("Table Function ivf_create must run in single process. MaxParallel = %d\n", arg.MaxParallel))
	}

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
		//u.idxcfg.Ivfflat.InitType = uint16(kmeans.Random)

		metrictype, ok := metric.OpTypeToIvfMetric[u.param.OpType]
		if !ok {
			return moerr.NewInternalError(proc.Ctx, "invalid optype")
		}
		u.idxcfg.Ivfflat.Metric = uint16(metrictype)
		u.idxcfg.Ivfflat.Normalize = false

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
			return moerr.NewInvalidInput(proc.Ctx, "Second argument (vector must be a vecf32 or vecf64 type")
		}
		dimension := f32aVec.GetType().Width

		// dimension
		u.idxcfg.Ivfflat.Dimensions = uint(dimension)
		u.idxcfg.Type = "ivfflat"

		u.nsample = u.idxcfg.Ivfflat.Lists * 50
		min_nsample := uint(10000)
		if u.nsample < min_nsample {
			u.nsample = min_nsample
		}
		u.data = make([][]float64, 0, u.nsample)
		u.sample_ratio = float64(1)
		if u.tblcfg.DataSize > 0 {
			u.sample_ratio = float64(u.nsample) / float64(u.tblcfg.DataSize)
			if u.sample_ratio < 0.1 {
				u.sample_ratio = 0.1
			}
		}

		u.rand = rand.New(rand.NewSource(uint64(time.Now().UnixMicro())))
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

	if u.sample_ratio < u.rand.Float64() {
		// skip the sample
		return nil
	}

	var f64a []float64
	if fpaVec.GetType().Oid == types.T_array_float32 {
		f32a := types.BytesToArray[float32](fpaVec.GetBytesAt(nthRow))
		f64a = convertToF64(f32a)
	} else {
		f64a = types.BytesToArray[float64](fpaVec.GetBytesAt(nthRow))
	}

	if uint(len(f64a)) != u.idxcfg.Ivfflat.Dimensions {
		return moerr.NewInternalError(proc.Ctx, "vector dimension mismatch")
	}

	u.data = append(u.data, append(make([]float64, 0, len(f64a)), f64a...))

	return nil
}
