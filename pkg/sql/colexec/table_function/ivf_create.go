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
	"fmt"
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
	defaultKmeansDeltaThreshold = 0.01
	defaultKmeansDistanceType   = metric.Metric_L2Distance
	defaultKmeansInitType       = kmeans.Random
	defaultKmeansClusterCnt     = 1
	defaultKmeansNormalize      = false
)

var (
	ivf_runSql = sqlexec.RunSql
)

func IsClusterCentersSupportedType(t types.T) bool {
	return t == types.T_array_float32 || t == types.T_array_float64
}

type ivfCreateState struct {
	inited       bool
	tblcfg       vectorindex.IndexTableCfg
	idxcfg       vectorindex.IndexConfig
	data32       [][]float32
	data64       [][]float64
	rand         *rand.Rand
	nsample      uint
	sample_ratio float64
	offset       int

	// holding one call batch, tokenizedState owns it.
	batch *batch.Batch
}

func clustering[T types.RealNumbers](u *ivfCreateState, tf *TableFunction, proc *process.Process, data [][]T) error {

	var clusterer kmeans.Clusterer
	var err error
	var ok bool

	version, err := ivfflat.GetVersion(proc, u.tblcfg)
	if err != nil {
		return err
	}

	nworker := vectorindex.GetConcurrencyForBuild(u.tblcfg.ThreadsBuild())

	// NOTE: We use L2 distance to caculate centroid.  Ivfflat metric just for searching.
	var centers [][]T
	if clusterer, err = elkans.NewKMeans(
		data, int(u.idxcfg.Ivfflat.Lists),
		int(u.tblcfg.ExtraIVFCfg().KmeansMaxIteration()),
		defaultKmeansDeltaThreshold,
		metric.MetricType(u.idxcfg.Ivfflat.Metric),
		kmeans.InitType(u.idxcfg.Ivfflat.InitType),
		u.idxcfg.Ivfflat.Spherical, // For dense vector, spherical kmeans is false.
		int(nworker)); err != nil {
		return err
	}
	anycenters, err := clusterer.Cluster()
	if err != nil {
		return err
	}

	centers, ok = anycenters.([][]T)
	if !ok {
		return moerr.NewInternalError(proc.Ctx, "centers is not [][]float64")
	}

	// insert into centroid table
	values := make([]string, 0, len(centers))
	for i, c := range centers {
		s := types.ArrayToString(c)
		values = append(values, fmt.Sprintf("(%d, %d, '%s')", version, i, s))
	}

	if len(values) == 0 {
		return moerr.NewInternalError(proc.Ctx, "output centroids is empty")
	}

	sql := fmt.Sprintf(
		"INSERT INTO `%s`.`%s` (`%s`, `%s`, `%s`) VALUES %s",
		u.tblcfg.DBName(), u.tblcfg.IndexTable(),
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
		strings.Join(values, ","),
	)

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

func (u *ivfCreateState) end(tf *TableFunction, proc *process.Process) error {

	if !u.inited || (len(u.data32) == 0 && len(u.data64) == 0) {
		return nil
	}

	if u.data32 != nil {
		return clustering(u, tf, proc, u.data32)
	} else {
		return clustering(u, tf, proc, u.data64)
	}
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
func (u *ivfCreateState) start(
	tf *TableFunction,
	proc *process.Process,
	nthRow int,
	analyzer process.Analyzer,
) (err error) {

	if !u.inited {
		if err = InitIVFCfgFromParam(tf.Params, &u.idxcfg); err != nil {
			return
		}

		// IndexTableConfig
		cfgVec := tf.ctr.argVecs[0]
		if cfgVec.GetType().Oid != types.T_varchar {
			err = moerr.NewInvalidInput(proc.Ctx, "First argument (IndexTableConfig must be a string")
			return
		}
		if !cfgVec.IsConst() {
			err = moerr.NewInternalError(proc.Ctx, "IndexTableConfig must be a String constant")
			return
		}
		cfgstr := cfgVec.GetStringAt(0)
		if len(cfgstr) == 0 {
			err = moerr.NewInternalError(proc.Ctx, "IndexTableConfig is empty")
			return
		}
		u.tblcfg = vectorindex.IndexTableCfgV1(cfgstr)

		// support both vecf32 and vecf64
		f32aVec := tf.ctr.argVecs[1]
		if !IsClusterCentersSupportedType(f32aVec.GetType().Oid) {
			err = moerr.NewInvalidInput(proc.Ctx, "Second argument (vector must be a vecf32 or vecf64 type")
			return
		}
		// dimension
		dimension := f32aVec.GetType().Width
		u.idxcfg.Ivfflat.Dimensions = uint(dimension)

		u.idxcfg.Ivfflat.InitType = uint16(kmeans.KmeansPlusPlus)
		u.idxcfg.Ivfflat.Spherical = false // For Dense vector, spherical = false

		u.nsample = u.idxcfg.Ivfflat.Lists * 50
		train_percent := float64(u.tblcfg.ExtraIVFCfg().KmeansTrainPercent()) / float64(100)
		if u.tblcfg.ExtraIVFCfg().DataSize() > 0 {
			ns := uint(train_percent * float64(u.tblcfg.ExtraIVFCfg().DataSize()))
			if u.nsample > ns {
				u.nsample = ns
			}
		}
		min_nsample := uint(10000)
		if u.nsample < min_nsample {
			u.nsample = min_nsample
		}

		if f32aVec.GetType().Oid == types.T_array_float32 {
			u.data32 = make([][]float32, 0, u.nsample)
		} else {
			u.data64 = make([][]float64, 0, u.nsample)
		}

		u.sample_ratio = train_percent
		if u.tblcfg.ExtraIVFCfg().DataSize() > 0 {
			u.sample_ratio = float64(u.nsample) / float64(u.tblcfg.ExtraIVFCfg().DataSize())
			if u.sample_ratio < train_percent {
				u.sample_ratio = train_percent
			}
		}

		u.rand = rand.New(rand.NewSource(uint64(time.Now().UnixMicro())))
		u.batch = tf.createResultBatch()
		u.inited = true
		//os.Stderr.WriteString(fmt.Sprintf("nsample %d, train_percent %f, iter %d\n", u.nsample, train_percent, u.tblcfg.KmeansMaxIteration))
	}

	// reset slice
	u.offset = 0

	// cleanup the batch
	u.batch.CleanOnlyData()

	datasz := 0
	if u.data32 != nil {
		datasz = len(u.data32)
	} else {
		datasz = len(u.data64)
	}
	if uint(datasz) >= u.nsample {
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

	if fpaVec.GetType().Oid == types.T_array_float32 {
		f32a := types.BytesToArray[float32](fpaVec.GetBytesAt(nthRow))
		if uint(len(f32a)) != u.idxcfg.Ivfflat.Dimensions {
			return moerr.NewInternalError(proc.Ctx, "vector dimension mismatch")
		}
		u.data32 = append(u.data32, append(make([]float32, 0, len(f32a)), f32a...))
	} else {
		f64a := types.BytesToArray[float64](fpaVec.GetBytesAt(nthRow))
		if uint(len(f64a)) != u.idxcfg.Ivfflat.Dimensions {
			return moerr.NewInternalError(proc.Ctx, "vector dimension mismatch")
		}
		u.data64 = append(u.data64, append(make([]float64, 0, len(f64a)), f64a...))
	}

	return nil
}
