// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionAgg/algos/kmeans"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionAgg/algos/kmeans/elkans"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
	"strconv"
	"strings"
)

const (
	defaultKmeansMaxIteration   = 500
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
		"vector_l2_ops": kmeans.L2Distance,
	}

	initTypeStrToEnum = map[string]kmeans.InitType{
		"random":         kmeans.Random,
		"kmeansplusplus": kmeans.KmeansPlusPlus,
	}
)

func ClusterCentersReturnType(argType []types.Type) types.Type {
	return types.T_varchar.ToType()
}

type clusterCentersExec struct {
	singleAggInfo
	singleAggOptimizedInfo
	arg sBytesArg
	ret aggFuncBytesResult

	// groupData hold float64 which was converted from the inputting []byte.
	groupData []*vector.Vector

	// Kmeans parameters.
	clusterCnt uint64
	distType   kmeans.DistanceType
	initType   kmeans.InitType
	normalize  bool
}

func newClusterCentersExecutor(mg AggMemoryManager, info singleAggInfo) (AggFuncExec, error) {
	if info.distinct {
		return nil, moerr.NewInternalErrorNoCtx("do not support distinct for cluster_centers()")
	}
	return &clusterCentersExec{
		singleAggInfo: info,
		ret:           initBytesAggFuncResult(mg, info.retType, true),
		clusterCnt:    defaultKmeansClusterCnt,
		distType:      defaultKmeansDistanceType,
		initType:      defaultKmeansInitType,
		normalize:     defaultKmeansNormalize,
	}, nil
}

func (exec *clusterCentersExec) GroupGrow(more int) error {
	if err := exec.ret.grows(more); err != nil {
		return err
	}
	exec.groupData = append(exec.groupData, exec.ret.mg.GetVector(types.T_varchar.ToType()))
	return nil
}

func (exec *clusterCentersExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	if vectors[0].IsNull(uint64(row)) {
		return nil
	}
	if vectors[0].IsConst() {
		row = 0
	}

	value := vector.MustBytesCol(vectors[0])[row]
	return vector.AppendBytes(exec.groupData[groupIndex], value, false, exec.ret.mp)
}

func (exec *clusterCentersExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	if vectors[0].IsConstNull() {
		return nil
	}

	if vectors[0].IsConst() {
		value := vector.MustBytesCol(vectors[0])[0]
		return vector.AppendBytes(exec.groupData[groupIndex], value, false, exec.ret.mp)
	}

	exec.arg.prepare(vectors[0])
	for i, j := uint64(0), uint64(vectors[0].Length()); i < j; i++ {
		v, null := exec.arg.w.GetStrValue(i)
		if null {
			continue
		}
		if err := vector.AppendBytes(exec.groupData[groupIndex], v, false, exec.ret.mp); err != nil {
			return err
		}
	}
	return nil
}

func (exec *clusterCentersExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if vectors[0].IsConstNull() {
		return nil
	}

	if vectors[0].IsConst() {
		value := vector.MustBytesCol(vectors[0])[0]
		for i := 0; i < len(groups); i++ {
			if groups[i] != GroupNotMatched {
				if err := vector.AppendBytes(
					exec.groupData[groups[i]-1],
					value, false, exec.ret.mp); err != nil {
					return err
				}
			}
		}
		return nil
	}

	exec.arg.prepare(vectors[0])
	for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
		if groups[idx] != GroupNotMatched {
			v, null := exec.arg.w.GetStrValue(i)
			if !null {
				if err := vector.AppendBytes(
					exec.groupData[groups[idx]-1],
					v, false, exec.ret.mp); err != nil {
					return err
				}
			}
		}
		idx++
	}
	return nil
}

func (exec *clusterCentersExec) Merge(next AggFuncExec, groupIdx1 int, groupIdx2 int) error {
	other := next.(*clusterCentersExec)
	if other.groupData[groupIdx2] == nil || other.groupData[groupIdx2].Length() == 0 {
		return nil
	}

	if exec.groupData[groupIdx1] == nil || exec.groupData[groupIdx1].Length() == 0 {
		exec.groupData[groupIdx1] = other.groupData[groupIdx2]
		other.groupData[groupIdx2] = nil
		return nil
	}

	bts := vector.MustBytesCol(other.groupData[groupIdx2])
	if err := vector.AppendBytesList(exec.groupData[groupIdx1], bts, nil, exec.ret.mp); err != nil {
		return err
	}
	other.groupData[groupIdx2] = nil
	return nil
}

func (exec *clusterCentersExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	for i, group := range groups {
		if group != GroupNotMatched {
			if err := exec.Merge(next, int(group)-1, i+offset); err != nil {
				return err
			}
		}
	}
	return nil
}

func (exec *clusterCentersExec) Flush() (*vector.Vector, error) {
	switch exec.singleAggInfo.argType.Oid {
	case types.T_array_float32:
		if err := exec.flushArray32(); err != nil {
			return nil, err
		}
	case types.T_array_float64:
		if err := exec.flushArray64(); err != nil {
			return nil, err
		}
	default:
		return nil, moerr.NewInternalErrorNoCtx(
			"unsupported type '%s' for cluster_centers", exec.singleAggInfo.argType.String())
	}

	return exec.ret.flush(), nil
}

func (exec *clusterCentersExec) flushArray32() error {
	for i, group := range exec.groupData {
		exec.ret.groupToSet = i

		if group == nil || group.Length() == 0 {
			continue
		}

		bts := vector.MustBytesCol(group)
		// todo: it's bad here this f64s is out of the memory control.
		f64s := make([][]float64, group.Length())
		for i := range f64s {
			f32s := types.BytesToArray[float32](bts[i])
			f64s[i] = make([]float64, len(f32s))
			for j := range f32s {
				f64s[i][j] = float64(f32s[j])
			}
		}

		centers, err := exec.getCentersByKmeansAlgorithm(f64s)
		if err != nil {
			return err
		}
		res, err := exec.arraysToString(centers)
		if err != nil {
			return err
		}
		if err = exec.ret.aggSet(util.UnsafeStringToBytes(res)); err != nil {
			return err
		}
	}
	return nil
}

func (exec *clusterCentersExec) flushArray64() error {
	for i, group := range exec.groupData {
		exec.ret.groupToSet = i

		if group == nil || group.Length() == 0 {
			continue
		}

		bts := vector.MustBytesCol(group)
		f64s := make([][]float64, group.Length())
		for i := range f64s {
			f64s[i] = types.BytesToArray[float64](bts[i])
		}

		centers, err := exec.getCentersByKmeansAlgorithm(f64s)
		if err != nil {
			return err
		}
		res, err := exec.arraysToString(centers)
		if err != nil {
			return err
		}
		if err = exec.ret.aggSet(util.UnsafeStringToBytes(res)); err != nil {
			return err
		}
	}
	return nil
}

func (exec *clusterCentersExec) getCentersByKmeansAlgorithm(f64s [][]float64) ([][]float64, error) {
	var clusterer kmeans.Clusterer
	var centers [][]float64
	var err error

	if clusterer, err = elkans.NewKMeans(
		f64s, int(exec.clusterCnt),
		defaultKmeansMaxIteration,
		defaultKmeansDeltaThreshold,
		exec.distType,
		exec.initType,
		exec.normalize); err != nil {
		return nil, err
	}
	if centers, err = clusterer.Cluster(); err != nil {
		return nil, err
	}

	return centers, nil
}

// converts [][]float64 to json string.
func (exec *clusterCentersExec) arraysToString(centers [][]float64) (res string, err error) {
	switch exec.singleAggInfo.argType.Oid {
	case types.T_array_float32:
		// cast [][]float64 to [][]float32
		_centers := make([][]float32, len(centers))
		for i, center := range centers {
			_centers[i], err = moarray.Cast[float64, float32](center)
			if err != nil {
				return "", err
			}
		}

		// comments that copied from old code.
		// create json string for [][]float32
		// NOTE: here we can't use jsonMarshall as it does not accept precision as done in ArraysToString
		// We need precision here, as it is the final output that will be printed on SQL console.
		res = fmt.Sprintf("[ %s ]", types.ArraysToString[float32](_centers, ","))

	case types.T_array_float64:
		res = fmt.Sprintf("[ %s ]", types.ArraysToString[float64](centers, ","))
	}
	return res, nil
}

func (exec *clusterCentersExec) Free() {
	exec.ret.free()
	for _, v := range exec.groupData {
		if v == nil {
			continue
		}

		if v.NeedDup() {
			v.Free(exec.ret.mp)
		} else {
			exec.ret.mg.PutVector(v)
		}
	}
}

func (exec *clusterCentersExec) SetExtraInformation(partialResult any, groupIndex int) {
	if bts, ok := partialResult.([]byte); ok {
		k, distType, initType, normalize, err := decodeConfig(bts)
		if err != nil {
			exec.clusterCnt = k
			exec.distType = distType
			exec.initType = initType
			exec.normalize = normalize
		}
	}
}

// that's very bad codes here, because we cannot know how to encode this config.
// support an encode method here is a better way.
func decodeConfig(extra []byte) (
	clusterCount uint64, distType kmeans.DistanceType, initType kmeans.InitType, normalize bool, err error) {
	// decode methods.
	parseClusterCount := func(s string) (uint64, error) {
		return strconv.ParseUint(strings.TrimSpace(s), 10, 64)
	}
	parseDistType := func(s string) (kmeans.DistanceType, error) {
		v := strings.ToLower(s)
		if res, ok := distTypeStrToEnum[v]; ok {
			return res, nil
		}
		return 0, moerr.NewInternalErrorNoCtx("unsupported distance_type '%s' for cluster_centers", v)
	}
	parseInitType := func(s string) (kmeans.InitType, error) {
		if res, ok := initTypeStrToEnum[s]; ok {
			return res, nil
		}
		return 0, moerr.NewInternalErrorNoCtx("unsupported init_type '%s' for cluster_centers", s)
	}

	if len(extra) == 0 {
		return defaultKmeansClusterCnt, defaultKmeansDistanceType, defaultKmeansInitType, defaultKmeansNormalize, nil
	}

	configs := strings.Split(string(extra), configSeparator)
	for i := range configs {
		configs[i] = strings.TrimSpace(configs[i])

		switch i {
		case 0:
			clusterCount, err = parseClusterCount(configs[i])
		case 1:
			distType, err = parseDistType(configs[i])
		case 2:
			initType, err = parseInitType(configs[i])
		case 3:
			normalize, err = strconv.ParseBool(configs[i])
		}
		if err != nil {
			return
		}
	}
	return
}
