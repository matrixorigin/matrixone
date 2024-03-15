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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionAgg/algos/kmeans"
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
	distTypeStrToEnum = map[string]kmeans.DistanceType{
		"vector_l2_ops": kmeans.L2Distance,
	}

	initTypeStrToEnum = map[string]kmeans.InitType{
		"random":         kmeans.Random,
		"kmeansplusplus": kmeans.KmeansPlusPlus,
	}
)

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

func (exec *clusterCentersExec) GroupGrow(more int) error {
	if err := exec.ret.grows(more); err != nil {
		return err
	}
	exec.groupData = append(exec.groupData, exec.ret.mg.GetVector(types.T_float64.ToType()))
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
			if null {
				continue
			}
			if err := vector.AppendBytes(
				exec.groupData[groups[idx]-1],
				v, false, exec.ret.mp); err != nil {
				return err
			}
		}
		idx++
	}
	return nil
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
