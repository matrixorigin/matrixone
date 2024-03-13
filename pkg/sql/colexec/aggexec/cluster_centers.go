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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionAgg/algos/kmeans"
)

type clusterCentersExec struct {
	singleAggInfo
	singleAggOptimizedInfo
	ret aggFuncBytesResult

	// groupData hold the input data for each group.
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
	exec.groupData = append(exec.groupData, exec.ret.mg.GetVector(exec.argType))
	return nil
}

func (exec *clusterCentersExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	// todo: 晚点再搞
	return nil
}
