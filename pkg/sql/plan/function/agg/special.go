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

package agg

import "github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"

func RegisterCountColumn(id int64) {
	aggexec.RegisterCountColumnAgg(id)
}

func RegisterCountStar(id int64) {
	aggexec.RegisterCountStarAgg(id)
}

func RegisterGroupConcat(id int64) {
	aggexec.RegisterGroupConcatAgg(id, ",")
}

func RegisterApproxCount(id int64) {
	aggexec.RegisterApproxCountAgg(id)
}

func RegisterMedian(id int64) {
	aggexec.RegisterMedian(id)
}

func RegisterClusterCenters(id int64) {
	aggexec.RegisterClusterCenters(id)
}
