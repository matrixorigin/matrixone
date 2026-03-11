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

func RegisterJsonArrayAgg(id int64) {
	aggexec.RegisterJsonArrayAgg(id)
}

func RegisterJsonObjectAgg(id int64) {
	aggexec.RegisterJsonObjectAgg(id)
}

func RegisterSum(id int64) {
	aggexec.RegisterSum(id)
}

func RegisterAvg(id int64) {
	aggexec.RegisterAvg(id)
}

func RegisterMin(id int64) {
	aggexec.RegisterMin(id)
}

func RegisterMax(id int64) {
	aggexec.RegisterMax(id)
}

func RegisterAny(id int64) {
	aggexec.RegisterAny(id)
}

func RegisterVarPop(id int64) {
	aggexec.RegisterVarPop(id)
}

func RegisterStdDevPop(id int64) {
	aggexec.RegisterStdDevPop(id)
}

func RegisterVarSample(id int64) {
	aggexec.RegisterVarSample(id)
}

func RegisterStdDevSample(id int64) {
	aggexec.RegisterStdDevSample(id)
}

func RegisterBitXor(id int64) {
	aggexec.RegisterBitXorAgg(id)
}

func RegisterBitAnd(id int64) {
	aggexec.RegisterBitAndAgg(id)
}

func RegisterBitOr(id int64) {
	aggexec.RegisterBitOrAgg(id)
}

func RegisterBitmapConstruct(id int64) {
	aggexec.RegisterBitmapConstruct(id)
}

func RegisterBitmapOr(id int64) {
	aggexec.RegisterBitmapOr(id)
}
