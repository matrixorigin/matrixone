// Copyright 2026 Matrix Origin
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

// EmptyResultKind describes the value produced by an aggregate group that has
// no input rows. Keep this contract with the aggregate executors so planner
// rewrites do not maintain a separate function-name whitelist.
type EmptyResultKind uint8

const (
	EmptyResultUnsupported EmptyResultKind = iota
	EmptyResultNull
	EmptyResultZero
	EmptyResultAllBitsSet
)

// GetEmptyResultKind returns the aggregate executor's empty-input contract.
// Unsupported means the executor has a non-scalar or otherwise non-trivial
// empty value that a planner rewrite must not synthesize.
func GetEmptyResultKind(aggID int64) EmptyResultKind {
	switch aggID {
	case AggIdOfCountColumn, AggIdOfCountStar, AggIdOfApproxCount, AggIdOfApproxCountDistinct:
		return EmptyResultZero
	case AggIdOfBitAnd:
		return EmptyResultAllBitsSet
	case AggIdOfBitOr, AggIdOfBitXor:
		return EmptyResultZero
	case AggIdOfAny, AggIdOfAvg, AggIdOfGroupConcat, AggIdOfJsonArrayAgg,
		AggIdOfJsonObjectAgg, AggIdOfMax, AggIdOfMaxBy, AggIdOfMaxByNonNull,
		AggIdOfMedian, AggIdOfMin, AggIdOfApproxPercentile, AggIdOfStdDevPop,
		AggIdOfStdDevSample, AggIdOfSum, AggIdOfVarPop, AggIdOfVarSample:
		return EmptyResultNull
	case AggIdOfAvgTwCache, AggIdOfAvgTwResult, AggIdOfBitmapConstruct,
		AggIdOfBitmapOr, AggIdOfHllAdd, AggIdOfHllMerge:
		return EmptyResultUnsupported
	default:
		return EmptyResultUnsupported
	}
}
