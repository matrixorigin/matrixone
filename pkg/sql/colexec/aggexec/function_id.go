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

// Aggregate executors receive the encoded function overload ID. All aggregate
// and window functions below have overload zero, so these constants match the
// IDs encoded by the function catalog without requiring init-time registration.
const (
	AggIdOfAny                 int64 = 44 << 32
	AggIdOfApproxCount         int64 = 45 << 32
	AggIdOfAvg                 int64 = 57 << 32
	AggIdOfAvgTwCache          int64 = 58 << 32
	AggIdOfAvgTwResult         int64 = 59 << 32
	AggIdOfBitAnd              int64 = 62 << 32
	AggIdOfBitOr               int64 = 65 << 32
	AggIdOfBitXor              int64 = 66 << 32
	AggIdOfCountColumn         int64 = 82 << 32
	WinIdOfCumeDist            int64 = 87 << 32
	WinIdOfDenseRank           int64 = 96 << 32
	WinIdOfFirstValue          int64 = 102 << 32
	WinIdOfLag                 int64 = 119 << 32
	WinIdOfLastValue           int64 = 120 << 32
	WinIdOfLead                int64 = 121 << 32
	AggIdOfMax                 int64 = 137 << 32
	AggIdOfMedian              int64 = 138 << 32
	AggIdOfMin                 int64 = 139 << 32
	WinIdOfNthValue            int64 = 143 << 32
	WinIdOfNtile               int64 = 144 << 32
	WinIdOfPercentRank         int64 = 146 << 32
	WinIdOfRank                int64 = 155 << 32
	WinIdOfRowNumber           int64 = 168 << 32
	AggIdOfCountStar           int64 = 178 << 32
	AggIdOfStdDevPop           int64 = 180 << 32
	AggIdOfStdDevSample        int64 = 181 << 32
	AggIdOfSum                 int64 = 183 << 32
	AggIdOfGroupConcat         int64 = 185 << 32
	AggIdOfVarPop              int64 = 199 << 32
	AggIdOfVarSample           int64 = 200 << 32
	AggIdOfApproxCountDistinct int64 = 226 << 32
	AggIdOfBitmapConstruct     int64 = 333 << 32
	AggIdOfBitmapOr            int64 = 334 << 32
	AggIdOfJsonArrayAgg        int64 = 400 << 32
	AggIdOfJsonObjectAgg       int64 = 401 << 32
	AggIdOfHllAdd              int64 = 453 << 32
	AggIdOfHllMerge            int64 = 454 << 32
	AggIdOfApproxPercentile    int64 = 549 << 32
)
