// Copyright 2021 Matrix Origin
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

package colexec

// TopRowsToFill bounds the rows accepted from a batch by the remaining top limit.
func TopRowsToFill(limit uint64, currentRows int, batchRows int) int {
	if uint64(currentRows) >= limit {
		return 0
	}
	remaining := limit - uint64(currentRows)
	if remaining >= uint64(batchRows) {
		return batchRows
	}
	return int(remaining)
}
