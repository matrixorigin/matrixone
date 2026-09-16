// Copyright 2026 Matrix Origin
// Copyright 2020 PingCAP, Inc.
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
//
// The utf8mb4_general_ci weight table below is adapted from TiDB commit
// 6cbbd222c786948379edc50ef8a8c37e485957c0, pkg/util/collate/general_ci.go.

package aggexec

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/collation"
)

// compareUTF8mb4GeneralCI retains the existing aggregate comparison contract.
// The shared mapping has one weight per character, with no expansions. This
// legacy comparator's trimmed-suffix ordering is intentionally unchanged here;
// unlike the new PAD-aware key codec, it orders an exhausted input before a
// remaining control character. See issue-28164-weight-key-v1.md for the native
// MySQL discrepancy that must be resolved before weight-index activation.
func compareUTF8mb4GeneralCI(a, b []byte) int {
	// utf8mb4_general_ci is a PAD SPACE collation.
	a = bytes.TrimRight(a, " ")
	b = bytes.TrimRight(b, " ")

	ai, bi := 0, 0
	for ai < len(a) && bi < len(b) {
		wa, sizeA := collation.NextGeneralCIWeight(a[ai:])
		wb, sizeB := collation.NextGeneralCIWeight(b[bi:])
		if wa < wb {
			return -1
		}
		if wa > wb {
			return 1
		}
		ai += sizeA
		bi += sizeB
	}
	if ai < len(a) {
		return 1
	}
	if bi < len(b) {
		return -1
	}
	return 0
}
