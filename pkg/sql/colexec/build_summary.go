// Copyright 2026 Matrix Origin
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

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

const BuildSummaryShuffleIDX int32 = -1

const (
	buildSummaryNonEmpty byte = 1 << iota
	buildSummaryHasNull
	buildSummaryValidMask = buildSummaryNonEmpty | buildSummaryHasNull
)

// NewBuildSummaryBatch returns a zero-vector control batch. Its row count is
// one so ordinary pipeline operators do not discard it as an empty batch.
func NewBuildSummaryBatch(nonEmpty, hasNull bool) *batch.Batch {
	flags := byte(0)
	if nonEmpty {
		flags |= buildSummaryNonEmpty
	}
	if hasNull {
		flags |= buildSummaryHasNull
	}
	bat := batch.NewWithSize(0)
	bat.ShuffleIDX = BuildSummaryShuffleIDX
	bat.ExtraBuf = []byte{flags}
	bat.SetRowCount(1)
	return bat
}

func IsBuildSummaryBatch(bat *batch.Batch) bool {
	return bat != nil && bat.ShuffleIDX == BuildSummaryShuffleIDX
}

func DecodeBuildSummaryBatch(bat *batch.Batch) (nonEmpty, hasNull bool, err error) {
	if !IsBuildSummaryBatch(bat) || len(bat.Vecs) != 0 || bat.RowCount() != 1 || len(bat.ExtraBuf) != 1 || bat.ExtraBuf[0]&^buildSummaryValidMask != 0 {
		return false, false, moerr.NewInternalErrorNoCtx("malformed shuffle build summary")
	}
	flags := bat.ExtraBuf[0]
	return flags&buildSummaryNonEmpty != 0, flags&buildSummaryHasNull != 0, nil
}
