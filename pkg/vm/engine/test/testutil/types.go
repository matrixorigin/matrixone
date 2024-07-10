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

package testutil

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

type PObjectStats struct {
	ObjCnt int
	BlkCnt int
	RowCnt int
}

type PInmemRowsStats struct {
	VisibleCnt   int
	InvisibleCnt int
}

type PartitionStateStats struct {
	DataObjectsVisible   PObjectStats
	DataObjectsInvisible PObjectStats
	InmemRows            PInmemRowsStats
	CheckpointCnt        int
}

func (s *PartitionStateStats) String() string {
	return fmt.Sprintf("dataObjects:{iobj-%d, iblk-%d, irow-%d; dobj-%d, dblk-%d, drow-%d};\n"+
		"InmemRows:{visible-%d, invisible-%d};\ncheckpoint:{%d}",
		s.DataObjectsVisible.ObjCnt, s.DataObjectsVisible.BlkCnt, s.DataObjectsVisible.RowCnt,
		s.DataObjectsInvisible.ObjCnt, s.DataObjectsInvisible.BlkCnt, s.DataObjectsInvisible.RowCnt,
		s.InmemRows.VisibleCnt, s.InmemRows.InvisibleCnt,
		s.CheckpointCnt)
}

type TestOptions struct {
	TaeEngineOptions *options.Options
}
