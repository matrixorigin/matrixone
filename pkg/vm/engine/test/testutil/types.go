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
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"path/filepath"
)

type PObjectStats struct {
	ObjCnt int
	BlkCnt int
	RowCnt int
}

type PInmemRowsStats struct {
	VisibleCnt                int
	InvisibleCnt              int
	VisibleDistinctBlockCnt   int
	InvisibleDistinctBlockCnt int
}

type PartitionStateStats struct {
	DataObjectsVisible   PObjectStats
	DataObjectsInvisible PObjectStats

	TombstoneObjectsVisible   PObjectStats
	TombstoneObjectsInvisible PObjectStats

	InmemRows     PInmemRowsStats
	CheckpointCnt int

	Details struct {
		// 0: locations
		// 1: versions
		DeletedRows    []*batch.Batch
		CheckpointLocs [2][]string
		DataObjectList struct {
			Visible, Invisible []logtailreplay.ObjectEntry
		}
		TombstoneObjectList struct {
			Visible, Invisible []logtailreplay.ObjectEntry
		}
	}
}

func (s *PartitionStateStats) Summary() PartitionStateStats {
	return PartitionStateStats{
		DataObjectsVisible:   s.DataObjectsVisible,
		DataObjectsInvisible: s.DataObjectsInvisible,
		InmemRows:            s.InmemRows,
		CheckpointCnt:        s.CheckpointCnt,
	}
}

func (s *PartitionStateStats) String() string {
	return fmt.Sprintf("dataObjects:{iobj-%d, iblk-%d, irow-%d; dobj-%d, dblk-%d, drow-%d};\n"+
		"InmemRows:{visible-%d, invisible-%d};\ncheckpoint:{%d};\n"+
		"visible objects:   %v\n"+
		"invisible objects: %v\n"+
		"visible tombstone: %v\n"+
		"invisible tombstone objects: %v\n",
		s.DataObjectsVisible.ObjCnt, s.DataObjectsVisible.BlkCnt, s.DataObjectsVisible.RowCnt,
		s.DataObjectsInvisible.ObjCnt, s.DataObjectsInvisible.BlkCnt, s.DataObjectsInvisible.RowCnt,
		s.InmemRows.VisibleCnt, s.InmemRows.InvisibleCnt,
		s.CheckpointCnt, s.Details.DataObjectList.Visible, s.Details.DataObjectList.Invisible,
		s.TombstoneObjectsVisible, s.TombstoneObjectsInvisible)

}

type TestOptions struct {
	TaeEngineOptions *options.Options
	Timeout          time.Duration
}

func GetS3SharedFileServiceOption(ctx context.Context, dir string) (*options.Options, error) {
	config := fileservice.Config{
		Name:    defines.SharedFileServiceName,
		Backend: "DISK",
		DataDir: filepath.Join(dir, "share"),
	}

	fs, err := fileservice.NewFileService(
		ctx,
		config,
		[]*perfcounter.CounterSet{},
	)

	if err != nil {
		return nil, err
	}

	return &options.Options{
		Fs: fs,
	}, nil
}
