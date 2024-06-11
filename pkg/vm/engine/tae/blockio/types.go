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

package blockio

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

const (
	JTLoad tasks.JobType = 200 + iota
	JTFlush
)

func init() {
	tasks.RegisterJobType(JTLoad, "LoadJob")
	tasks.RegisterJobType(JTFlush, "FlushJob")
}

const (
	CheckpointExt = "ckp"
	GCFullExt     = "fgc"
	SnapshotExt   = "snap"
	AcctExt       = "acct"
)

func EncodeCheckpointMetadataFileName(dir, prefix string, start, end types.TS) string {
	return fmt.Sprintf("%s/%s_%s_%s.%s", dir, prefix, start.ToString(), end.ToString(), CheckpointExt)
}

func EncodeCheckpointMetadataFileNameWithoutDir(prefix string, start, end types.TS) string {
	return fmt.Sprintf("%s_%s_%s.%s", prefix, start.ToString(), end.ToString(), CheckpointExt)
}

func EncodeSnapshotMetadataFileName(dir, prefix string, start, end types.TS) string {
	return fmt.Sprintf("%s/%s_%s_%s.%s", dir, prefix, start.ToString(), end.ToString(), SnapshotExt)
}

func EncodeTableMetadataFileName(dir, prefix string, start, end types.TS) string {
	return fmt.Sprintf("%s/%s_%s_%s.%s", dir, prefix, start.ToString(), end.ToString(), AcctExt)
}

func EncodeGCMetadataFileName(dir, prefix string, start, end types.TS) string {
	return fmt.Sprintf("%s/%s_%s_%s.%s", dir, prefix, start.ToString(), end.ToString(), GCFullExt)
}

func DecodeCheckpointMetadataFileName(name string) (start, end types.TS) {
	fileName := strings.Split(name, ".")
	info := strings.Split(fileName[0], "_")
	start = types.StringToTS(info[1])
	end = types.StringToTS(info[2])
	return
}

func DecodeGCMetadataFileName(name string) (start, end types.TS, ext string) {
	fileName := strings.Split(name, ".")
	info := strings.Split(fileName[0], "_")
	start = types.StringToTS(info[1])
	end = types.StringToTS(info[2])
	ext = fileName[1]
	return
}

func GetObjectSizeWithBlocks(blocks []objectio.BlockObject) (uint32, error) {
	objectSize := uint32(0)
	for _, block := range blocks {
		meta := block.GetMeta()
		count := meta.BlockHeader().MetaColumnCount()
		for i := 0; i < int(count); i++ {
			col := block.MustGetColumn(uint16(i))
			objectSize += col.Location().Length()
		}
	}
	return objectSize, nil
}

// EncodeLocationFromString Generate a metaloc from an info string
func EncodeLocationFromString(info string) (objectio.Location, error) {
	location := strings.Split(info, "_")
	if len(location) < 8 {
		panic(fmt.Sprintf("info: %v", info))
	}
	num, err := strconv.ParseUint(location[1], 10, 32)
	if err != nil {
		return nil, err
	}
	alg, err := strconv.ParseUint(location[2], 10, 32)
	if err != nil {
		return nil, err
	}
	offset, err := strconv.ParseUint(location[3], 10, 32)
	if err != nil {
		return nil, err
	}
	size, err := strconv.ParseUint(location[4], 10, 32)
	if err != nil {
		return nil, err
	}
	osize, err := strconv.ParseUint(location[5], 10, 32)
	if err != nil {
		return nil, err
	}
	rows, err := strconv.ParseUint(location[6], 10, 32)
	if err != nil {
		return nil, err
	}
	id, err := strconv.ParseUint(location[7], 10, 32)
	if err != nil {
		return nil, err
	}
	extent := objectio.NewExtent(uint8(alg), uint32(offset), uint32(size), uint32(osize))
	uid, err := types.ParseUuid(location[0])
	if err != nil {
		return nil, err
	}
	name := objectio.BuildObjectName(&uid, uint16(num))
	return objectio.BuildLocation(name, extent, uint32(rows), uint16(id)), nil
}

// EncodeLocation Generate a metaloc
func EncodeLocation(
	name objectio.ObjectName,
	extent objectio.Extent,
	rows uint32,
	id uint16) objectio.Location {
	return objectio.BuildLocation(name, extent, rows, id)
}
