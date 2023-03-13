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

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

const (
	CheckpointExt = "ckp"
	GCFullExt     = "fgc"
)

func EncodeCheckpointMetadataFileName(dir, prefix string, start, end types.TS) string {
	return fmt.Sprintf("%s/%s_%s_%s.%s", dir, prefix, start.ToString(), end.ToString(), CheckpointExt)
}

func EncodeGCMetadataFileName(dir, prefix string, start, end types.TS) string {
	return fmt.Sprintf("%s/%s_%s_%s.%s", dir, prefix, start.ToString(), end.ToString(), GCFullExt)
}

// EncodeObjectName Generate uuid as the file name of the block&segment
func EncodeObjectName() (name string) {
	name = uuid.NewString()
	return name
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
		header := meta.GetHeader()
		count := header.GetColumnCount()
		for i := 0; i < int(count); i++ {
			col, err := block.GetColumn(uint16(i))
			if err != nil {
				return 0, err
			}
			objectSize += col.GetMeta().GetLocation().Length()
		}
	}
	return objectSize, nil
}

// EncodeLocation Generate a metaloc from an object file
func EncodeLocation(
	extent objectio.Extent,
	rows uint32,
	blocks []objectio.BlockObject) (string, error) {
	size, err := GetObjectSizeWithBlocks(blocks)
	if err != nil {
		return "", err
	}
	meta := blocks[0].GetMeta()
	metaLen := blocks[0].GetExtent().Length()
	name := meta.GetName()
	metaLoc := fmt.Sprintf("%s:%d_%d_%d_%d:%d:%d",
		name,
		blocks[0].GetExtent().Offset(),
		metaLen,
		metaLen,
		extent.Id(),
		rows,
		size,
	)
	return metaLoc, nil
}

func DecodeLocation(metaLoc string) (name string, id uint32, extent objectio.Extent, rows uint32, err error) {
	info := strings.Split(metaLoc, ":")
	name = info[0]
	location := strings.Split(info[1], "_")
	offset, err := strconv.ParseUint(location[0], 10, 32)
	if err != nil {
		return
	}
	size, err := strconv.ParseUint(location[1], 10, 32)
	if err != nil {
		return
	}
	osize, err := strconv.ParseUint(location[2], 10, 32)
	if err != nil {
		return
	}
	num, err := strconv.ParseUint(location[3], 10, 32)
	if err != nil {
		return
	}
	id = uint32(num)
	r, err := strconv.ParseUint(info[2], 10, 32)
	if err != nil {
		return
	}
	rows = uint32(r)
	extent = objectio.NewExtent(uint32(id), uint32(offset), uint32(size), uint32(osize))
	return
}
