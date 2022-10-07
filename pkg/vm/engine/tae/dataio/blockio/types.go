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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

const (
	BlockExt   = "blk"
	SegmentExt = "seg"
)

func EncodeBlkName(id *common.ID) (name string) {
	basename := fmt.Sprintf("%d-%d-%d.%s", id.TableID, id.SegmentID, id.BlockID, BlockExt)
	return basename
}

func EncodeSegName(id *common.ID) (name string) {
	basename := fmt.Sprintf("%d-%d.%s", id.TableID, id.SegmentID, SegmentExt)
	return basename
}

func DecodeName(name string) []string {
	fileName := strings.Split(name, ".")
	info := strings.Split(fileName[0], "-")
	return info
}

func DecodeBlkName(name string) (id *common.ID, err error) {
	info := DecodeName(name)
	tid, err := strconv.ParseUint(info[0], 10, 32)
	if err != nil {
		return
	}
	sid, err := strconv.ParseUint(info[1], 10, 32)
	if err != nil {
		return
	}
	bid, err := strconv.ParseUint(info[2], 10, 32)
	if err != nil {
		return
	}
	id = &common.ID{
		TableID:   tid,
		SegmentID: sid,
		BlockID:   bid,
	}
	return
}

func DecodeSegName(name string) (id *common.ID, err error) {
	info := DecodeName(name)
	tid, err := strconv.ParseUint(info[0], 10, 32)
	if err != nil {
		return
	}
	sid, err := strconv.ParseUint(info[1], 10, 32)
	if err != nil {
		return
	}
	id = &common.ID{
		TableID:   tid,
		SegmentID: sid,
	}
	return
}

func EncodeBlkMetaLoc(id *common.ID, extent objectio.Extent, rows uint32) string {
	metaLoc := fmt.Sprintf("%s:%d_%d_%d:%d",
		EncodeBlkName(id),
		extent.Offset(),
		extent.Length(),
		extent.OriginSize(),
		rows,
	)
	return metaLoc
}

func EncodeSegMetaLoc(id *common.ID, extent objectio.Extent, rows uint32) string {
	metaLoc := fmt.Sprintf("%s:%d_%d_%d:%d",
		EncodeSegName(id),
		extent.Offset(),
		extent.Length(),
		extent.OriginSize(),
		rows,
	)
	return metaLoc
}

func EncodeBlkDeltaLoc(id *common.ID, extent objectio.Extent) string {
	deltaLoc := fmt.Sprintf("%s:%d_%d_%d",
		EncodeBlkName(id),
		extent.Offset(),
		extent.Length(),
		extent.OriginSize(),
	)
	return deltaLoc
}

func DecodeMetaLoc(metaLoc string) (string, objectio.Extent, uint32) {
	info := strings.Split(metaLoc, ":")
	name := info[0]
	location := strings.Split(info[1], "_")
	offset, err := strconv.ParseUint(location[0], 10, 32)
	if err != nil {
		panic(any(err))
	}
	size, err := strconv.ParseUint(location[1], 10, 32)
	if err != nil {
		panic(any(err))
	}
	osize, err := strconv.ParseUint(location[2], 10, 32)
	if err != nil {
		panic(any(err))
	}
	rows, err := strconv.ParseUint(info[2], 10, 32)
	if err != nil {
		panic(any(err))
	}
	extent := objectio.NewExtent(uint32(offset), uint32(size), uint32(osize))
	return name, extent, uint32(rows)
}
