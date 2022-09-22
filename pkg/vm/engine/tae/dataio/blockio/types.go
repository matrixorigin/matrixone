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

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type Extension int16

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
