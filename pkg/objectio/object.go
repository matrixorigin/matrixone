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

package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

const Magic = 0xFFFFFFFF
const Version = 1
const FSName = "local"

type Object struct {
	// name is the object file's name
	name string
	// fs is an instance of fileservice
	fs fileservice.FileService
}

func NewObject(name string, fs fileservice.FileService) *Object {
	object := &Object{
		name: name,
		fs:   fs,
	}
	return object
}

func (o *Object) GetFs() fileservice.FileService {
	return o.fs
}

type ObjectDescriber interface {
	DescribeObject() (*ObjectStats, error)
}

type ObjectStats struct {
	// 0: Data
	// 1: Tombstone
	zoneMaps   [2]ZoneMap
	blkCnt     int
	extent     Extent
	name       ObjectName
	sortKeyIdx int
}

func newObjectStats() *ObjectStats {
	description := new(ObjectStats)
	return description
}

func (des *ObjectStats) GetSortKeyIdx() int {
	return des.sortKeyIdx
}

func (des *ObjectStats) GetOriginSize() uint32 {
	return des.extent.OriginSize()
}

func (des *ObjectStats) GetCompSize() uint32 {
	return des.extent.Length()
}

func (des *ObjectStats) GetObjLoc() Location {
	return BuildLocation(des.name, des.extent, 0, 0)
}

func (des *ObjectStats) GetBlkCnt() int {
	return des.blkCnt
}

func (des *ObjectStats) GetDataSortKeyZoneMap() ZoneMap {
	return des.zoneMaps[SchemaData]
}

func (des *ObjectStats) GetTombstoneZoneSortKeyMap() ZoneMap {
	return des.zoneMaps[SchemaTombstone]
}

func (des *ObjectStats) GetZoneMapsBySeqNum(seqNum uint16) ZoneMap {
	if len(des.zoneMaps) <= int(seqNum) {
		return nil
	}
	return des.zoneMaps[seqNum]
}
