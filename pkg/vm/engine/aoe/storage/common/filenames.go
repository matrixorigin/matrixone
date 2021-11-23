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

package common

import (
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
)

type FileT int

const (
	FTLock FileT = iota
	FTTBlock
	FTBlock
	FTSegment
	FTTransientNode
)

const (
	TmpSuffix  = ".tmp"
	TBlkSuffix = ".tblk"
	BlkSuffix  = ".blk"
	SegSuffix  = ".seg"
	LockSuffix = ".lock"
	NodeSuffix = ".nod"
	BSISuffix = ".bsi"

	SpillDirName = "spill"
	TempDirName  = "temp"
	DataDirName  = "data"
	MetaDirName  = "meta"
)

func MakeSpillDir(dirname string) string {
	return path.Join(dirname, SpillDirName)
}

func MakeTempDir(dirname string) string {
	return path.Join(dirname, TempDirName)
}

func MakeDataDir(dirname string) string {
	return path.Join(dirname, DataDirName)
}

func MakeMetaDir(dirname string) string {
	return path.Join(dirname, MetaDirName)
}

func MakeTBlockFileName(dirname, name string, isTmp bool) string {
	return MakeFilename(dirname, FTTBlock, name, isTmp)
}

func MakeBlockFileName(dirname, name string, tableId uint64, isTmp bool) string {
	dir := dirname
	return MakeFilename(dir, FTBlock, name, isTmp)
}

func MakeSegmentFileName(dirname, name string, tableId uint64, isTmp bool) string {
	dir := dirname
	return MakeFilename(dir, FTSegment, name, isTmp)
}

func MakeLockFileName(dirname, name string) string {
	return MakeFilename(dirname, FTLock, name, false)
}

func MakeBitSlicedIndexFileName(version, tid, sid uint64, col uint16) string {
	return fmt.Sprintf("%d_%d_%d_%d.bsi", version, tid, sid, col)
}

func ParseSegmentFileName(filename string) (name string, ok bool) {
	name = strings.TrimSuffix(filename, SegSuffix)
	if len(name) == len(filename) {
		return name, false
	}
	return name, true
}

func ParseTBlockfileName(filename string) (name string, ok bool) {
	name = strings.TrimSuffix(filename, TBlkSuffix)
	if len(name) == len(filename) {
		return name, false
	}
	return name, true
}

func ParseBlockfileName(filename string) (name string, ok bool) {
	name = strings.TrimSuffix(filename, BlkSuffix)
	if len(name) == len(filename) {
		return name, false
	}
	return name, true
}

func ParseBitSlicedIndexFileName(filename string) (name string, ok bool) {
	name = strings.TrimSuffix(filename, BSISuffix)
	if len(name) == len(filename) {
		return name, false
	}
	return name, true
}

func ParseBitSlicedIndexFileNameToInfo(filename string) (version, tblId, segId uint64, colIdx uint16, ok bool) {
	filename = strings.Trim(filename, BSISuffix)
	infos := strings.Split(filename, "_")
	arr := make([]int, 4)
	var err error
	arr[0], err = strconv.Atoi(infos[0])
	if err != nil {
		return 0, 0, 0, 0, false
	}
	arr[1], err = strconv.Atoi(infos[1])
	if err != nil {
		return 0, 0, 0, 0, false
	}
	arr[2], err = strconv.Atoi(infos[2])
	if err != nil {
		return 0, 0, 0, 0, false
	}
	arr[3], err = strconv.Atoi(infos[3])
	if err != nil {
		return 0, 0, 0, 0, false
	}
	return uint64(arr[0]), uint64(arr[1]), uint64(arr[2]), uint16(arr[3]), true
}

func MakeFilename(dirname string, ft FileT, name string, isTmp bool) string {
	var s string
	switch ft {
	case FTLock:
		s = path.Join(dirname, fmt.Sprintf("%s%s", name, LockSuffix))
	case FTTransientNode:
		s = path.Join(MakeSpillDir(dirname), fmt.Sprintf("%s%s", name, NodeSuffix))
		isTmp = false
	case FTTBlock:
		s = path.Join(MakeDataDir(dirname), fmt.Sprintf("%s%s", name, TBlkSuffix))
	case FTBlock:
		s = path.Join(MakeDataDir(dirname), fmt.Sprintf("%s%s", name, BlkSuffix))
	case FTSegment:
		s = path.Join(MakeDataDir(dirname), fmt.Sprintf("%s%s", name, SegSuffix))
	default:
		panic(fmt.Sprintf("unsupported %d", ft))
	}
	if isTmp {
		s += TmpSuffix
	}
	return s
}

func IsTempFile(name string) bool {
	return strings.HasSuffix(name, TmpSuffix)
}

func IsTBlockFile(name string) bool {
	return strings.HasSuffix(name, TBlkSuffix)
}

func IsBlockFile(name string) bool {
	return strings.HasSuffix(name, BlkSuffix)
}

func IsSegmentFile(name string) bool {
	return strings.HasSuffix(name, SegSuffix)
}

func FilenameFromTmpfile(tmpFile string) (fname string, err error) {
	fname = strings.TrimSuffix(tmpFile, TmpSuffix)
	if len(fname) == len(tmpFile) {
		return "", errors.New(fmt.Sprintf("Cannot extract filename from temp file %s", tmpFile))
	}
	return fname, nil
}
