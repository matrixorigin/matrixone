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
	FTInfoCkp FileT = iota
	FTTableCkp
	FTLock
	FTTBlock
	FTBlock
	FTSegment
	FTTransientNode
)

func MakeSpillDir(dirname string) string {
	return path.Join(dirname, "spill")
}

func MakeDataDir(dirname string) string {
	return path.Join(dirname, "data")
}

func MakeMetaDir(dirname string) string {
	return path.Join(dirname, "meta")
}

//func MakeTableDir(dirname string, id uint64) string {
//	return path.Join(dirname, fmt.Sprintf("%d", id))
//}

func MakeTBlockFileName(dirname, name string, isTmp bool) string {
	return MakeFilename(dirname, FTTBlock, name, isTmp)
}

func MakeBlockFileName(dirname, name string, tableId uint64, isTmp bool) string {
	// dir := MakeTableDir(dirname, id)
	dir := dirname
	return MakeFilename(dir, FTBlock, name, isTmp)
}

func MakeSegmentFileName(dirname, name string, tableId uint64, isTmp bool) string {
	// dir := MakeTableDir(dirname, id)
	dir := dirname
	return MakeFilename(dir, FTSegment, name, isTmp)
}

func MakeTableCkpFileName(dirname, name string, tableId uint64, isTmp bool) string {
	return MakeFilename(dirname, FTTableCkp, name, isTmp)
}

func MakeInfoCkpFileName(dirname, name string, isTmp bool) string {
	return MakeFilename(dirname, FTInfoCkp, name, isTmp)
}

func MakeLockFileName(dirname, name string) string {
	return MakeFilename(dirname, FTLock, name, false)
}

func ParseSegmentfileName(filename string) (name string, ok bool) {
	name = strings.TrimSuffix(filename, ".seg")
	if len(name) == len(filename) {
		return name, false
	}
	return name, true
}

func ParseTBlockfileName(filename string) (name string, ok bool) {
	name = strings.TrimSuffix(filename, ".tblk")
	if len(name) == len(filename) {
		return name, false
	}
	return name, true
}

func ParseBlockfileName(filename string) (name string, ok bool) {
	name = strings.TrimSuffix(filename, ".blk")
	if len(name) == len(filename) {
		return name, false
	}
	return name, true
}

func ParseInfoMetaName(filename string) (version int, ok bool) {
	name := strings.TrimSuffix(filename, ".ckp")
	if len(name) == len(filename) {
		return version, false
	}
	version, err := strconv.Atoi(name)
	if err != nil {
		panic(err)
	}
	return version, true
}

func ParseTableMetaName(filename string) (name string, ok bool) {
	name = strings.TrimSuffix(filename, ".tckp")
	if len(name) == len(filename) {
		return name, false
	}
	return name, true
}

func MakeFilename(dirname string, ft FileT, name string, isTmp bool) string {
	var s string
	switch ft {
	case FTLock:
		s = path.Join(dirname, fmt.Sprintf("%s.lock", name))
	case FTInfoCkp:
		s = path.Join(MakeMetaDir(dirname), fmt.Sprintf("%s.ckp", name))
	case FTTableCkp:
		s = path.Join(MakeMetaDir(dirname), fmt.Sprintf("%s.tckp", name))
	case FTTransientNode:
		s = path.Join(MakeSpillDir(dirname), fmt.Sprintf("%s.nod", name))
		isTmp = false
	case FTTBlock:
		s = path.Join(MakeDataDir(dirname), fmt.Sprintf("%s.tblk", name))
	case FTBlock:
		s = path.Join(MakeDataDir(dirname), fmt.Sprintf("%s.blk", name))
	case FTSegment:
		s = path.Join(MakeDataDir(dirname), fmt.Sprintf("%s.seg", name))
	default:
		panic(fmt.Sprintf("unsupported %d", ft))
	}
	if isTmp {
		s += ".tmp"
	}
	return s
}

func IsTempFile(name string) bool {
	return strings.HasSuffix(name, ".tmp")
}

func FilenameFromTmpfile(tmpFile string) (fname string, err error) {
	fname = strings.TrimSuffix(tmpFile, ".tmp")
	if len(fname) == len(tmpFile) {
		return "", errors.New(fmt.Sprintf("Cannot extract filename from temp file %s", tmpFile))
	}
	return fname, nil
}
