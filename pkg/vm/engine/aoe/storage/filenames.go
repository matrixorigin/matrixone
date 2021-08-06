package engine

import (
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
)

type FileType int

const (
	FTInfoCkp FileType = iota
	FTTableCkp
	FTLock
	FTBlock
	FTSegment
	FTSegmentIndex
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

func MakeTableDir(dirname string, id uint64) string {
	return path.Join(dirname, fmt.Sprintf("%d", id))
}

func MakeBlockFileName(dirname, name string, tableId uint64) string {
	// dir := MakeTableDir(dirname, id)
	dir := dirname
	return MakeFilename(dir, FTBlock, name, false)
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

func MakeFilename(dirname string, ft FileType, name string, isTmp bool) string {
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
	case FTBlock:
		s = path.Join(MakeDataDir(dirname), fmt.Sprintf("%s.blk", name))
		isTmp = false
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
