package engine

import (
	"errors"
	"fmt"
	"path"
	"strings"
)

type FileType int

const (
	FTCheckpoint FileType = iota
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

func MakeFilename(dirname string, ft FileType, name string, isTmp bool) string {
	var s string
	switch ft {
	case FTCheckpoint:
		s = path.Join(MakeMetaDir(dirname), fmt.Sprintf("%s.ckp", name))
	case FTTransientNode:
		s = path.Join(MakeSpillDir(dirname), fmt.Sprintf("%s.nod", name))
		isTmp = false
	case FTBlock:
		s = path.Join(MakeDataDir(dirname), fmt.Sprintf("%s.blk", name))
		isTmp = false
	case FTSegment:
		s = path.Join(MakeDataDir(dirname), fmt.Sprintf("%s.seg", name))
		isTmp = false
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
