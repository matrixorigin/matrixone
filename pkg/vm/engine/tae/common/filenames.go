package common

import (
	"fmt"
	"path"
)

type FileT int

const (
	FTLock FileT = iota
)

const (
	TmpSuffix  = ".tmp"
	LockSuffix = ".lock"
)

func MakeFilename(dirname string, ft FileT, name string, isTmp bool) string {
	var s string
	switch ft {
	case FTLock:
		s = path.Join(dirname, fmt.Sprintf("%s%s", name, LockSuffix))
	default:
		panic(fmt.Sprintf("unsupported %d", ft))
	}
	if isTmp {
		s += TmpSuffix
	}
	return s
}

func MakeLockFileName(dirname, name string) string {
	return MakeFilename(dirname, FTLock, name, false)
}
