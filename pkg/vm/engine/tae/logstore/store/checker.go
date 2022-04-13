package store

import (
	"errors"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

const (
	DefaultRotateCheckerMaxSize = int(common.M) * 512
)

type MaxSizeRotateChecker struct {
	MaxSize int
}

func NewMaxSizeRotateChecker(size int) *MaxSizeRotateChecker {
	return &MaxSizeRotateChecker{
		MaxSize: size,
	}
}

func (c *MaxSizeRotateChecker) PrepareAppend(vfile VFile, delta int) (needRot bool, err error) {
	if delta > c.MaxSize {
		return false, errors.New(fmt.Sprintf("MaxSize is %d, but %d is received", c.MaxSize, delta))

	}
	if vfile == nil {
		return false, nil
	}
	if vfile.SizeLocked()+delta > c.MaxSize {
		return true, nil
	}
	return false, nil
}
