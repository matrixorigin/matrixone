package logstore

import (
	"errors"
	"fmt"
)

type IRotateChecker interface {
	PrepareAppend(*VersionFile, int64) (bool, error)
}

type MaxSizeRotationChecker struct {
	MaxSize int
}

func (c *MaxSizeRotationChecker) PrepareAppend(f *VersionFile, delta int64) (bool, error) {
	if delta > int64(c.MaxSize) {
		return false, errors.New(fmt.Sprintf("MaxSize is %d, but %d is received", c.MaxSize, delta))
	}
	if f == nil {
		return false, nil
	}
	if f.Size+delta > int64(c.MaxSize) {
		return true, nil
	}
	return false, nil
}

type noRotationChecker struct{}

func (c *noRotationChecker) PrepareAppend(_ *VersionFile, delta int64) (bool, error) {
	return false, nil
}
