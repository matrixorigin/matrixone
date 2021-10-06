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
