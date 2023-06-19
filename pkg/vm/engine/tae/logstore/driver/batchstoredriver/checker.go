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

package batchstoredriver

import "github.com/matrixorigin/matrixone/pkg/common/mpool"

const (
	DefaultRotateCheckerMaxSize = int(mpool.MB) * 64
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
	if vfile == nil {
		return false, nil
	}
	if vfile.SizeLocked() > c.MaxSize {
		return true, nil
	}
	return false, nil
}
