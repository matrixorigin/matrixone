// Copyright 2023 Matrix Origin
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

package interval

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type OverlapChecker struct {
	tag          string
	intervalsMap map[string]*IntTree
}

func NewIntervalChecker(tag string) *OverlapChecker {
	return &OverlapChecker{
		intervalsMap: make(map[string]*IntTree),
		tag:          tag,
	}
}

func (i *OverlapChecker) Insert(key string, low, high int64) error {
	interval := Interval{low: low, high: high}
	if _, ok := i.intervalsMap[key]; !ok {
		i.intervalsMap[key] = NewIntervalTree()
	} else if i.intervalsMap[key].Contains(interval) {
		return moerr.NewInternalErrorNoCtx("Duplicate data interval in %s", i.tag)
	}

	i.intervalsMap[key].Insert(interval)
	return nil
}

func (i *OverlapChecker) Remove(key string, low, high int64) {
	interval := Interval{low: low, high: high}
	if _, ok := i.intervalsMap[key]; !ok {
		return
	}

	if i.intervalsMap[key].Contains(interval) {
		i.intervalsMap[key].Remove(interval)
	}
}
