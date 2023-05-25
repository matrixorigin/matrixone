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
	keyIntervals map[string]*IntTree
}

func NewOverlapChecker(tag string) *OverlapChecker {
	return &OverlapChecker{
		keyIntervals: make(map[string]*IntTree),
		tag:          tag,
	}
}

func (i *OverlapChecker) Insert(key string, low, high int64) error {
	interval := Interval{low: low, high: high}
	if _, ok := i.keyIntervals[key]; !ok {
		i.keyIntervals[key] = NewIntervalTree()
	} else if i.keyIntervals[key].Contains(interval) {
		return moerr.NewInternalErrorNoCtx("Duplicate key range in %s", i.tag)
	}

	i.keyIntervals[key].Insert(interval)
	return nil
}

func (i *OverlapChecker) Remove(key string, low, high int64) error {
	interval := Interval{low: low, high: high}
	if _, ok := i.keyIntervals[key]; !ok {
		return moerr.NewInternalErrorNoCtx("Key Range not found for removal in %s", i.tag)
	}

	if i.keyIntervals[key].Contains(interval) {
		i.keyIntervals[key].Remove(interval)

		if i.keyIntervals[key].Size() == 0 {
			delete(i.keyIntervals, key)
		}
	}

	return nil
}
