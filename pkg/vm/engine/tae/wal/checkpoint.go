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

package wal

func (driver *walDriver) Checkpoint(indexes []*Index) (e LogEntry, err error) {
	e, err = driver.impl.FuzzyCheckpoint(GroupPrepare, indexes)
	return
}

func (driver *walDriver) RangeCheckpoint(start, end uint64) (e LogEntry, err error) {
	e, err = driver.impl.RangeCheckpoint(GroupPrepare, start, end)
	return
}
