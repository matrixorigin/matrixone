// Copyright 2022 Matrix Origin
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

package memtable

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/tidwall/btree"
)

type LogIter[
	K Ordered[K],
	V any,
] struct {
	iter     btree.GenericIter[*LogEntry[K, V]]
	fromTime Time
	toTime   Time
}

func (t *Table[K, V, R]) NewLogIter(fromTime *Time, toTime *Time) *LogIter[K, V] {

	if fromTime == nil {
		fromTime = &Time{}
	}
	if toTime == nil {
		toTime = &Time{
			Timestamp: timestamp.Timestamp{
				PhysicalTime: math.MaxInt64,
			},
		}
	}

	state := t.state.Load()
	iter := state.logs.Copy().Iter()

	return &LogIter[K, V]{
		iter:     iter,
		fromTime: *fromTime,
		toTime:   *toTime,
	}
}

func (l *LogIter[K, V]) First() bool {
	pivot := &LogEntry[K, V]{
		Time: l.fromTime,
	}
	if !l.iter.Seek(pivot) {
		return false
	}
	for {
		item := l.iter.Item()
		if item.Time.Before(l.fromTime) {
			if !l.iter.Next() {
				return false
			}
			continue
		}
		if item.Time.Equal(l.toTime) {
			return false
		}
		if item.Time.After(l.toTime) {
			return false
		}
		break
	}
	return true
}

func (l *LogIter[K, V]) Close() error {
	l.iter.Release()
	return nil
}

func (l *LogIter[K, V]) Next() bool {
	if !l.iter.Next() {
		return false
	}
	for {
		item := l.iter.Item()
		if item.Time.Before(l.fromTime) {
			if !l.iter.Next() {
				return false
			}
			continue
		}
		if item.Time.Equal(l.toTime) {
			return false
		}
		if item.Time.After(l.toTime) {
			return false
		}
		break
	}
	return true
}

func (l *LogIter[K, V]) Item() *LogEntry[K, V] {
	return l.iter.Item()
}
