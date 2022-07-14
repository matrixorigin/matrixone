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

package common

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

// "errors"
// "fmt"
// "sync/atomic"

type ClosedIntervals struct {
	Intervals []*ClosedInterval
}

func (intervals *ClosedIntervals) TryMerge(o ClosedIntervals) {
	intervals.Intervals = append(intervals.Intervals, o.Intervals...)
	sort.Slice(intervals.Intervals, func(i, j int) bool {
		return intervals.Intervals[i].Start < intervals.Intervals[j].Start
	})
	newIntervals := make([]*ClosedInterval, 0)
	if len(intervals.Intervals) == 0 {
		intervals.Intervals = newIntervals
		return
	}
	start := intervals.Intervals[0].Start
	end := intervals.Intervals[0].End
	for _, interval := range intervals.Intervals {
		if interval.Start <= end+1 {
			if interval.End > end {
				end = interval.End
			}
		} else {
			newIntervals = append(newIntervals, &ClosedInterval{
				Start: start,
				End:   end,
			})
			start = interval.Start
			end = interval.End
		}
	}
	newIntervals = append(newIntervals, &ClosedInterval{
		Start: start,
		End:   end,
	})
	intervals.Intervals = newIntervals
}

func (intervals *ClosedIntervals) Contains(o ClosedIntervals) bool {
	// sort.Slice(intervals.Intervals, func(i, j int) bool {
	// 	return intervals.Intervals[i].Start < intervals.Intervals[j].Start
	// })
	// sort.Slice(o.Intervals, func(i, j int) bool {
	// 	return o.Intervals[i].Start < o.Intervals[j].Start
	// })
	ilen := len(intervals.Intervals)
	i := 0
	for _, oIntervals := range o.Intervals {
		contains := false
		for _, iIntervals := range intervals.Intervals[i:] {
			if iIntervals.Start > oIntervals.End {
				return false
			}
			if iIntervals.Contains(*oIntervals) {
				contains = true
				break
			}
			i++
			if i == ilen {
				return false
			}
		}
		if !contains {
			return false
		}
	}
	return true
}

func (intervals *ClosedIntervals) ContainsInterval(oIntervals ClosedInterval) bool {
	// sort.Slice(intervals.Intervals, func(i, j int) bool {
	// 	return intervals.Intervals[i].Start < intervals.Intervals[j].Start
	// })
	// sort.Slice(o.Intervals, func(i, j int) bool {
	// 	return o.Intervals[i].Start < o.Intervals[j].Start
	// })
	ilen := len(intervals.Intervals)
	i := 0
	contains := false
	for _, iIntervals := range intervals.Intervals[i:] {
		if iIntervals.Start > oIntervals.End {
			return false
		}
		if iIntervals.Contains(oIntervals) {
			contains = true
			break
		}
		i++
		if i == ilen {
			return false
		}
	}
	return contains
}

func (intervals *ClosedIntervals) IsCoveredByInt(i uint64) bool {
	if intervals.Intervals == nil || len(intervals.Intervals) == 0 {
		return true
	}
	return i >= intervals.Intervals[len(intervals.Intervals)-1].End
}

func (intervals *ClosedIntervals) GetCardinality() int {
	cardinality := 0
	for _, interval := range intervals.Intervals {
		cardinality += (int(interval.End) - int(interval.Start) + 1)
	}
	return cardinality
}
func (intervals *ClosedIntervals) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, uint64(len(intervals.Intervals))); err != nil {
		return
	}
	n += 8
	for _, interval := range intervals.Intervals {
		if err = binary.Write(w, binary.BigEndian, interval.Start); err != nil {
			return
		}
		n += 8
		if err = binary.Write(w, binary.BigEndian, interval.End); err != nil {
			return
		}
		n += 8
	}
	return
}
func (intervals *ClosedIntervals) ReadFrom(r io.Reader) (n int64, err error) {
	length := uint64(0)
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}
	n += 8
	intervals.Intervals = make([]*ClosedInterval, length)
	for i := 0; i < int(length); i++ {
		intervals.Intervals[i] = &ClosedInterval{}
		if err = binary.Read(r, binary.BigEndian, &intervals.Intervals[i].Start); err != nil {
			return
		}
		n += 8
		if err = binary.Read(r, binary.BigEndian, &intervals.Intervals[i].End); err != nil {
			return
		}
		n += 8
	}
	return
}

// Equal is for test
func (intervals *ClosedIntervals) Equal(o *ClosedIntervals) bool {
	if len(intervals.Intervals) != len(o.Intervals) {
		fmt.Printf("%v\n%v\n", intervals.Intervals, o.Intervals)
		return false
	}
	for i, interval := range intervals.Intervals {
		if interval.Start != o.Intervals[i].Start || interval.End != o.Intervals[i].End {
			fmt.Printf("%v\n%v\n", intervals.Intervals, o.Intervals)
			return false
		}
	}
	return true
}

func NewClosedIntervals() *ClosedIntervals {
	return &ClosedIntervals{
		Intervals: make([]*ClosedInterval, 0),
	}
}
func NewClosedIntervalsBySlice(array []uint64) *ClosedIntervals {
	ranges := &ClosedIntervals{
		Intervals: make([]*ClosedInterval, 0),
	}
	if len(array) == 0 {
		return ranges
	}
	sort.Slice(array, func(i, j int) bool {
		return array[i] < array[j]
	})
	interval := &ClosedInterval{Start: array[0]}
	pre := array[0]
	array = array[1:]
	for _, idx := range array {
		if idx <= pre+1 {
			pre = idx
			continue
		} else {
			interval.End = pre
			ranges.Intervals = append(ranges.Intervals, interval)
			interval = &ClosedInterval{Start: idx}
			pre = idx
		}
	}
	interval.End = pre
	ranges.Intervals = append(ranges.Intervals, interval)
	return ranges
}
func NewClosedIntervalsByInt(i uint64) *ClosedIntervals {
	return &ClosedIntervals{
		Intervals: []*ClosedInterval{{
			Start: i,
			End:   i,
		}},
	}
}

func NewClosedIntervalsByInterval(i *ClosedInterval) *ClosedIntervals {
	return &ClosedIntervals{
		Intervals: []*ClosedInterval{{
			Start: i.Start,
			End:   i.End,
		}},
	}
}

func NewClosedIntervalsByIntervals(i *ClosedIntervals) *ClosedIntervals {
	intervals := &ClosedIntervals{
		Intervals: make([]*ClosedInterval, len(i.Intervals)),
	}
	for i, interval := range i.Intervals {
		intervals.Intervals[i] = &ClosedInterval{
			Start: interval.Start,
			End:   interval.End,
		}
	}
	return intervals
}
