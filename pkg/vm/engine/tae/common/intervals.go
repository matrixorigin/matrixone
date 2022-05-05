package common

import (
	"fmt"
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

func (intervals *ClosedIntervals) GetCardinality() int {
	cardinality := 0
	for _, interval := range intervals.Intervals {
		cardinality += (int(interval.End) - int(interval.Start) + 1)
	}
	return cardinality
}

//for test
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
