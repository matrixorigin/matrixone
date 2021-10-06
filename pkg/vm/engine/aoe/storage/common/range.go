package common

import (
	"errors"
	"fmt"
)

var (
	RangeNotContinousErr = errors.New("aoe: range not continous")
	RangeInvalidErr      = errors.New("aoe: invalid range")
)

type Range struct {
	Left  uint64
	Right uint64
}

func (r *Range) String() string {
	return fmt.Sprintf("[%d, %d]", r.Left, r.Right)
}

func (r *Range) Valid() bool {
	return r.Left <= r.Right
}

func (r *Range) CanCover(o *Range) bool {
	if r == nil {
		return false
	}
	if o == nil {
		return true
	}
	return r.Left <= o.Left && r.Right >= o.Right
}

func (r *Range) Union(o *Range) error {
	if o.Left > r.Right+1 || r.Left > o.Right+1 {
		return RangeNotContinousErr
	}
	if r.Left > o.Left {
		r.Left = o.Left
	}
	if r.Right < o.Right {
		r.Right = o.Right
	}
	return nil
}

func (r *Range) Append(right uint64) error {
	if right < r.Left || right > r.Right+1 {
		return RangeInvalidErr
	}
	r.Right = right
	return nil
}
