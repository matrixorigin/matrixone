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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var (
	ErrRangeNotContinuous = moerr.NewInternalErrorNoCtx("tae: range not continuous")
	ErrRangeInvalid       = moerr.NewInternalErrorNoCtx("tae: invalid range")
)

type Range struct {
	Left  uint64 `json:"l"`
	Right uint64 `json:"r"`
}

func (r *Range) String() string {
	if r == nil {
		return "[]"
	}
	return fmt.Sprintf("[%d, %d]", r.Left, r.Right)
}

func (r *Range) Valid() bool {
	return r.Left <= r.Right
}

func (r *Range) LT(id uint64) bool {
	return r.Right < id
}

func (r *Range) GT(id uint64) bool {
	return r.Left > id
}

func (r *Range) ClosedIn(id uint64) bool {
	return r.Left <= id && r.Right >= id
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

func (r *Range) CommitLeft(left uint64) bool {
	if left > r.Right {
		return false
	}
	if left < r.Left {
		r.Left = left
	}
	return true
}

func (r *Range) Union(o *Range) error {
	if o.Left > r.Right+1 || r.Left > o.Right+1 {
		return ErrRangeNotContinuous
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
	if r.Left == r.Right && r.Right == 0 {
		r.Right = right
		r.Left = right
		return nil
	}
	// if right < r.Left || right > r.Right+1 {
	if right <= r.Right {
		return ErrRangeInvalid
	}
	r.Right = right
	return nil
}
