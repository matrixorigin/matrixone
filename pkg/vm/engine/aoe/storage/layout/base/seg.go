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

package base

type SegmentType uint8

const (
	// UNSORTED_SEG is logical segment file,
	// only .blk&memtbale files have not been merged,
	// or only memtbale block.
	UNSORTED_SEG SegmentType = iota

	// SORTED_SEG is (block count) Already FULL, merge sorted.
	SORTED_SEG
)

func (st SegmentType) String() string {
	switch st {
	case UNSORTED_SEG:
		return "USSEG"
	case SORTED_SEG:
		return "SSEG"
	}
	panic("unsupported")
}
