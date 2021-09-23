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

package db

import (
	"errors"
	"fmt"
	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"matrixone/pkg/vm/engine"
)

// SegmentSummarizer provides segment-level aggregations with bitmap
// support. (e.g. Count(string, *roaring.Bitmap) (uint64, error)
// where inputs are column name and row filter, returns a count
// telling the number of rows filtered by input.
// Sum(string, *roaring.Bitmap) (int64, uint64, error) where inputs
// are column name and row filter, returns count of rows, sum of those
// counted rows, and an error if returned. Others are similar.)
type SegmentSummarizer struct {
	segment *Segment
}

func NewSegmentSummarizer(s *Segment) engine.Summarizer {
	return &SegmentSummarizer{segment: s}
}

func (s *SegmentSummarizer) Count(attr string, filter *roaring.Bitmap) (uint64, error) {
	colIdx := s.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return 0, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	return s.segment.Data.GetIndexHolder().Count(colIdx, filter)
}

func (s *SegmentSummarizer) NullCount(attr string, filter *roaring.Bitmap) (uint64, error) {
	colIdx := s.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return 0, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	return s.segment.Data.GetIndexHolder().NullCount(colIdx, filter)
}

func (s *SegmentSummarizer) Max(attr string, filter *roaring.Bitmap) (interface{}, error) {
	colIdx := s.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return 0, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	return s.segment.Data.GetIndexHolder().Max(colIdx, filter)
}

func (s *SegmentSummarizer) Min(attr string, filter *roaring.Bitmap) (interface{}, error) {
	colIdx := s.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return 0, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	return s.segment.Data.GetIndexHolder().Min(colIdx, filter)
}

func (s *SegmentSummarizer) Sum(attr string, filter *roaring.Bitmap) (int64, uint64, error) {
	colIdx := s.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return 0, 0, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	return s.segment.Data.GetIndexHolder().Sum(colIdx, filter)
}




