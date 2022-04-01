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

/*
func NewSegmentSummarizer(s *Segment) engine.Summarizer {
	return &SegmentSummarizer{segment: s}
}

func (s *SegmentSummarizer) Count(attr string, filter *roaring.Bitmap) (uint64, error) {
	colIdx := s.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return 0, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	if s.segment.Data.GetType() == base.SORTED_SEG {
		return s.segment.Data.GetIndexHolder().Count(colIdx, filter)
	} else {
		holder := s.segment.Data.GetIndexHolder()
		normalPart, err := holder.Count(colIdx, filter)
		if err != nil {
			return 0, err
		}
		transientPart := uint64(0)
		for _, blkId := range s.segment.Data.BlockIds() {
			blk := s.segment.Data.WeakRefBlock(blkId)
			if blk.GetType() == base.TRANSIENT_BLK {
				var ranger *roaring.Bitmap
				startPos := uint64(blk.GetMeta().Idx) * blk.GetMeta().Segment.Table.Schema.BlockMaxRows
				endPos := startPos + blk.GetRowCount()
				if filter != nil {
					ranger = roaring.NewBitmap()
					ranger.AddRange(startPos, endPos)
					ranger.And(filter)
					arr := ranger.ToArray()
					ranger.Clear()
					for _, e := range arr {
						ranger.Add(e - startPos)
					}
				} else {
					ranger = roaring.NewBitmap()
					ranger.AddRange(0, endPos - startPos)
				}
				transientPart += blk.Count(colIdx, ranger)
			}
		}
		return normalPart + transientPart, nil
	}
}

func (s *SegmentSummarizer) NullCount(attr string, filter *roaring.Bitmap) (uint64, error) {
	colIdx := s.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return 0, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	if s.segment.Data.GetType() == base.SORTED_SEG {
		return s.segment.Data.GetIndexHolder().NullCount(colIdx, 0, filter)
	} else {
		transientPart := uint64(0)
		for _, blkId := range s.segment.Data.BlockIds() {
			blk := s.segment.Data.WeakRefBlock(blkId)
			if blk.GetType() == base.TRANSIENT_BLK {
				startPos := uint64(blk.GetMeta().Idx) * blk.GetMeta().Segment.Table.Schema.BlockMaxRows
				endPos := startPos + blk.GetRowCount()
				maximum := startPos + blk.GetMeta().Segment.Table.Schema.BlockMaxRows
				ranger := roaring.NewBitmap()
				ranger.AddRange(startPos, maximum)
				if filter != nil {
					ranger.And(filter)
					arr := ranger.ToArray()
					ranger.Clear()
					for _, e := range arr {
						if e >= endPos && e < maximum {
							filter.Remove(e)
							continue
						}
						ranger.Add(e - startPos)
						filter.Remove(e)
					}
				}
				transientPart += blk.NullCount(colIdx, ranger)
			}
		}
		holder := s.segment.Data.GetIndexHolder()
		total := filter.GetCardinality()
		normalCount, err := holder.Count(colIdx, filter)
		if err != nil {
			return 0, err
		}
		normalPart := total - normalCount
		return normalPart + transientPart, nil
	}
}

func (s *SegmentSummarizer) Max(attr string, filter *roaring.Bitmap) (interface{}, error) {
	colIdx := s.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return 0, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	if s.segment.Data.GetType() == base.SORTED_SEG {
		return s.segment.Data.GetIndexHolder().Max(colIdx, filter)
	} else {
		holder := s.segment.Data.GetIndexHolder()
		max, err := holder.Max(colIdx, filter)
		if err != nil {
			return 0, err
		}
		for _, blkId := range s.segment.Data.BlockIds() {
			blk := s.segment.Data.WeakRefBlock(blkId)
			if blk.GetType() == base.TRANSIENT_BLK {
				startPos := uint64(blk.GetMeta().Idx) * blk.GetMeta().Segment.Table.Schema.BlockMaxRows
				endPos := startPos + blk.GetRowCount()
				ranger := roaring.NewBitmap()
				ranger.AddRange(startPos, endPos)
				filter.And(ranger)
				arr := filter.ToArray()
				ranger.Clear()
				for _, e := range arr {
					ranger.Add(e - startPos)
				}
				tmax := blk.Max(colIdx, ranger)
				if common.CompareInterface(tmax, max) > 0 {
					max = tmax
				}
			}
		}
		return max, nil
	}
}

func (s *SegmentSummarizer) Min(attr string, filter *roaring.Bitmap) (interface{}, error) {
	colIdx := s.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return 0, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	if s.segment.Data.GetType() == base.SORTED_SEG {
		return s.segment.Data.GetIndexHolder().Min(colIdx, filter)
	} else {
		holder := s.segment.Data.GetIndexHolder()
		min, err := holder.Min(colIdx, filter)
		if err != nil {
			return 0, err
		}
		for _, blkId := range s.segment.Data.BlockIds() {
			blk := s.segment.Data.WeakRefBlock(blkId)
			if blk.GetType() == base.TRANSIENT_BLK {
				startPos := uint64(blk.GetMeta().Idx) * blk.GetMeta().Segment.Table.Schema.BlockMaxRows
				endPos := startPos + blk.GetRowCount()
				ranger := roaring.NewBitmap()
				ranger.AddRange(startPos, endPos)
				filter.And(ranger)
				arr := filter.ToArray()
				ranger.Clear()
				for _, e := range arr {
					ranger.Add(e - startPos)
				}
				tmin := blk.Min(colIdx, ranger)
				if common.CompareInterface(min, tmin) > 0 {
					min = tmin
				}
			}
		}
		return min, nil
	}
}

func (s *SegmentSummarizer) Sum(attr string, filter *roaring.Bitmap) (int64, uint64, error) {
	colIdx := s.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return 0, 0, errors.New(fmt.Sprintf("column %s not found", attr))
	}
	if s.segment.Data.GetType() == base.SORTED_SEG {
		return s.segment.Data.GetIndexHolder().Sum(colIdx, filter)
	} else {
		holder := s.segment.Data.GetIndexHolder()
		sum, cnt, err := holder.Sum(colIdx, filter)
		if err != nil {
			return 0, 0, err
		}
		//logutil.Infof("...... %d %d", sum, cnt)
		for _, blkId := range s.segment.Data.BlockIds() {
			blk := s.segment.Data.WeakRefBlock(blkId)
			if blk.GetType() == base.TRANSIENT_BLK {
				startPos := uint64(blk.GetMeta().Idx) * blk.GetMeta().Segment.Table.Schema.BlockMaxRows
				endPos := startPos + blk.GetRowCount()
				ranger := roaring.NewBitmap()
				ranger.AddRange(startPos, endPos)
				filter.And(ranger)
				arr := filter.ToArray()
				ranger.Clear()
				for _, e := range arr {
					ranger.Add(e - startPos)
				}
				deltasum, deltacnt := blk.Sum(colIdx, ranger)
				sum += deltasum
				cnt += deltacnt
			}
		}
		return sum, cnt, nil
	}
}




*/
