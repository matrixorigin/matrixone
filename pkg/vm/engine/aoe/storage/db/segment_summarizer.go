package db

import (
	"errors"
	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"matrixone/pkg/vm/engine"
)

type SegmentSummarizer struct {
	segment *Segment
}

func NewSegmentSummarizer(s *Segment) engine.Summarizer {
	return &SegmentSummarizer{segment: s}
}

func (s *SegmentSummarizer) Count(attr string, filter *roaring.Bitmap) (uint64, error) {
	colIdx := s.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return 0, errors.New("attr not found")
	}
	return s.segment.Data.GetIndexHolder().Count(colIdx, filter)
}

func (s *SegmentSummarizer) NullCount(attr string, filter *roaring.Bitmap) (uint64, error) {
	colIdx := s.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return 0, errors.New("attr not found")
	}
	return s.segment.Data.GetIndexHolder().NullCount(colIdx, filter)
}

func (s *SegmentSummarizer) Max(attr string, filter *roaring.Bitmap) (interface{}, error) {
	colIdx := s.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return 0, errors.New("attr not found")
	}
	return s.segment.Data.GetIndexHolder().Max(colIdx, filter)
}

func (s *SegmentSummarizer) Min(attr string, filter *roaring.Bitmap) (interface{}, error) {
	colIdx := s.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return 0, errors.New("attr not found")
	}
	return s.segment.Data.GetIndexHolder().Min(colIdx, filter)
}

func (s *SegmentSummarizer) Sum(attr string, filter *roaring.Bitmap) (int64, uint64, error) {
	colIdx := s.segment.Data.GetMeta().Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return 0, 0, errors.New("attr not found")
	}
	return s.segment.Data.GetIndexHolder().Sum(colIdx, filter)
}




