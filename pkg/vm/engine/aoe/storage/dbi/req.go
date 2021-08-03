package dbi

import (
	"matrixone/pkg/container/batch"
)

type OnTableDroppedCB = func(error)

type TableOpCtx struct {
	OpIndex   uint64
	TableName string
}

type GetSnapshotCtx struct {
	OpIndex    uint64
	TableName  string
	SegmentIds []uint64
	ScanAll    bool
	Cols       []int
}

type DropTableCtx struct {
	OpIndex    uint64
	TableName  string
	OnFinishCB OnTableDroppedCB
}

type GetSegmentsCtx struct {
	OpIndex   uint64
	TableName string
}

type AppendCtx struct {
	OpIndex   uint64
	TableName string
	Data      *batch.Batch
}

type MatchType uint8

const (
	MTPrefix MatchType = iota
	MTFull
	MTRegex
)

type StringMatcher struct {
	Type    MatchType
	Pattern string
}

type GetSegmentedIdCtx struct {
	Matchers []*StringMatcher
}

func NewTabletSegmentedIdCtx(tablet string) *GetSegmentedIdCtx {
	ctx := &GetSegmentedIdCtx{
		Matchers: make([]*StringMatcher, 1),
	}
	ctx.Matchers[0] = &StringMatcher{
		Pattern: tablet,
	}
	return ctx
}
