package dbi

import "matrixone/pkg/container/batch"

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
