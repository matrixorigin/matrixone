package dbi

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
