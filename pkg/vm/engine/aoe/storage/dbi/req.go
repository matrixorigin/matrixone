package dbi

type OnTableDroppedCB = func(name string, opIdx uint64)

type baseOpCtx struct {
	OpIndex uint64
}

type baseTableOpCtx struct {
	baseOpCtx
	TableName string
}

type GetSnapshotCtx struct {
	baseTableOpCtx
	TableName  string
	SegmentIds []uint64
	ScanAll    bool
	Cols       []int
}

type DropTableCtx struct {
	baseTableOpCtx
	TableName  string
	OnFinishCB OnTableDroppedCB
}
