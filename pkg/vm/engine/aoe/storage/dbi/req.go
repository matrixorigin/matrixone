package dbi

type GetSnapshotCtx struct {
	TableName  string
	SegmentIds []uint64
	ScanAll    bool
	Cols       []int
}
