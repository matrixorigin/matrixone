package metadata

type writeCtx struct {
	exIndex *LogIndex
}

type createTableCtx struct {
	writeCtx
	schema *Schema
	table  *Table
}

type dropTableCtx struct {
	writeCtx
	name  string
	table *Table
}

type deleteTableCtx struct {
	writeCtx
	table *Table
}

type createSegmentCtx struct {
	writeCtx
	segment *Segment
	table   *Table
}

type upgradeSegmentCtx struct {
	writeCtx
	segment  *Segment
	exIndice []*LogIndex
}

type createBlockCtx struct {
	writeCtx
	segment *Segment
	block   *Block
}

type upgradeBlockCtx struct {
	writeCtx
	block    *Block
	exIndice []*LogIndex
}

func newCreateTableCtx(schema *Schema, exIndex *LogIndex) *createTableCtx {
	return &createTableCtx{
		writeCtx: writeCtx{
			exIndex: exIndex,
		},
		schema: schema,
	}
}

func newDropTableCtx(name string, exIndex *LogIndex) *dropTableCtx {
	return &dropTableCtx{
		writeCtx: writeCtx{
			exIndex: exIndex,
		},
		name: name,
	}
}

func newDeleteTableCtx(table *Table) *deleteTableCtx {
	return &deleteTableCtx{
		table: table,
	}
}

func newCreateSegmentCtx(table *Table) *createSegmentCtx {
	return &createSegmentCtx{
		table: table,
	}
}

func newUpgradeSegmentCtx(segment *Segment, exIndice []*LogIndex) *upgradeSegmentCtx {
	return &upgradeSegmentCtx{
		segment:  segment,
		exIndice: exIndice,
	}
}

func newCreateBlockCtx(segment *Segment) *createBlockCtx {
	return &createBlockCtx{
		segment: segment,
	}
}

func newUpgradeBlockCtx(block *Block, exIndice []*LogIndex) *upgradeBlockCtx {
	return &upgradeBlockCtx{
		block:    block,
		exIndice: exIndice,
	}
}
