package metadata

import (
	"encoding/json"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type TableNameFactory interface {
	Encoder(shardId uint64, name string) string
	Decoder(name string) (uint64, string)
	Rename(name string, shardId uint64) string
}

type TableRangeSpec struct {
	ShardId uint64       `json:"sid"`
	Range   common.Range `json:"range"`
}

type TableSplitSpec struct {
	Index LogIndex          `json:"idx"`
	Specs []*TableRangeSpec `json:"specs"`
}

func NewTableSplitSpec(index *LogIndex) *TableSplitSpec {
	return &TableSplitSpec{
		Index: *index,
		Specs: make([]*TableRangeSpec, 0),
	}
}

func (split *TableSplitSpec) AddSpec(spec *TableRangeSpec) {
	split.Specs = append(split.Specs, spec)
}

type ShardSplitSpec struct {
	Index       uint64                       `json:"idx"`
	ShardId     uint64                       `json:"id"`
	Specs       map[LogIndex]*TableSplitSpec `json:"specs"`
	NameFactory TableNameFactory             `json:"-"`
	view        *catalogLogEntry
}

func NewShardSplitSpec(shardId, index uint64, nameFactory TableNameFactory) *ShardSplitSpec {
	return &ShardSplitSpec{
		ShardId:     shardId,
		Index:       index,
		Specs:       make(map[LogIndex]*TableSplitSpec),
		NameFactory: nameFactory,
	}
}

func (split *ShardSplitSpec) Prepare(catalog *Catalog) error {
	split.view = catalog.ShardView(split.ShardId, split.Index)
	inverted := make(map[LogIndex]*Table)
	onTable := func(table *Table) error {
		if table.CommitInfo.LogIndex == nil {
			return errors.New("log index should not be nil")
		}
		inverted[*table.CommitInfo.LogIndex] = table
		return nil
	}
	processor := new(loopProcessor)
	processor.TableFn = onTable
	err := split.view.Catalog.RecurLoop(processor)
	if err != nil {
		return err
	}
	if len(split.Specs) != len(inverted) {
		return errors.New("inconsistent")
	}
	// for index, spec := range split.Specs {
	// 	table := inverted[index]
	// }
	return nil
}

func (split *ShardSplitSpec) AddSpec(spec *TableSplitSpec) {
	split.Specs[spec.Index] = spec
}

func (split *ShardSplitSpec) Marshal() ([]byte, error) {
	return json.Marshal(split)
}

func (split *ShardSplitSpec) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, split)
}

type ShardSplitter struct {
	Spec    *ShardSplitSpec
	Catalog *Catalog
}

func NewShardSplitter(catalog *Catalog, spec *ShardSplitSpec) *ShardSplitter {
	return &ShardSplitter{
		Spec:    spec,
		Catalog: catalog,
	}
}

func (splitter *ShardSplitter) Prepare() error {
	// err := splitter.Spec.Prepare(splitter.Catalog)
	// if err != nil {
	// 	return err
	// }
	// for _, spec := range splitter.Spec.Specs {
	// }
	return nil
}

func (splitter *ShardSplitter) Commit() error {
	return nil
}
