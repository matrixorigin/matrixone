package metadata

import (
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type TableRangeSpec struct {
	ShardId uint64       `json:"sid"`
	Name    string       `json:"name"`
	Range   common.Range `json:"range"`
}

type TableSplitSpec struct {
	Name  string            `json:"name"`
	Specs []*TableRangeSpec `json:"specs"`
}

func NewTableSplitSpec(name string) *TableSplitSpec {
	return &TableSplitSpec{
		Name:  name,
		Specs: make([]*TableRangeSpec, 0),
	}
}

func (split *TableSplitSpec) AddSpec(spec *TableRangeSpec) {
	split.Specs = append(split.Specs, spec)
}

type ShardSplitSpec struct {
	Index   uint64            `json:"idx"`
	ShardId uint64            `json:"id"`
	Specs   []*TableSplitSpec `json:"specs"`
	view    *catalogLogEntry
}

func NewShardSplitSpec(shardId, index uint64) *ShardSplitSpec {
	return &ShardSplitSpec{
		ShardId: shardId,
		Index:   index,
		Specs:   make([]*TableSplitSpec, 0),
	}
}

func (split *ShardSplitSpec) Prepare(catalog *Catalog) error {
	split.view = catalog.ShardView(split.ShardId, split.Index)
	return nil
}

func (split *ShardSplitSpec) AddSpec(spec *TableSplitSpec) {
	split.Specs = append(split.Specs, spec)
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
