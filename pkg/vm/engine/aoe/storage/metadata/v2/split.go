package metadata

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type TableNameFactory interface {
	Encode(shardId uint64, name string) string
	Decode(name string) (uint64, string)
	Rename(name string, shardId uint64) string
}

type TableRangeSpec struct {
	ShardId    uint64       `json:"-"`
	CoarseSize int64        `json:"size"`
	Group      uint32       `json:"group"`
	Range      common.Range `json:"range"`
}

type TableSplitSpec struct {
	Index LogIndex          `json:"idx"`
	Specs []*TableRangeSpec `json:"spec"`
}

func NewTableSplitSpec(index *LogIndex) *TableSplitSpec {
	return &TableSplitSpec{
		Index: *index,
		Specs: make([]*TableRangeSpec, 0),
	}
}

func (spec *TableRangeSpec) String() string {
	return fmt.Sprintf("(sid-%d|grp-%d|size-%d|%s)", spec.ShardId, spec.Group, spec.CoarseSize, spec.Range.String())
}

func (split *TableSplitSpec) Rewrite(newShards []uint64) {
	for _, spec := range split.Specs {
		spec.ShardId = newShards[int(spec.Group)]
	}
}

func (split *TableSplitSpec) AddSpec(spec *TableRangeSpec) {
	split.Specs = append(split.Specs, spec)
}

func (split *TableSplitSpec) String() string {
	s := fmt.Sprintf("TableSpec<%s>{", split.Index.String())
	for _, spec := range split.Specs {
		s = fmt.Sprintf("%s\n%s", s, spec.String())
	}
	return fmt.Sprintf("%s\n}", s)
}

type ShardSplitSpec struct {
	Index    uint64            `json:"idx"`
	ShardId  uint64            `json:"id"`
	Specs    []*TableSplitSpec `json:"specs"`
	view     *catalogLogEntry
	splitted map[LogIndex]*Table
}

func NewShardSplitSpec(shardId, index uint64) *ShardSplitSpec {
	return &ShardSplitSpec{
		ShardId:  shardId,
		Index:    index,
		Specs:    make([]*TableSplitSpec, 0),
		splitted: make(map[LogIndex]*Table),
	}
}

func NewEmptyShardSplitSpec() *ShardSplitSpec {
	return &ShardSplitSpec{
		Specs:    make([]*TableSplitSpec, 0),
		splitted: make(map[LogIndex]*Table),
	}
}

func (split *ShardSplitSpec) String() string {
	s := fmt.Sprintf("ShardSplit<%d-%d>{", split.ShardId, split.Index)
	for _, spec := range split.Specs {
		s = fmt.Sprintf("%s\n%s", s, spec.String())
	}
	return fmt.Sprintf("%s\n}", s)
}

func (split *ShardSplitSpec) Rewrite(newShards []uint64) {
	for _, spec := range split.Specs {
		spec.Rewrite(newShards)
	}
}

func (split *ShardSplitSpec) Prepare(catalog *Catalog, nameFactory TableNameFactory, newShards []uint64) error {
	split.Rewrite(newShards)
	split.view = catalog.ShardView(split.ShardId, split.Index)
	onTable := func(table *Table) error {
		if table.CommitInfo.LogIndex == nil {
			return errors.New("log index should not be nil")
		}
		split.splitted[*table.CommitInfo.LogIndex] = table
		return nil
	}
	processor := new(loopProcessor)
	processor.TableFn = onTable
	err := split.view.Catalog.RecurLoop(processor)
	if err != nil {
		return err
	}
	if len(split.Specs) != len(split.splitted) {
		return errors.New("inconsistent")
	}
	// TODO: more checks
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
	Spec        *ShardSplitSpec
	Catalog     *Catalog
	NameFactory TableNameFactory
	NewShards   []uint64
	Index       *LogIndex
	TranId      uint64
}

func NewShardSplitter(catalog *Catalog, spec *ShardSplitSpec, newShards []uint64, index *LogIndex, nameFactory TableNameFactory) *ShardSplitter {
	return &ShardSplitter{
		Spec:        spec,
		Catalog:     catalog,
		NameFactory: nameFactory,
		NewShards:   newShards,
		Index:       index,
	}
}

func (splitter *ShardSplitter) Prepare() error {
	splitter.TranId = splitter.Catalog.NextUncommitId()
	return splitter.Spec.Prepare(splitter.Catalog, splitter.NameFactory, splitter.NewShards)
}

func (splitter *ShardSplitter) Commit() error {
	return splitter.Catalog.doSpliteShard(splitter.NameFactory, splitter.Spec, splitter.TranId, splitter.Index)
}
