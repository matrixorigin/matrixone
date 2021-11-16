// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	DBSpec     *DBSpec      `json:"dbspec"`
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
	return fmt.Sprintf("(ndb-%s|grp-%d|size-%d|%s)", spec.DBSpec.String(), spec.Group, spec.CoarseSize, spec.Range.String())
}

func (split *TableSplitSpec) Rewrite(dbSpecs []*DBSpec) {
	for _, spec := range split.Specs {
		spec.DBSpec = dbSpecs[int(spec.Group)]
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

type DBSpec struct {
	Name    string
	ShardId uint64
}

func (spec *DBSpec) String() string {
	if spec == nil {
		return ""
	}
	return spec.Name
}

type ShardSplitSpec struct {
	Index    uint64            `json:"idx"`
	Name     string            `json:"name"`
	Specs    []*TableSplitSpec `json:"specs"`
	view     *databaseLogEntry
	splitted map[LogIndex]*Table
	db       *Database
}

func NewShardSplitSpec(name string, index uint64) *ShardSplitSpec {
	return &ShardSplitSpec{
		Name:     name,
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
	s := fmt.Sprintf("ShardSplit<%s-%d>{", split.Name, split.Index)
	for _, spec := range split.Specs {
		s = fmt.Sprintf("%s\n%s", s, spec.String())
	}
	return fmt.Sprintf("%s\n}", s)
}

func (split *ShardSplitSpec) Rewrite(specs []*DBSpec) {
	for _, spec := range split.Specs {
		spec.Rewrite(specs)
	}
}

func (split *ShardSplitSpec) Prepare(catalog *Catalog, nameFactory TableNameFactory, dbSpecs []*DBSpec) error {
	var err error
	split.db, err = catalog.SimpleGetDatabaseByName(split.Name)
	if err != nil {
		return err
	}
	split.Rewrite(dbSpecs)
	split.view = split.db.View(split.Index)
	onTable := func(table *Table) error {
		if table.CommitInfo.LogIndex == nil {
			return errors.New("log index should not be nil")
		}
		split.splitted[*table.CommitInfo.LogIndex] = table
		return nil
	}
	processor := new(LoopProcessor)
	processor.TableFn = onTable
	if err = split.view.Database.RecurLoopLocked(processor); err != nil {
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
	DBSpecs     []*DBSpec
	Index       *LogIndex
	TranId      uint64
}

func NewShardSplitter(catalog *Catalog, spec *ShardSplitSpec, dbSpecs []*DBSpec, index *LogIndex, nameFactory TableNameFactory) *ShardSplitter {
	return &ShardSplitter{
		Spec:        spec,
		Catalog:     catalog,
		NameFactory: nameFactory,
		DBSpecs:     dbSpecs,
		Index:       index,
	}
}

func (splitter *ShardSplitter) Prepare() error {
	splitter.TranId = splitter.Catalog.NextUncommitId()
	return splitter.Spec.Prepare(splitter.Catalog, splitter.NameFactory, splitter.DBSpecs)
}

func (splitter *ShardSplitter) Commit() error {
	return splitter.Catalog.execSplit(splitter.NameFactory, splitter.Spec, splitter.TranId, splitter.Index, splitter.DBSpecs)
}
