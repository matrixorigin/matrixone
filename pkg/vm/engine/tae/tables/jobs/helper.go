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

package jobs

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	idxCommon "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/io"
)

func BuildAndFlushBlockIndex(file file.Block, meta *catalog.BlockEntry, pkColumnData *vector.Vector) (err error) {
	// write indexes, collect their meta, and refresh host's index holder
	schema := meta.GetSchema()
	pkColumn, err := file.OpenColumn(int(schema.PrimaryKey))
	if err != nil {
		return
	}
	zmIdx := uint16(0)
	sfIdx := uint16(1)
	metas := idxCommon.NewEmptyIndicesMeta()

	zoneMapWriter := io.NewBlockZoneMapIndexWriter()
	zmFile, err := pkColumn.OpenIndexFile(int(zmIdx))
	if err != nil {
		return err
	}
	err = zoneMapWriter.Init(zmFile, idxCommon.Plain, uint16(schema.PrimaryKey), zmIdx)
	if err != nil {
		return err
	}
	err = zoneMapWriter.AddValues(pkColumnData)
	if err != nil {
		return err
	}
	zmMeta, err := zoneMapWriter.Finalize()
	if err != nil {
		return err
	}
	metas.AddIndex(*zmMeta)

	staticFilterWriter := io.NewStaticFilterIndexWriter()
	sfFile, err := pkColumn.OpenIndexFile(int(sfIdx))
	if err != nil {
		return err
	}
	err = staticFilterWriter.Init(sfFile, idxCommon.Plain, uint16(schema.PrimaryKey), sfIdx)
	if err != nil {
		return err
	}
	err = staticFilterWriter.AddValues(pkColumnData)
	if err != nil {
		return err
	}
	sfMeta, err := staticFilterWriter.Finalize()
	if err != nil {
		return err
	}
	metas.AddIndex(*sfMeta)
	metaBuf, err := metas.Marshal()
	if err != nil {
		return err
	}

	err = file.WriteIndexMeta(metaBuf)
	if err != nil {
		return err
	}
	return nil
}
