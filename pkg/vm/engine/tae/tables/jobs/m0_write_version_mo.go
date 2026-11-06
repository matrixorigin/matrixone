//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jobs

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

func init() {
	activeM0WriteSchemaVersion = m0WriteSchemaVersion
}

func m0WriteSchemaVersion(schema *catalog.Schema, seqnums []uint16, databaseID, relationID uint64, fallback uint32) (uint32, error) {
	if schema == nil || schema.Extra == nil || schema.Extra.M0Evolution == nil {
		return fallback, nil
	}
	c := schema.Extra.M0Evolution
	owner := m0read.EvolutionBinding{Account: schema.AcInfo.TenantID, Database: databaseID, Relation: relationID}
	if err := m0read.ValidateEvolution(c, owner); err != nil {
		return 0, err
	}
	columns := 0
	for _, col := range schema.ColDefs {
		if col.IsPhyAddr() {
			continue
		}
		if int(col.SeqNum) >= len(c.Columns) {
			return 0, moerr.NewInternalErrorNoCtx("M0: writer schema exceeds contract")
		}
		if int(col.SeqNum) != columns {
			return 0, moerr.NewInternalErrorNoCtx("M0: writer schema has non-contiguous identities")
		}
		columns++
		want := c.Columns[col.SeqNum]
		actual, err := catalog.EvolutionSchemaColumnSemantic(schema, col, want.ColumnId)
		if err != nil {
			return 0, err
		}
		if !bytes.Equal(actual, want.Semantics) {
			return 0, moerr.NewInternalErrorNoCtx("M0: writer semantics disagree with contract")
		}
	}
	if columns != len(c.Columns) {
		return 0, moerr.NewInternalErrorNoCtx("M0: writer schema does not cover contract")
	}
	count := uint32(0)
	for _, seq := range seqnums {
		if seq >= objectio.SEQNUM_UPPER {
			continue
		}
		if uint32(seq) != count {
			return 0, moerr.NewInternalErrorNoCtx("M0: writer has incomplete declared layout")
		}
		count++
	}
	if count < c.RootColumns || int(count) > len(c.Columns) {
		return 0, moerr.NewInternalErrorNoCtx("M0: writer layout outside contract")
	}
	// Match an explicitly recorded layout, including a pinned older prefix.
	generation := c.Columns[count-1].IntroducedLayout
	width, err := m0read.EvolutionWidth(c, owner, generation)
	if err != nil {
		return 0, err
	}
	if width != count {
		return 0, moerr.NewInternalErrorNoCtx("M0: writer layout is not a complete contract generation")
	}
	return generation, nil
}
