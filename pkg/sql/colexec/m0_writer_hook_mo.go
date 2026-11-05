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

package colexec

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func init() {
	activeM0WriterSchemaVersion = m0WriterSchemaVersion
}

func m0WriterSchemaVersion(tableDef *plan.TableDef, seqnums []uint16) (uint32, error) {
	if tableDef == nil {
		return 0, nil
	}
	c, err := m0read.PlanEvolution(tableDef)
	if err != nil {
		return 0, err
	}
	if c == nil {
		return tableDef.Version, nil
	}
	if tableDef.ClusterBy != nil || tableDef.IsTemporary || len(tableDef.Indexes) != 0 || len(tableDef.Fkeys) != 0 || len(tableDef.Checks) != 0 {
		return 0, moerr.NewInternalErrorNoCtx("M0: writer schema outside contract domain")
	}
	owner := m0read.EvolutionBinding{Account: c.AccountId, Database: tableDef.DbId, Relation: tableDef.TblId}
	if err := m0read.ValidateEvolution(c, owner); err != nil {
		return 0, err
	}
	if len(seqnums) != len(c.Columns) {
		return 0, moerr.NewInternalErrorNoCtx("M0: CN writer does not contain the complete bound layout")
	}
	for i, seq := range seqnums {
		if uint32(seq) != c.Columns[i].Seqnum || int(seq) >= len(tableDef.Cols) {
			return 0, moerr.NewInternalErrorNoCtx("M0: CN writer column identity mismatch")
		}
		col := tableDef.Cols[seq]
		if col == nil || col.ColId != c.Columns[i].ColumnId {
			return 0, moerr.NewInternalErrorNoCtx("M0: CN writer stable identity mismatch")
		}
		encoded, err := m0read.PlanEvolutionColumnSemantic(tableDef, col)
		if err != nil {
			return 0, err
		}
		if !bytes.Equal(encoded, c.Columns[i].Semantics) {
			return 0, moerr.NewInternalErrorNoCtx("M0: CN writer semantics mismatch")
		}
	}
	return c.LayoutGeneration, nil
}
