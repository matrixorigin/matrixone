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

package catalog

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type EntryState int8

const (
	ES_Appendable EntryState = iota
	ES_NotAppendable
	ES_Frozen
)

var (
	AppendNodeApproxSize int
)

func (es EntryState) Repr() string {
	switch es {
	case ES_Appendable:
		return "A"
	case ES_NotAppendable:
		return "NA"
	case ES_Frozen:
		return "F"
	}
	panic("not supported")
}

func GetTombstoneSchema(objectSchema *Schema) *Schema {
	if !objectSchema.HasPKOrFakePK() {
		return nil
	}
	pkType := objectSchema.GetPrimaryKey().GetType()
	schema := NewEmptySchema("tombstone")
	schema.Extra.BlockMaxRows = objectSchema.Extra.BlockMaxRows
	schema.Extra.ObjectMaxBlocks = objectSchema.Extra.ObjectMaxBlocks
	colTypes := []types.Type{
		types.T_Rowid.ToType(),
		pkType,
	}
	for i, colname := range objectio.TombstoneAttrs_CN_Created {
		if i == 0 {
			if err := schema.AppendPKCol(colname, colTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := schema.AppendCol(colname, colTypes[i]); err != nil {
				panic(err)
			}
		}
	}
	schema.Finalize(false)
	return schema
}

// rowid, pk
// used in range delete
func NewTombstoneBatchWithPKVector(pkVec, rowIDVec containers.Vector, mp *mpool.MPool) *containers.Batch {
	bat := containers.NewBatchWithCapacity(2)
	bat.AddVector(objectio.TombstoneAttr_Rowid_Attr, rowIDVec)
	bat.AddVector(objectio.TombstoneAttr_PK_Attr, pkVec)
	return bat
}

// rowid, pk, commitTS
// used in Collect Delete in Range
func NewTombstoneBatchByPKType(pkType types.Type, mp *mpool.MPool) *containers.Batch {
	bat := containers.NewBatchWithCapacity(3)
	rowIDVec := containers.MakeVector(objectio.RowidType, mp)
	pkVec := containers.MakeVector(pkType, mp)
	commitTSVec := containers.MakeVector(objectio.TSType, mp)
	bat.AddVector(objectio.TombstoneAttr_Rowid_Attr, rowIDVec)
	bat.AddVector(objectio.TombstoneAttr_PK_Attr, pkVec)
	bat.AddVector(objectio.TombstoneAttr_CommitTs_Attr, commitTSVec)
	return bat
}

// rowid, pk, commitTS
// used in Collect Delete in Range
func NewCNTombstoneBatchByPKType(pkType types.Type, mp *mpool.MPool) *containers.Batch {
	bat := containers.NewBatchWithCapacity(2)
	rowIDVec := containers.MakeVector(objectio.RowidType, mp)
	pkVec := containers.MakeVector(pkType, mp)
	bat.AddVector(objectio.TombstoneAttr_Rowid_Attr, rowIDVec)
	bat.AddVector(objectio.TombstoneAttr_PK_Attr, pkVec)
	return bat
}
