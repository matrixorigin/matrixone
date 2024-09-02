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

var DefaultTableDataFactory TableDataFactory

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

var (
	TombstoneCNSchemaAttr = []string{
		AttrRowID,
		AttrPKVal,
	}
)

const (
	TombstonePrimaryKeyIdx int = 0
)

var (
	TombstoneBatchIdxes = []int{0, 1}
)

func GetTombstoneSchema(objectSchema *Schema) *Schema {
	pkType := objectSchema.GetPrimaryKey().GetType()
	schema := NewEmptySchema("tombstone")
	schema.BlockMaxRows = objectSchema.BlockMaxRows
	schema.ObjectMaxBlocks = objectSchema.ObjectMaxBlocks
	colTypes := []types.Type{
		types.T_Rowid.ToType(),
		pkType,
	}
	for i, colname := range TombstoneCNSchemaAttr {
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
func NewTombstoneBatchWithPKVector(pkVec containers.Vector, mp *mpool.MPool) *containers.Batch {
	bat := containers.NewBatch()
	rowIDVec := containers.MakeVector(types.T_Rowid.ToType(), mp)
	// commitTSVec := containers.MakeVector(types.T_TS.ToType(), mp)
	// abortVec := containers.MakeVector(types.T_bool.ToType(), mp)
	bat.AddVector(AttrRowID, rowIDVec)
	// bat.AddVector(AttrCommitTs, commitTSVec)
	bat.AddVector(AttrPKVal, pkVec)
	// bat.AddVector(AttrAborted, abortVec)
	return bat
}

// rowid, pk, commitTS
// used in Collect Delete in Range
func NewTombstoneBatchByPKType(pkType types.Type, mp *mpool.MPool) *containers.Batch {
	bat := containers.NewBatch()
	rowIDVec := containers.MakeVector(types.T_Rowid.ToType(), mp)
	pkVec := containers.MakeVector(pkType, mp)
	commitTSVec := containers.MakeVector(types.T_TS.ToType(), mp)
	bat.AddVector(AttrRowID, rowIDVec)
	bat.AddVector(AttrPKVal, pkVec)
	bat.AddVector(AttrCommitTs, commitTSVec)
	return bat
}

func BuildLocation(stats objectio.ObjectStats, blkOffset uint16, blkMaxRows uint32) objectio.Location {
	blkRow := blkMaxRows
	if blkOffset == uint16(stats.BlkCnt())-1 {
		blkRow = stats.Rows() - uint32(blkOffset)*blkMaxRows
	}
	metaloc := objectio.BuildLocation(stats.ObjectName(), stats.Extent(), blkRow, blkOffset)
	return metaloc
}
