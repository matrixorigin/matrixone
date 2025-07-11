// Copyright 2025 Matrix Origin
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

package table_clone

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var _ vm.Operator = new(TableClone)

type TableCloneCtx struct {
	Eng engine.Engine

	SrcTblDef *plan.TableDef
	SrcObjDef *plan.ObjectRef

	DstTblName      string
	DstDatabaseName string

	SrcCtx       context.Context
	ScanSnapshot *plan.Snapshot

	SrcAutoIncrOffsets map[int32]uint64
}

type TableClone struct {
	vm.OperatorBase
	Ctx *TableCloneCtx

	srcRel engine.Relation
	dstRel engine.Relation

	srcRelReader engine.Reader

	dstIdxNameToRel map[string]engine.Relation
	idxNameToReader map[string]engine.Reader

	dataObjBat      *batch.Batch
	tombstoneObjBat *batch.Batch
}

func NewTableClone() *TableClone {
	return reuse.Alloc[TableClone](nil)
}
