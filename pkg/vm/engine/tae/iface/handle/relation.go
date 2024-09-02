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

package handle

import (
	"context"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type Relation interface {
	io.Closer
	ID() uint64
	String() string
	SimplePPString(common.PPLevel) string
	GetCardinality(attr string) int64
	Schema(bool) any
	AlterTable(ctx context.Context, req *apipb.AlterTableReq) error
	MakeObjectIt(bool) ObjectIt
	MakeObjectItOnSnap(bool) ObjectIt

	DeleteByPhyAddrKey(key any) error
	GetValueByPhyAddrKey(key any, col int) (any, bool, error)
	DeleteByPhyAddrKeys(keys containers.Vector, pkVec containers.Vector) error
	RangeDelete(id *common.ID, start, end uint32, dt DeleteType) error
	TryDeleteByDeltaloc(id *common.ID, deltaloc objectio.Location) (ok bool, err error)
	Update(id *common.ID, row uint32, col uint16, v any, isNull bool) error
	GetByFilter(ctx context.Context, filter *Filter) (id *common.ID, offset uint32, err error)
	GetValue(id *common.ID, row uint32, col uint16, skipCheckDelete bool) (any, bool, error)
	GetValueByFilter(ctx context.Context, filter *Filter, col int) (any, bool, error)
	UpdateByFilter(ctx context.Context, filter *Filter, col uint16, v any, isNull bool) error
	DeleteByFilter(ctx context.Context, filter *Filter) error

	BatchDedup(col containers.Vector) error
	Append(ctx context.Context, data *containers.Batch) error
	AddObjsWithMetaLoc(ctx context.Context, stats containers.Vector) error

	GetMeta() any
	CreateObject(bool) (Object, error)
	CreateNonAppendableObject(isTombstone bool, opt *objectio.CreateObjOpt) (Object, error)
	GetObject(id *types.Objectid, isTombstone bool) (Object, error)
	SoftDeleteObject(id *types.Objectid, isTombstone bool) (err error)
	FillInWorkspaceDeletes(blkID types.Blockid, view **nulls.Nulls) error

	GetDB() (Database, error)
}

type RelationIt interface {
	Iterator
	GetRelation() Relation
}
