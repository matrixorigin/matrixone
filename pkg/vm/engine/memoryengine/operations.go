// Copyright 2022 Matrix Origin
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

package memoryengine

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
)

const (
	OpCreateDatabase = iota + 64
	OpOpenDatabase
	OpGetDatabases
	OpDeleteDatabase
	OpCreateRelation
	OpDeleteRelation
	OpTruncateRelation
	OpOpenRelation
	OpGetRelations
	OpAddTableDef
	OpDelTableDef
	OpDelete
	OpGetPrimaryKeys
	OpGetTableColumns
	OpGetTableDefs
	OpGetHiddenKeys
	OpUpdate
	OpWrite
	OpNewTableIter
	OpRead
	OpCloseTableIter
	OpTableStats
	OpPreCommit  = uint32(apipb.OpCode_OpPreCommit)
	OpGetLogTail = uint32(apipb.OpCode_OpGetLogTail)
)

type ReadRequest interface {
	OpenDatabaseReq |
		GetDatabasesReq |
		OpenRelationReq |
		GetRelationsReq |
		GetPrimaryKeysReq |
		GetTableColumnsReq |
		GetTableDefsReq |
		GetHiddenKeysReq |
		NewTableIterReq |
		ReadReq |
		CloseTableIterReq |
		TableStatsReq |
		apipb.SyncLogTailReq
}

type WriteReqeust interface {
	CreateDatabaseReq |
		DeleteDatabaseReq |
		CreateRelationReq |
		DeleteRelationReq |
		TruncateRelationReq |
		AddTableDefReq |
		DelTableDefReq |
		DeleteReq |
		UpdateReq |
		WriteReq
}

type Request interface {
	ReadRequest | WriteReqeust
}

type Response interface {
	CreateDatabaseResp |
		OpenDatabaseResp |
		GetDatabasesResp |
		DeleteDatabaseResp |
		CreateRelationResp |
		DeleteRelationResp |
		TruncateRelationResp |
		OpenRelationResp |
		GetRelationsResp |
		AddTableDefResp |
		DelTableDefResp |
		DeleteResp |
		GetPrimaryKeysResp |
		GetTableColumnsResp |
		GetTableDefsResp |
		GetHiddenKeysResp |
		UpdateResp |
		WriteResp |
		NewTableIterResp |
		ReadResp |
		CloseTableIterResp |
		TableStatsResp |
		apipb.SyncLogTailResp
}

//type TruncateReq struct {
//	TableID      ID
//	DatabaseName string
//	TableName    string
//}
//
//type TruncateResp struct {
//	AffectedRows int64
//}

type ReadResp struct {
	Batch *batch.Batch

	mp *mpool.MPool
}

func (r *ReadResp) Close() error {
	if r.Batch != nil {
		r.Batch.Clean(r.mp)
	}
	return nil
}

func (r *ReadResp) SetHeap(mp *mpool.MPool) {
	r.mp = mp
}
