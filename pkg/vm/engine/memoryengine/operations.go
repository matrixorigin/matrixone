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
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	LengthBytes = 4
	IDBytes     = 8
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

type CreateDatabaseReq struct {
	ID         ID
	AccessInfo AccessInfo
	Name       string
	Typ        string
	CreateSql  string
}

func (m *CreateDatabaseReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *CreateDatabaseReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type CreateDatabaseResp struct {
	ID ID
}

func (m *CreateDatabaseResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *CreateDatabaseResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type OpenDatabaseReq struct {
	AccessInfo AccessInfo
	Name       string
}

func (m *OpenDatabaseReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *OpenDatabaseReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type OpenDatabaseResp struct {
	ID        ID
	Name      string
	DatTyp    string
	CreateSql string
}

func (m *OpenDatabaseResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *OpenDatabaseResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type GetDatabasesReq struct {
	AccessInfo AccessInfo
}

func (m *GetDatabasesReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *GetDatabasesReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type GetDatabasesResp struct {
	Names []string
}

func (m *GetDatabasesResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *GetDatabasesResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type DeleteDatabaseReq struct {
	AccessInfo AccessInfo
	Name       string
}

func (m *DeleteDatabaseReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *DeleteDatabaseReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type DeleteDatabaseResp struct {
	ID ID
}

func (m *DeleteDatabaseResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *DeleteDatabaseResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type CreateRelationReq struct {
	ID           ID
	DatabaseID   ID
	DatabaseName string
	Name         string
	Type         RelationType
	Defs         []engine.TableDefPB
}

func (m *CreateRelationReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *CreateRelationReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type CreateRelationResp struct {
	ID ID
}

func (m *CreateRelationResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *CreateRelationResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type DeleteRelationReq struct {
	DatabaseID   ID
	DatabaseName string
	Name         string
}

func (m *DeleteRelationReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *DeleteRelationReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type TruncateRelationReq struct {
	NewTableID   ID
	OldTableID   ID
	DatabaseID   ID
	DatabaseName string
	Name         string
}

func (m *TruncateRelationReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *TruncateRelationReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type DeleteRelationResp struct {
	ID ID
}

func (m *DeleteRelationResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *DeleteRelationResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type TruncateRelationResp struct {
	ID ID
}

func (m *TruncateRelationResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *TruncateRelationResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type OpenRelationReq struct {
	DatabaseID   ID
	DatabaseName string
	Name         string
}

func (m *OpenRelationReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *OpenRelationReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type OpenRelationResp struct {
	ID           ID
	Type         RelationType
	DatabaseName string
	RelationName string
}

func (m *OpenRelationResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *OpenRelationResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type GetRelationsReq struct {
	DatabaseID ID
}

func (m *GetRelationsReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *GetRelationsReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type GetRelationsResp struct {
	Names []string
}

func (m *GetRelationsResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *GetRelationsResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type AddTableDefReq struct {
	TableID ID
	Def     engine.TableDefPB

	DatabaseName string
	TableName    string
}

func (m *AddTableDefReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *AddTableDefReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type AddTableDefResp struct {
}

func (m *AddTableDefResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *AddTableDefResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type DelTableDefReq struct {
	TableID      ID
	DatabaseName string
	TableName    string
	Def          engine.TableDefPB
}

func (m *DelTableDefReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *DelTableDefReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type DelTableDefResp struct {
}

func (m *DelTableDefResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *DelTableDefResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type DeleteReq struct {
	TableID      ID
	DatabaseName string
	TableName    string
	ColumnName   string
	Vector       *vector.Vector
}

func (r *DeleteReq) MarshalBinary() ([]byte, error) {
	vecData, err := r.Vector.MarshalBinary()
	if err != nil {
		return nil, err
	}

	//  ----------------------------------------------------------------------------------------------
	// | TableID | length | DatabaseName | length | TableName | length | ColumnName | length | Vector |
	//  ----------------------------------------------------------------------------------------------
	size := IDBytes + LengthBytes + len(r.DatabaseName) + LengthBytes + len(r.TableName) + LengthBytes + len(r.ColumnName) + LengthBytes + len(vecData)
	data := make([]byte, size)
	index := 0

	// TableID
	binary.BigEndian.PutUint64(data[index:index+IDBytes], uint64(r.TableID))
	index += IDBytes

	// DatabaseName
	binary.BigEndian.PutUint32(data[index:index+LengthBytes], uint32(len(r.DatabaseName)))
	index += LengthBytes
	n := copy(data[index:], r.DatabaseName)
	index += n

	// TableName
	binary.BigEndian.PutUint32(data[index:index+LengthBytes], uint32(len(r.TableName)))
	index += LengthBytes
	n = copy(data[index:], r.TableName)
	index += n

	// ColumnName
	binary.BigEndian.PutUint32(data[index:index+LengthBytes], uint32(len(r.ColumnName)))
	index += LengthBytes
	n = copy(data[index:], r.ColumnName)
	index += n

	// Vector
	binary.BigEndian.PutUint32(data[index:index+LengthBytes], uint32(len(vecData)))
	index += LengthBytes
	copy(data[index:], vecData)

	return data, nil
}

func (r *DeleteReq) UnmarshalBinary(data []byte) error {
	index := 0

	// TableID
	r.TableID = ID(binary.BigEndian.Uint64(data[index : index+IDBytes]))
	index += IDBytes

	// DatabaseName
	l := int(binary.BigEndian.Uint32(data[index : index+LengthBytes]))
	index += LengthBytes
	r.DatabaseName = string(data[index : index+l])
	index += l

	// TableName
	l = int(binary.BigEndian.Uint32(data[index : index+LengthBytes]))
	index += LengthBytes
	r.TableName = string(data[index : index+l])
	index += l

	// ColumnName
	l = int(binary.BigEndian.Uint32(data[index : index+LengthBytes]))
	index += LengthBytes
	r.ColumnName = string(data[index : index+l])
	index += l

	// Vector
	l = int(binary.BigEndian.Uint32(data[index : index+LengthBytes]))
	index += LengthBytes

	var vec vector.Vector
	if err := vec.UnmarshalBinary(data[index : index+l]); err != nil {
		return err
	}
	r.Vector = &vec

	return nil
}

type DeleteResp struct {
}

func (m *DeleteResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *DeleteResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type GetPrimaryKeysReq struct {
	TableID ID
}

func (m *GetPrimaryKeysReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *GetPrimaryKeysReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type GetPrimaryKeysResp struct {
	Attrs []engine.Attribute
}

func (m *GetPrimaryKeysResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *GetPrimaryKeysResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type GetTableDefsReq struct {
	TableID ID
}

func (m *GetTableDefsReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *GetTableDefsReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type GetTableColumnsReq struct {
	TableID ID
}

func (m *GetTableColumnsReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *GetTableColumnsReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type GetTableColumnsResp struct {
	Attrs []engine.Attribute
}

func (m *GetTableColumnsResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *GetTableColumnsResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type GetTableDefsResp struct {
	Defs []engine.TableDefPB
}

func (m *GetTableDefsResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *GetTableDefsResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type GetHiddenKeysReq struct {
	TableID ID
}

func (m *GetHiddenKeysReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *GetHiddenKeysReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type GetHiddenKeysResp struct {
	Attrs []engine.Attribute
}

func (m *GetHiddenKeysResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *GetHiddenKeysResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
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

type UpdateReq struct {
	TableID      ID
	DatabaseName string
	TableName    string
	Batch        *batch.Batch
}

func (r *UpdateReq) MarshalBinary() ([]byte, error) {
	batchData, err := r.Batch.MarshalBinary()
	if err != nil {
		return nil, err
	}

	//  -----------------------------------------------------------------------
	// | TableID | length | DatabaseName | length | TableName | length | Batch |
	//  -----------------------------------------------------------------------
	size := IDBytes + LengthBytes + len(r.DatabaseName) + LengthBytes + len(r.TableName) + LengthBytes + len(batchData)
	data := make([]byte, size)
	index := 0

	// TableID
	binary.BigEndian.PutUint64(data[index:index+IDBytes], uint64(r.TableID))
	index += IDBytes

	// DatabaseName
	binary.BigEndian.PutUint32(data[index:index+LengthBytes], uint32(len(r.DatabaseName)))
	index += LengthBytes
	n := copy(data[index:], r.DatabaseName)
	index += n

	// TableName
	binary.BigEndian.PutUint32(data[index:index+LengthBytes], uint32(len(r.TableName)))
	index += LengthBytes
	n = copy(data[index:], r.TableName)
	index += n

	// Batch
	binary.BigEndian.PutUint32(data[index:index+LengthBytes], uint32(len(batchData)))
	index += LengthBytes
	copy(data[index:], batchData)

	return data, nil
}

func (r *UpdateReq) UnmarshalBinary(data []byte) error {
	index := 0

	// TableID
	r.TableID = ID(binary.BigEndian.Uint64(data[index : index+IDBytes]))
	index += IDBytes

	// DatabaseName
	l := int(binary.BigEndian.Uint32(data[index : index+LengthBytes]))
	index += LengthBytes
	r.DatabaseName = string(data[index : index+l])
	index += l

	// TableName
	l = int(binary.BigEndian.Uint32(data[index : index+LengthBytes]))
	index += LengthBytes
	r.TableName = string(data[index : index+l])
	index += l

	// Batch
	l = int(binary.BigEndian.Uint32(data[index : index+LengthBytes]))
	index += LengthBytes

	var bat batch.Batch
	if err := bat.UnmarshalBinary(data[index : index+l]); err != nil {
		return err
	}
	r.Batch = &bat

	return nil
}

type UpdateResp struct {
}

func (m *UpdateResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *UpdateResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type WriteReq struct {
	TableID      ID
	DatabaseName string
	TableName    string
	Batch        *batch.Batch
}

func (r *WriteReq) MarshalBinary() ([]byte, error) {
	batchData, err := r.Batch.MarshalBinary()
	if err != nil {
		return nil, err
	}

	//  -----------------------------------------------------------------------
	// | TableID | length | DatabaseName | length | TableName | length | Batch |
	//  -----------------------------------------------------------------------
	size := IDBytes + LengthBytes + len(r.DatabaseName) + LengthBytes + len(r.TableName) + LengthBytes + len(batchData)
	data := make([]byte, size)
	index := 0

	// TableID
	binary.BigEndian.PutUint64(data[index:index+IDBytes], uint64(r.TableID))
	index += IDBytes

	// DatabaseName
	binary.BigEndian.PutUint32(data[index:index+LengthBytes], uint32(len(r.DatabaseName)))
	index += LengthBytes
	copy(data[index:], r.DatabaseName)
	index += len(r.DatabaseName)

	// TableName
	binary.BigEndian.PutUint32(data[index:index+LengthBytes], uint32(len(r.TableName)))
	index += LengthBytes
	copy(data[index:], r.TableName)
	index += len(r.TableName)

	// Batch
	binary.BigEndian.PutUint32(data[index:index+LengthBytes], uint32(len(batchData)))
	index += LengthBytes
	copy(data[index:], batchData)

	return data, nil
}

func (r *WriteReq) UnmarshalBinary(data []byte) error {
	index := 0

	// TableID
	r.TableID = ID(binary.BigEndian.Uint64(data[index : index+IDBytes]))
	index += IDBytes

	// DatabaseName
	l := int(binary.BigEndian.Uint32(data[index : index+LengthBytes]))
	index += LengthBytes
	r.DatabaseName = string(data[index : index+l])
	index += l

	// TableName
	l = int(binary.BigEndian.Uint32(data[index : index+LengthBytes]))
	index += LengthBytes
	r.TableName = string(data[index : index+l])
	index += l

	// Batch
	l = int(binary.BigEndian.Uint32(data[index : index+LengthBytes]))
	index += LengthBytes

	var bat batch.Batch
	if err := bat.UnmarshalBinary(data[index : index+l]); err != nil {
		return err
	}
	r.Batch = &bat

	return nil
}

type WriteResp struct {
}

func (m *WriteResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *WriteResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type NewTableIterReq struct {
	TableID ID
	Expr    *plan.Expr
}

func (m *NewTableIterReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *NewTableIterReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type NewTableIterResp struct {
	IterID ID
}

func (m *NewTableIterResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *NewTableIterResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type ReadReq struct {
	IterID   ID
	ColNames []string
}

func (m *ReadReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *ReadReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type ReadResp struct {
	Batch *batch.Batch

	mp *mpool.MPool
}

func (r *ReadResp) MarshalBinary() ([]byte, error) {
	if r.Batch != nil {
		return r.Batch.MarshalBinary()
	}
	return nil, nil
}

func (r *ReadResp) UnmarshalBinary(data []byte) error {
	if len(data) > 0 {
		var bat batch.Batch
		if err := bat.UnmarshalBinary(data); err != nil {
			return err
		}
		r.Batch = &bat
	}
	return nil
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

type CloseTableIterReq struct {
	IterID ID
}

func (m *CloseTableIterReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *CloseTableIterReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type CloseTableIterResp struct {
}

func (m *CloseTableIterResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *CloseTableIterResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type TableStatsReq struct {
	TableID ID
}

func (m *TableStatsReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *TableStatsReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

type TableStatsResp struct {
	Rows int
}

func (m *TableStatsResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *TableStatsResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}
