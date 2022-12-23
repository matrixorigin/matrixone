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

package frontend

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

const QueryResultPath = "s3:/query_result/%s_%s.blk"
const QueryResultMetaPath = "s3:/query_result_meta/%s_%s.blk"

type Meta struct {
	QueryId    [16]byte
	Statement  string
	AccountId  uint32
	RoleId     uint32
	ResultPath string
	CreateTime types.Timestamp
	ResultSize float64
	Columns    string
}

var MetaColTypes = []types.Type{
	types.New(types.T_uuid, 0, 0, 0),      // query_id
	types.New(types.T_text, 0, 0, 0),      // statement
	types.New(types.T_uint32, 0, 0, 0),    // account_id
	types.New(types.T_uint32, 0, 0, 0),    // role_id
	types.New(types.T_text, 0, 0, 0),      // result_path
	types.New(types.T_timestamp, 0, 0, 0), // create_time
	types.New(types.T_float64, 0, 0, 0),   // result_size
	types.New(types.T_text, 0, 0, 0),      // columns
}

var MetaColNames = []string{
	"query_id",
	"statement",
	"account_id",
	"role_id",
	"result_path",
	"create_time",
	"result_size",
	"columns",
}

func buildQueryResultPath(accountName, statementId string) string {
	return fmt.Sprintf(QueryResultPath, accountName, statementId)
}

func buildQueryResultMetaPath(accountName, statementId string) string {
	return fmt.Sprintf(QueryResultMetaPath, accountName, statementId)
}

func saveQueryResult(ses *Session, bat *batch.Batch) error {
	fs := ses.GetParameterUnit().FileService
	// write query result
	path := buildQueryResultPath(ses.GetTenantInfo().GetTenant(), uuid.UUID(ses.tStmt.StatementID).String())
	writer, err := objectio.NewObjectWriter(path, fs)
	if err != nil {
		return err
	}
	_, err = writer.Write(bat)
	if err != nil {
		return err
	}
	_, err = writer.WriteEnd(ses.requestCtx)
	if err != nil {
		return err
	}
	// write query result meta
	var cols string
	for _, attr := range bat.Attrs {
		cols += fmt.Sprintf("`%s`", attr)
	}
	m := &Meta{
		QueryId:    ses.tStmt.StatementID,
		Statement:  ses.tStmt.Statement,
		AccountId:  ses.GetTenantInfo().GetTenantID(),
		RoleId:     ses.tStmt.RoleId,
		ResultPath: path,
		CreateTime: types.CurrentTimestamp(),
		ResultSize: 100,
		Columns:    cols,
	}
	metaBat, err := buildQueryResultMetaBatch(m, ses.mp)
	if err != nil {
		return err
	}
	metaPath := buildQueryResultMetaPath(ses.GetTenantInfo().GetTenant(), uuid.UUID(ses.tStmt.StatementID).String())
	metaWriter, err := objectio.NewObjectWriter(metaPath, fs)
	if err != nil {
		return err
	}
	_, err = metaWriter.Write(metaBat)
	if err != nil {
		return err
	}
	_, err = metaWriter.WriteEnd(ses.requestCtx)
	if err != nil {
		return err
	}
	return nil
}

func buildQueryResultMetaBatch(m *Meta, mp *mpool.MPool) (*batch.Batch, error) {
	var err error
	bat := batch.NewWithSize(len(MetaColTypes))
	bat.SetAttributes(MetaColNames)
	for i, t := range MetaColTypes {
		bat.Vecs[i] = vector.New(t)
	}
	if err = bat.Vecs[0].Append(types.Uuid(m.QueryId), false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[1].Append([]byte(m.Statement), false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[2].Append(m.AccountId, false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[3].Append(m.RoleId, false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[4].Append([]byte(m.ResultPath), false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[5].Append(m.CreateTime, false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[6].Append(m.ResultSize, false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[7].Append([]byte(m.Columns), false, mp); err != nil {
		return nil, err
	}
	return bat, nil
}
