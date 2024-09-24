// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
)

func newLocalETLFS(t *testing.T, fsName string) fileservice.FileService {
	dir := t.TempDir()
	fs, err := fileservice.NewLocalETLFS(fsName, dir)
	assert.Nil(t, err)
	return fs
}

func newTestSession(t *testing.T, ctrl *gomock.Controller) *Session {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	go startConsumeRead(clientConn)

	var err error
	var testPool *mpool.MPool
	//parameter
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	_, err = toml.DecodeFile("test/system_vars_config.toml", pu.SV)
	assert.Nil(t, err)
	pu.SV.SetDefaultValues()
	pu.SV.SaveQueryResult = "on"
	testPool, err = mpool.NewMPool("testPool", pu.SV.GuestMmuLimitation, mpool.NoFixed)
	if err != nil {
		assert.Nil(t, err)
	}
	//file service
	pu.FileService = newLocalETLFS(t, defines.SharedFileServiceName)
	setGlobalPu(pu)
	//io session

	ioses, err := NewIOSession(serverConn, pu)
	assert.Nil(t, err)
	proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

	testutil.SetupAutoIncrService("")
	//new session
	ses := NewSession(context.TODO(), "", proto, testPool)
	var c clock.Clock
	catalog.SetupDefines("")
	_ = ses.GetTxnHandler().CreateTempStorage(c)
	tenant := &TenantInfo{
		Tenant:        sysAccountName,
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      sysAccountID,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	}
	ses.SetTenantInfo(tenant)

	stubs := gostub.StubFunc(&ExeSqlInBgSes, nil, nil)
	defer stubs.Reset()
	_ = ses.InitSystemVariables(context.TODO())
	ses.mrs = &MysqlResultSet{}

	return ses
}

func newBatch(ts []types.Type, rows int, proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(len(ts))
	bat.SetRowCount(rows)
	for i, typ := range ts {
		switch typ.Oid {
		case types.T_int8:
			vec, _ := proc.AllocVectorOfRows(typ, rows, nil)
			vs := vector.MustFixedColWithTypeCheck[int8](vec)
			for j := range vs {
				vs[j] = int8(j)
			}
			bat.Vecs[i] = vec
		default:
			panic("invalid type")
		}
	}
	return bat
}

func Test_saveQueryResultMeta(t *testing.T) {
	blockio.RunPipelineTest(
		func() {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			var err error
			var retColDef *plan.ResultColDef
			var files []resultFileInfo
			//prepare session
			ses := newTestSession(t, ctrl)
			_ = ses.SetSessionSysVar(context.TODO(), "save_query_result", int8(1))
			defer ses.Close()

			const blockCnt int = 3

			tenant := &TenantInfo{
				Tenant:   sysAccountName,
				TenantID: sysAccountID,
			}
			ses.SetTenantInfo(tenant)
			proc := testutil.NewProcess()
			proc.Base.FileService = getGlobalPu().FileService

			proc.Base.SessionInfo = process.SessionInfo{Account: sysAccountName}
			ses.GetTxnCompileCtx().execCtx = &ExecCtx{
				reqCtx: context.TODO(),
				proc:   proc,
			}

			//three columns
			typs := []types.Type{
				types.T_int8.ToType(),
				types.T_int8.ToType(),
				types.T_int8.ToType(),
			}

			colDefs := make([]*plan.ColDef, len(typs))
			for i, ty := range typs {
				colDefs[i] = &plan.ColDef{
					Name: fmt.Sprintf("a_%d", i),
					Typ: plan.Type{
						Id:    int32(ty.Oid),
						Scale: ty.Scale,
						Width: ty.Width,
					},
				}
			}

			ses.rs = &plan.ResultColDef{
				ResultCols: colDefs,
			}

			testUUID := uuid.NullUUID{}.UUID
			ses.tStmt = &motrace.StatementInfo{
				StatementID: testUUID,
			}

			ctx := context.Background()
			asts, err := parsers.Parse(ctx, dialect.MYSQL, "select a,b,c from t", 1)
			assert.Nil(t, err)

			ses.ast = asts[0]
			ses.p = &plan.Plan{}

			yes := canSaveQueryResult(ctx, ses)
			assert.True(t, yes)

			//result string
			wantResult := "0,0,0\n1,1,1\n2,2,2\n0,0,0\n1,1,1\n2,2,2\n0,0,0\n1,1,1\n2,2,2\n"
			//save blocks

			for i := 0; i < blockCnt; i++ {
				data := newBatch(typs, blockCnt, proc)
				err = saveBatch(ctx, ses, data)
				assert.Nil(t, err)
			}

			//save result meta
			err = saveMeta(ctx, ses)
			assert.Nil(t, err)

			retColDef, err = openResultMeta(ctx, ses, testUUID.String())
			assert.Nil(t, err)
			assert.NotNil(t, retColDef)

			files, err = getResultFiles(ctx, ses, testUUID.String())
			assert.Nil(t, err)
			assert.Equal(t, len(files), blockCnt)
			for i := 0; i < blockCnt; i++ {
				assert.NotEqual(t, files[i].size, int64(0))
				assert.Equal(t, files[i].blockIndex, int64(i+1))
			}

			//dump
			exportFilePath := fileservice.JoinPath(defines.SharedFileServiceName, "/block3.csv")
			ep := &tree.ExportParam{
				Outfile:  true,
				QueryId:  testUUID.String(),
				FilePath: exportFilePath,
				Fields: &tree.Fields{
					Terminated: &tree.Terminated{
						Value: ",",
					},
					EnclosedBy: &tree.EnclosedBy{
						Value: '"',
					},
				},
				Lines: &tree.Lines{
					TerminatedBy: &tree.Terminated{
						Value: "\n",
					},
				},
				MaxFileSize: 0,
				Header:      false,
				ForceQuote:  nil,
			}
			err = doDumpQueryResult(ctx, ses, ep)
			assert.Nil(t, err)

			fs := getGlobalPu().FileService

			//csvBuf := &bytes.Buffer{}
			var r io.ReadCloser
			err = fs.Read(ctx, &fileservice.IOVector{
				FilePath: exportFilePath,
				Entries: []fileservice.IOEntry{
					{
						Offset: 0,
						Size:   -1,
						//WriterForRead: csvBuf,
						ReadCloserForRead: &r,
					},
				},
			})
			assert.Nil(t, err)
			content, err := io.ReadAll(r)
			assert.Nil(t, err)
			assert.Nil(t, r.Close())
			assert.Equal(t, wantResult, string(content))
			//fmt.Println(string(content))
		},
	)
}

func Test_getFileSize(t *testing.T) {
	files := []fileservice.DirEntry{
		{Name: "a", IsDir: false, Size: 1},
	}
	assert.Equal(t, int64(1), getFileSize(files, "a"))
	assert.Equal(t, int64(-1), getFileSize(files, "b"))
}
