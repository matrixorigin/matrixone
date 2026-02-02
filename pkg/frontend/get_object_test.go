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

package frontend

import (
	"context"
	"iter"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// stubFileService is a stub implementation of fileservice.FileService for testing
type stubFileService struct {
	statFileFunc func(ctx context.Context, filePath string) (*fileservice.DirEntry, error)
}

func (s *stubFileService) Name() string {
	return "stub"
}

func (s *stubFileService) Write(ctx context.Context, vector fileservice.IOVector) error {
	return nil
}

func (s *stubFileService) Read(ctx context.Context, vector *fileservice.IOVector) error {
	return nil
}

func (s *stubFileService) ReadCache(ctx context.Context, vector *fileservice.IOVector) error {
	return nil
}

func (s *stubFileService) List(ctx context.Context, dirPath string) iter.Seq2[*fileservice.DirEntry, error] {
	return func(yield func(*fileservice.DirEntry, error) bool) {}
}

func (s *stubFileService) Delete(ctx context.Context, filePaths ...string) error {
	return nil
}

func (s *stubFileService) StatFile(ctx context.Context, filePath string) (*fileservice.DirEntry, error) {
	if s.statFileFunc != nil {
		return s.statFileFunc(ctx, filePath)
	}
	return nil, moerr.NewInternalError(ctx, "not implemented")
}

func (s *stubFileService) PrefetchFile(ctx context.Context, filePath string) error {
	return nil
}

func (s *stubFileService) Cost() *fileservice.CostAttr {
	return nil
}

func (s *stubFileService) Close(ctx context.Context) {
}

func Test_handleGetObject(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	convey.Convey("handleGetObject succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Mock engine
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		// Note: We'll need to mock the fileservice, but for now we'll test the error path

		// Mock txn operator
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().GetWorkspace().Return(nil).AnyTimes()

		// Mock txn client
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		// Mock background exec for permission check
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		// Mock exec result for account name query
		erAccount := mock_frontend.NewMockExecResult(ctrl)
		erAccount.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		erAccount.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return("sys", nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return([]interface{}{erAccount}).AnyTimes()

		// Mock exec result for publication query
		erPub := mock_frontend.NewMockExecResult(ctrl)
		erPub.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		erPub.EXPECT().GetString(gomock.Any(), uint64(0), uint64(6)).Return("*", nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return([]interface{}{erPub}).AnyTimes()

		// Setup system variables
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		setSessionAlloc("", NewLeakCheckAllocator())
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.So(err, convey.ShouldBeNil)
		pu.StorageEngine = eng
		pu.TxnClient = txnClient
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, "", proto, nil)
		tenant := &TenantInfo{
			Tenant:   "sys",
			TenantID: catalog.System_Account,
			User:     DefaultTenantMoAdmin,
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}
		ses.SetDatabaseName("test_db")

		// Mock TxnHandler
		txnHandler := InitTxnHandler("", eng, ctx, txnOperator)
		ses.txnHandler = txnHandler

		proto.SetSession(ses)

		// Test with object name
		objectName := tree.Identifier("test_object")
		stmt := &tree.GetObject{
			ObjectName: objectName,
			ChunkIndex: 0,
		}

		// This will fail because we can't easily mock disttae.Engine and fileservice
		// But we can test that the function handles errors properly
		err = handleGetObject(ctx, ses, stmt)
		// We expect an error because fileservice is not available
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_handleGetObject_InvalidChunkIndex(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	convey.Convey("handleGetObject invalid chunk index", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)

		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		setSessionAlloc("", NewLeakCheckAllocator())
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.So(err, convey.ShouldBeNil)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, "", proto, nil)
		ses.mrs = &MysqlResultSet{}

		objectName := tree.Identifier("test_object")
		stmt := &tree.GetObject{
			ObjectName: objectName,
			ChunkIndex: -1, // Invalid chunk index
		}

		err = handleGetObject(ctx, ses, stmt)
		// Note: With mock engine, error occurs before chunkIndex validation
		// because mock engine is not *disttae.Engine
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_ReadObjectFromEngine(t *testing.T) {
	ctx := context.Background()
	convey.Convey("ReadObjectFromEngine invalid engine", t, func() {
		// Test with nil engine
		_, err := ReadObjectFromEngine(ctx, nil, "test_object", 0, 100)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(moerr.IsMoErrCode(err, moerr.ErrInternal), convey.ShouldBeTrue)
	})
}

// Test_handleGetObject_WithMockCheckers tests handleGetObject using mock checkers
// This test covers the main code paths (lines 142-229) in get_object.go
func Test_handleGetObject_WithMockCheckers(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)

	convey.Convey("handleGetObject with mock checkers - full path coverage", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Mock engine (for session setup)
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		// Mock txn operator
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().GetWorkspace().Return(newTestWorkspace()).AnyTimes()
		txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()

		// Mock txn client
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		// Setup system variables
		sv, err := getSystemVariables("test/system_vars_config.toml")
		convey.So(err, convey.ShouldBeNil)
		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		setSessionAlloc("", NewLeakCheckAllocator())
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.So(err, convey.ShouldBeNil)
		pu.StorageEngine = eng
		pu.TxnClient = txnClient
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, "", proto, nil)
		tenant := &TenantInfo{
			Tenant:   "sys",
			TenantID: catalog.System_Account,
			User:     DefaultTenantMoAdmin,
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}
		ses.SetDatabaseName("test_db")

		// Mock TxnHandler
		txnHandler := InitTxnHandler("", eng, ctx, txnOperator)
		ses.txnHandler = txnHandler

		proto.SetSession(ses)

		// Stub GetObjectPermissionChecker - permission passes
		permStub := gostub.Stub(&GetObjectPermissionChecker, func(ctx context.Context, ses *Session, pubAccountName, pubName string) (uint64, error) {
			return 0, nil
		})
		defer permStub.Reset()

		// Test case 1: chunk index = 0 (metadata only request)
		// File size = 500KB (less than 1MB chunk size, so totalChunks = 1)
		convey.Convey("chunk index 0 - metadata only", func() {
			stubFS := &stubFileService{
				statFileFunc: func(ctx context.Context, filePath string) (*fileservice.DirEntry, error) {
					return &fileservice.DirEntry{
						Name: "test_object",
						Size: 500 * 1024, // 500KB
					}, nil
				},
			}

			fsStub := gostub.Stub(&GetObjectFSProvider, func(ses *Session) (fileservice.FileService, error) {
				return stubFS, nil
			})
			defer fsStub.Reset()

			stmt := &tree.GetObject{
				ObjectName: tree.Identifier("test_object"),
				ChunkIndex: 0,
			}

			ses.mrs = &MysqlResultSet{}
			err = handleGetObject(ctx, ses, stmt)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ses.mrs.GetRowCount(), convey.ShouldEqual, 1)

			// Verify result: data=nil, fileSize=500*1024, chunkIndex=0, totalChunks=1, isComplete=false
			row, err := ses.mrs.GetRow(ctx, 0)
			convey.So(err, convey.ShouldBeNil)
			convey.So(row[0], convey.ShouldBeNil)                  // data
			convey.So(row[1], convey.ShouldEqual, int64(500*1024)) // fileSize
			convey.So(row[2], convey.ShouldEqual, int64(0))        // chunkIndex
			convey.So(row[3], convey.ShouldEqual, int64(1))        // totalChunks
			convey.So(row[4], convey.ShouldEqual, false)           // isComplete
		})

		// Test case 2: chunk index = 1 (first and only data chunk for small file)
		convey.Convey("chunk index 1 - single chunk file, is complete", func() {
			stubFS := &stubFileService{
				statFileFunc: func(ctx context.Context, filePath string) (*fileservice.DirEntry, error) {
					return &fileservice.DirEntry{
						Name: "test_object",
						Size: 500 * 1024, // 500KB, totalChunks = 1
					}, nil
				},
			}

			fsStub := gostub.Stub(&GetObjectFSProvider, func(ses *Session) (fileservice.FileService, error) {
				return stubFS, nil
			})
			defer fsStub.Reset()

			// Stub GetObjectDataReader for this test
			testData := []byte("test file content data")
			dataStub := gostub.Stub(&GetObjectDataReader, func(ctx context.Context, ses *Session, objectName string, offset int64, size int64) ([]byte, error) {
				convey.So(offset, convey.ShouldEqual, 0)
				convey.So(size, convey.ShouldEqual, 500*1024)
				return testData, nil
			})
			defer dataStub.Reset()

			stmt := &tree.GetObject{
				ObjectName: tree.Identifier("test_object"),
				ChunkIndex: 1,
			}

			ses.mrs = &MysqlResultSet{}
			err = handleGetObject(ctx, ses, stmt)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ses.mrs.GetRowCount(), convey.ShouldEqual, 1)

			// Verify result: data=testData, fileSize=500*1024, chunkIndex=1, totalChunks=1, isComplete=true
			row, err := ses.mrs.GetRow(ctx, 0)
			convey.So(err, convey.ShouldBeNil)
			convey.So(row[0], convey.ShouldResemble, testData)     // data
			convey.So(row[1], convey.ShouldEqual, int64(500*1024)) // fileSize
			convey.So(row[2], convey.ShouldEqual, int64(1))        // chunkIndex
			convey.So(row[3], convey.ShouldEqual, int64(1))        // totalChunks
			convey.So(row[4], convey.ShouldEqual, true)            // isComplete
		})

		// Test case 3: Multi-chunk file - chunk index = 1 (not last chunk)
		// Note: getObjectChunkSize is 100MB (100 * 1024 * 1024)
		convey.Convey("chunk index 1 - multi chunk file, not complete", func() {
			// File size = 250MB, so totalChunks = 3 (with 100MB chunk size)
			chunkSize := int64(100 * 1024 * 1024) // 100MB
			fileSize := int64(250 * 1024 * 1024)  // 250MB, totalChunks = 3
			stubFS := &stubFileService{
				statFileFunc: func(ctx context.Context, filePath string) (*fileservice.DirEntry, error) {
					return &fileservice.DirEntry{
						Name: "large_object",
						Size: fileSize,
					}, nil
				},
			}

			fsStub := gostub.Stub(&GetObjectFSProvider, func(ses *Session) (fileservice.FileService, error) {
				return stubFS, nil
			})
			defer fsStub.Reset()

			testData := make([]byte, chunkSize) // 100MB chunk
			dataStub := gostub.Stub(&GetObjectDataReader, func(ctx context.Context, ses *Session, objectName string, offset int64, size int64) ([]byte, error) {
				convey.So(offset, convey.ShouldEqual, 0)       // chunk 1 starts at offset 0
				convey.So(size, convey.ShouldEqual, chunkSize) // 100MB chunk size
				return testData, nil
			})
			defer dataStub.Reset()

			stmt := &tree.GetObject{
				ObjectName: tree.Identifier("large_object"),
				ChunkIndex: 1,
			}

			ses.mrs = &MysqlResultSet{}
			err = handleGetObject(ctx, ses, stmt)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ses.mrs.GetRowCount(), convey.ShouldEqual, 1)

			row, err := ses.mrs.GetRow(ctx, 0)
			convey.So(err, convey.ShouldBeNil)
			convey.So(row[0], convey.ShouldResemble, testData) // data
			convey.So(row[1], convey.ShouldEqual, fileSize)    // fileSize
			convey.So(row[2], convey.ShouldEqual, int64(1))    // chunkIndex
			convey.So(row[3], convey.ShouldEqual, int64(3))    // totalChunks (250MB / 100MB = 3)
			convey.So(row[4], convey.ShouldEqual, false)       // isComplete (not last chunk)
		})

		// Test case 4: Multi-chunk file - last chunk (isComplete = true)
		// Note: getObjectChunkSize is 100MB (100 * 1024 * 1024)
		convey.Convey("last chunk - is complete", func() {
			// File size = 250MB, so totalChunks = 3, last chunk is 50MB
			chunkSize := int64(100 * 1024 * 1024) // 100MB
			fileSize := int64(250 * 1024 * 1024)  // 250MB
			stubFS := &stubFileService{
				statFileFunc: func(ctx context.Context, filePath string) (*fileservice.DirEntry, error) {
					return &fileservice.DirEntry{
						Name: "large_object",
						Size: fileSize,
					}, nil
				},
			}

			fsStub := gostub.Stub(&GetObjectFSProvider, func(ses *Session) (fileservice.FileService, error) {
				return stubFS, nil
			})
			defer fsStub.Reset()

			lastChunkSize := fileSize - 2*chunkSize // 50MB (250MB - 200MB)
			testData := make([]byte, lastChunkSize)
			dataStub := gostub.Stub(&GetObjectDataReader, func(ctx context.Context, ses *Session, objectName string, offset int64, size int64) ([]byte, error) {
				convey.So(offset, convey.ShouldEqual, 2*chunkSize) // chunk 3 starts at 200MB
				convey.So(size, convey.ShouldEqual, lastChunkSize) // remaining size (50MB)
				return testData, nil
			})
			defer dataStub.Reset()

			stmt := &tree.GetObject{
				ObjectName: tree.Identifier("large_object"),
				ChunkIndex: 3, // last chunk
			}

			ses.mrs = &MysqlResultSet{}
			err = handleGetObject(ctx, ses, stmt)
			convey.So(err, convey.ShouldBeNil)

			row, err := ses.mrs.GetRow(ctx, 0)
			convey.So(err, convey.ShouldBeNil)
			convey.So(row[0], convey.ShouldResemble, testData) // data
			convey.So(row[1], convey.ShouldEqual, fileSize)    // fileSize
			convey.So(row[2], convey.ShouldEqual, int64(3))    // chunkIndex
			convey.So(row[3], convey.ShouldEqual, int64(3))    // totalChunks
			convey.So(row[4], convey.ShouldEqual, true)        // isComplete
		})

		// Test case 5: Invalid chunk index (negative)
		convey.Convey("invalid chunk index - negative", func() {
			stubFS := &stubFileService{
				statFileFunc: func(ctx context.Context, filePath string) (*fileservice.DirEntry, error) {
					return &fileservice.DirEntry{
						Name: "test_object",
						Size: 500 * 1024,
					}, nil
				},
			}

			fsStub := gostub.Stub(&GetObjectFSProvider, func(ses *Session) (fileservice.FileService, error) {
				return stubFS, nil
			})
			defer fsStub.Reset()

			stmt := &tree.GetObject{
				ObjectName: tree.Identifier("test_object"),
				ChunkIndex: -1,
			}

			ses.mrs = &MysqlResultSet{}
			err = handleGetObject(ctx, ses, stmt)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(moerr.IsMoErrCode(err, moerr.ErrInvalidInput), convey.ShouldBeTrue)
		})

		// Test case 6: Invalid chunk index (exceeds total chunks)
		convey.Convey("invalid chunk index - exceeds total chunks", func() {
			stubFS := &stubFileService{
				statFileFunc: func(ctx context.Context, filePath string) (*fileservice.DirEntry, error) {
					return &fileservice.DirEntry{
						Name: "test_object",
						Size: 500 * 1024, // totalChunks = 1
					}, nil
				},
			}

			fsStub := gostub.Stub(&GetObjectFSProvider, func(ses *Session) (fileservice.FileService, error) {
				return stubFS, nil
			})
			defer fsStub.Reset()

			stmt := &tree.GetObject{
				ObjectName: tree.Identifier("test_object"),
				ChunkIndex: 5, // exceeds totalChunks (1)
			}

			ses.mrs = &MysqlResultSet{}
			err = handleGetObject(ctx, ses, stmt)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(moerr.IsMoErrCode(err, moerr.ErrInvalidInput), convey.ShouldBeTrue)
		})

		// Test case 7: Permission check failed
		convey.Convey("permission check failed", func() {
			// Temporarily replace permission checker to return error
			permStub.Reset()
			permStub = gostub.Stub(&GetObjectPermissionChecker, func(ctx context.Context, ses *Session, pubAccountName, pubName string) (uint64, error) {
				return 0, moerr.NewInternalError(ctx, "permission denied")
			})
			defer permStub.Reset()

			stmt := &tree.GetObject{
				ObjectName: tree.Identifier("test_object"),
				ChunkIndex: 0,
			}

			ses.mrs = &MysqlResultSet{}
			err = handleGetObject(ctx, ses, stmt)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(moerr.IsMoErrCode(err, moerr.ErrInternal), convey.ShouldBeTrue)
		})

		// Test case 8: FileService provider failed
		convey.Convey("fileservice provider failed", func() {
			// Restore permission checker
			permStub.Reset()
			permStub = gostub.Stub(&GetObjectPermissionChecker, func(ctx context.Context, ses *Session, pubAccountName, pubName string) (uint64, error) {
				return 0, nil
			})

			fsStub := gostub.Stub(&GetObjectFSProvider, func(ses *Session) (fileservice.FileService, error) {
				return nil, moerr.NewInternalErrorNoCtx("fileservice not available")
			})
			defer fsStub.Reset()

			stmt := &tree.GetObject{
				ObjectName: tree.Identifier("test_object"),
				ChunkIndex: 0,
			}

			ses.mrs = &MysqlResultSet{}
			err = handleGetObject(ctx, ses, stmt)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(moerr.IsMoErrCode(err, moerr.ErrInternal), convey.ShouldBeTrue)
		})

		// Test case 9: StatFile failed
		convey.Convey("stat file failed", func() {
			stubFS := &stubFileService{
				statFileFunc: func(ctx context.Context, filePath string) (*fileservice.DirEntry, error) {
					return nil, moerr.NewInternalError(ctx, "file not found")
				},
			}

			fsStub := gostub.Stub(&GetObjectFSProvider, func(ses *Session) (fileservice.FileService, error) {
				return stubFS, nil
			})
			defer fsStub.Reset()

			stmt := &tree.GetObject{
				ObjectName: tree.Identifier("nonexistent_object"),
				ChunkIndex: 0,
			}

			ses.mrs = &MysqlResultSet{}
			err = handleGetObject(ctx, ses, stmt)
			convey.So(err, convey.ShouldNotBeNil)
		})

		// Test case 10: Data reader failed
		convey.Convey("data reader failed", func() {
			stubFS := &stubFileService{
				statFileFunc: func(ctx context.Context, filePath string) (*fileservice.DirEntry, error) {
					return &fileservice.DirEntry{
						Name: "test_object",
						Size: 500 * 1024,
					}, nil
				},
			}

			fsStub := gostub.Stub(&GetObjectFSProvider, func(ses *Session) (fileservice.FileService, error) {
				return stubFS, nil
			})
			defer fsStub.Reset()

			dataStub := gostub.Stub(&GetObjectDataReader, func(ctx context.Context, ses *Session, objectName string, offset int64, size int64) ([]byte, error) {
				return nil, moerr.NewInternalError(ctx, "read error")
			})
			defer dataStub.Reset()

			stmt := &tree.GetObject{
				ObjectName: tree.Identifier("test_object"),
				ChunkIndex: 1, // request data chunk
			}

			ses.mrs = &MysqlResultSet{}
			err = handleGetObject(ctx, ses, stmt)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(moerr.IsMoErrCode(err, moerr.ErrInternal), convey.ShouldBeTrue)
		})
	})
}
