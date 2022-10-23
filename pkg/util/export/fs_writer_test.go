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

package export

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/stretchr/testify/require"
)

func TestLocalFSWriter(t *testing.T) {
	ex, err := os.Executable()
	require.Equal(t, nil, err)
	t.Logf("executor: %s", ex)
	selfDir, err := filepath.Abs(".")
	require.Equal(t, nil, err)
	basedir := t.TempDir()
	t.Logf("whereami: %s, %s", selfDir, basedir)

	require.Equal(t, nil, err)
	fs, err := fileservice.NewLocalFS("test", path.Join(basedir, "system"), mpool.MB)
	require.Equal(t, nil, err)
	ctx := context.Background()
	// fs_writer_test.go:23: whereami: /private/var/folders/lw/05zz3bq12djbnhv1wyzk2jgh0000gn/T/GoLand
	// fs_writer_test.go:40: write statement file error: size not match
	// fs_writer_test.go:50: write span file error: file existed
	// file result: (has checksum)
	// 9a80 8760 3132 3334 3536 3738 0a         ...`12345678.
	err = fs.Write(ctx, fileservice.IOVector{ // write-once-read-multi
		FilePath: "statement_node_uuid_20220818_000000_123456",
		Entries: []fileservice.IOEntry{
			{
				Size: 4,
				Data: []byte("1234"),
			},
			{
				Offset: 4, // offset is import
				Size:   4,
				Data:   []byte("5678"),
			},
		},
	})
	t.Logf("write statement file error: %v", err)
	require.Equal(t, nil, err)
	err = fs.Write(ctx, fileservice.IOVector{
		FilePath: "statement_node_uuid_20220818_000000_123456",
		Entries: []fileservice.IOEntry{
			{
				Offset: 8, // offset is import
				Size:   4,
				Data:   []byte("321"),
			},
		},
	})
	t.Logf("write statement file error: %v", err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists))
	// file result: (has checksum)
	// 3f3f 3a3f 3132 3334 0a                   ??:?1234.
	err = fs.Write(ctx, fileservice.IOVector{
		FilePath: "span_node_uuid_20220818_000000_123456", // each file is only can open-write for one time.
		Entries: []fileservice.IOEntry{
			{
				Size: 4,
				Data: []byte("1234"),
			},
		},
	})
	t.Logf("write span file error: %v", err)
}

func TestLocalETLFSWriter(t *testing.T) {
	basedir := t.TempDir()
	fs, err := fileservice.NewLocalETLFS("etl", basedir)
	require.Equal(t, nil, err)

	// file result: (without checksum)
	ctx := context.Background()
	err = fs.Write(ctx, fileservice.IOVector{ // write-once-read-multi
		FilePath: "statement_node_uuid_20220818_000000_123456",
		Entries: []fileservice.IOEntry{
			{
				Size: 4,
				Data: []byte("1234"),
			},
			{
				Offset: 4, // offset is import
				Size:   4,
				Data:   []byte("5678"),
			},
		},
	})
	t.Logf("write statement file error: %v", err)
	require.Equal(t, nil, err)

	err = fs.Write(ctx, fileservice.IOVector{
		FilePath: "statement_node_uuid_20220818_000000_123456",
		Entries: []fileservice.IOEntry{
			{
				Offset: 8, // offset is import
				Size:   4,
				Data:   []byte("321"),
			},
		},
	})
	t.Logf("write statement file error: %v", err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists))

	err = fs.Write(ctx, fileservice.IOVector{
		FilePath: "span_node_uuid_20220818_000000_123456", // each file is only can open-write for one time.
		Entries: []fileservice.IOEntry{
			{
				Size: 4,
				Data: []byte("1234"),
			},
		},
	})
	t.Logf("write span file error: %v", err)
}

func TestFSWriter_Write(t *testing.T) {
	type fields struct {
		ctx      context.Context
		fs       fileservice.FileService
		prefix   batchpipe.HasName
		database string
		nodeUUID string
		nodeType string
	}
	type args struct {
		p []byte
	}

	basedir := t.TempDir()
	path, err := filepath.Abs(".")
	require.Equal(t, nil, err)
	t.Logf("path: %s", path)

	localFs, err := fileservice.NewLocalFS(etlFileServiceName, basedir, mpool.MB) // db root database.
	require.Equal(t, nil, err)
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantN   int
		wantErr bool
	}{
		{
			name: "local_fs",
			fields: fields{
				ctx:      context.Background(),
				fs:       localFs,
				prefix:   newDummy(1),
				database: "system",
				nodeUUID: "node_uuid",
				nodeType: "standalone",
			},
			args: args{
				p: []byte(`0000000000000001,00000000-0000-0000-0000-000000000001,0000000000000000,node_uuid,Node,span1,1970-01-01 00:00:00.000000,1970-01-01 00:00:00.000001,1000,"{""Node"":{""node_uuid"":""node_uuid"",""node_type"":""Standalone""},""version"":""v0.test.0""}"
0000000000000002,00000000-0000-0000-0000-000000000001,0000000000000000,node_uuid,Node,span2,1970-01-01 00:00:00.000001,1970-01-01 00:00:00.001000,999000,"{""Node"":{""node_uuid"":""node_uuid"",""node_type"":""Standalone""},""version"":""v0.test.0""}"
`,
				)},
			wantN:   500,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewFSWriter(tt.fields.ctx, tt.fields.fs,
				WithName(tt.fields.prefix),
				WithDatabase(tt.fields.database),
				WithNode(tt.fields.nodeUUID, tt.fields.nodeType),
			)
			gotN, err := w.Write(tt.args.p)
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotN != tt.wantN {
				t.Errorf("Write() gotN = %v, want %v", gotN, tt.wantN)
			}
		})
	}
}
