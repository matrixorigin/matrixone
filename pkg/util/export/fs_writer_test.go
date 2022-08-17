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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/stretchr/testify/require"
)

const (
	B = 1 << iota
	KB
	MB
	GB
)

func TestLocalFSWriter(t *testing.T) {
	ex, err := os.Executable()
	require.Equal(t, nil, err)
	t.Logf("executor: %s", ex)
	path, err := filepath.Abs(".")
	require.Equal(t, nil, err)
	t.Logf("whereami: %s", path)
	err = os.RemoveAll("store/")
	require.Equal(t, nil, err)
	fs, err := fileservice.NewLocalFS("test", "store/system", MB)
	require.Equal(t, nil, err)
	ctx := context.Background()
	// fs_writer_test.go:23: whereami: /private/var/folders/lw/05zz3bq12djbnhv1wyzk2jgh0000gn/T/GoLand
	// fs_writer_test.go:40: write statement file error: size not match
	// fs_writer_test.go:50: write span file error: file existed
	// file result: (has prefix)
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
	require.Equal(t, errors.New("file existed"), err)
	// file result: (has prefix)
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
	ex, err := os.Executable()
	require.Equal(t, nil, err)
	t.Logf("executor: %s", ex)
	path, err := filepath.Abs(".")
	require.Equal(t, nil, err)
	t.Logf("whereami: %s", path)
	//err = os.RemoveAll("store/")
	//require.Equal(t, nil, err)
	fs, err := fileservice.NewLocalETLFS("etl", "store")
	require.Equal(t, nil, err)
	ctx := context.Background()
	// fs_writer_test.go:23: whereami: /private/var/folders/lw/05zz3bq12djbnhv1wyzk2jgh0000gn/T/GoLand
	// fs_writer_test.go:40: write statement file error: size not match
	// fs_writer_test.go:50: write span file error: file existed
	// file result: (has prefix)
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
	require.Equal(t, errors.New("file existed"), err)
	// file result: (has prefix)
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

func TestFSWriter_Write(t *testing.T) {
	type fields struct {
		ctx      context.Context
		fs       fileservice.FileService
		prefix   batchpipe.HasName
		dir      string
		nodeUUID string
		nodeType string
	}
	type args struct {
		p []byte
	}

	os.RemoveAll("store/")
	path, err := filepath.Abs(".")
	require.Equal(t, nil, err)
	t.Logf("path: %s", path)

	localFs, err := fileservice.NewLocalFS("test", "store/", MB) // db root dir.
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
				prefix:   newNum(1),
				dir:      "system", // database name
				nodeUUID: "node_uuid",
				nodeType: "standalone",
			},
			args: args{
				p: []byte(`0000000000000001,00000000-0000-0000-0000-000000000001,0000000000000000,node_uuid,Node,span1,1970-01-01 00:00:00.000000,1970-01-01 00:00:00.000001,1000,"{""Node"":{""node_uuid"":""node_uuid"",""node_type"":0},""version"":""v0.test.0""}"
0000000000000002,00000000-0000-0000-0000-000000000001,0000000000000000,node_uuid,Node,span2,1970-01-01 00:00:00.000001,1970-01-01 00:00:00.001000,999000,"{""Node"":{""node_uuid"":""node_uuid"",""node_type"":0},""version"":""v0.test.0""}"
`,
				)},
			wantN:   474,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewFSWriter(tt.fields.ctx, tt.fields.fs,
				WithPrefix(tt.fields.prefix),
				WithDir(tt.fields.dir),
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

func TestParseFileService(t *testing.T) {
	type args struct {
		ctx      context.Context
		fsConfig fileservice.Config
	}
	localETLFS, err := fileservice.NewLocalETLFS("localETL", "store")
	require.Equal(t, nil, err)
	tests := []struct {
		name    string
		args    args
		wantFs  fileservice.FileService
		wantCfg Config
		wantErr bool
	}{
		{
			name: "disk",
			args: args{
				ctx: context.Background(),
				fsConfig: fileservice.Config{
					Name:                  "local",
					Backend:               DiskFSBackend,
					CacheMemCapacityBytes: 0,
					S3:                    fileservice.S3Config{},
					DataDir:               "store",
				},
			},
			wantFs:  localETLFS,
			wantCfg: Config{backend: DiskFSBackend, baseDir: "store"},
			wantErr: false,
		},
		{
			name: "s3",
			args: args{
				ctx: context.Background(),
				fsConfig: fileservice.Config{
					Name:                  "s3",
					Backend:               S3FSBackend,
					CacheMemCapacityBytes: 0,
					S3: fileservice.S3Config{
						SharedConfigProfile: "", /* in running env, shoule be 'default' */
						Endpoint:            "s3.us-west-2.amazonaws.com",
						Bucket:              "xzxiong2208",
						KeyPrefix:           "store",
					},
					DataDir: "store",
				},
			},
			wantFs: localETLFS,
			wantCfg: Config{
				backend:         S3FSBackend,
				baseDir:         "store",
				endpoint:        "s3.us-west-2.amazonaws.com",
				accessKeyID:     "key_id_2",
				secretAccessKey: "access_key_3",
				bucket:          "xzxiong2208",
				region:          "region_1",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("AWS_REGION", tt.wantCfg.region)
			t.Setenv("AWS_ACCESS_KEY_ID", tt.wantCfg.accessKeyID)
			t.Setenv("AWS_SECRET_ACCESS_KEY", tt.wantCfg.secretAccessKey)
			gotFs, gotCfg, err := ParseFileService(tt.args.ctx, tt.args.fsConfig)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseFileService() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			switch _type := gotFs.(type) {
			case *fileservice.LocalETLFS:
				require.Equal(t, tt.wantCfg, gotCfg)
				require.Equal(t, tt.wantFs, gotFs)
			case *fileservice.S3FS:
				require.Equal(t, tt.wantCfg, gotCfg)
			default:
				t.Errorf("unknown type: %v", _type)
			}
		})
	}
}
