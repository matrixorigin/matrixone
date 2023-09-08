// Copyright 2023 Matrix Origin
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

package backup

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
	"time"
)

func Test_writeFile(t *testing.T) {
	type args struct {
		ctx  context.Context
		fs   fileservice.FileService
		path string
		data []byte
	}

	tDir := os.TempDir()
	tPath := tDir + "/" + "test"
	err := os.RemoveAll(tPath)
	assert.NoError(t, err)
	etlFS, _, err := setupFilesystem(context.Background(), tPath, true)
	assert.NoError(t, err)

	etlFS2, _, err := setupFilesystem(context.Background(), tPath, true)
	assert.NoError(t, err)

	data1 := []byte("abc")
	data2 := []byte("abc")

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx:  context.Background(),
				fs:   etlFS,
				path: "t1",
				data: data1,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				wantData := i[0].([]byte)
				assert.NoError(t, err)
				data, err2 := readFileAndCheck(context.Background(), etlFS, "t1")
				assert.NoError(t, err2)
				assert.Equal(t, data, wantData)
				return true
			},
		},
		{
			name: "t2",
			args: args{
				ctx:  context.Background(),
				fs:   etlFS2,
				path: "t2",
				data: data1,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				wantData := i[0].([]byte)
				assert.NoError(t, err)
				data, err2 := readFileAndCheck(context.Background(), etlFS, "t2")
				assert.NoError(t, err2)
				assert.Equal(t, data, wantData)
				return true
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, writeFile(tt.args.ctx, tt.args.fs, tt.args.path, tt.args.data), data2, fmt.Sprintf("writeFile(%v, %v, %v, %v)", tt.args.ctx, tt.args.fs, tt.args.path, tt.args.data))
		})
	}
}

func getTestFs(t *testing.T, forEtl bool) fileservice.FileService {
	tPath := getTempDir(t, "test")
	etlFS, _, err := setupFilesystem(context.Background(), tPath, forEtl)
	assert.NoError(t, err)
	return etlFS
}

func getTempDir(t *testing.T, name string) string {
	tDir := os.TempDir()
	src := rand.NewSource(time.Now().UnixNano())
	tag := fmt.Sprintf("%x", src.Int63())
	tPath := tDir + "/" + tag + "/" + name
	//fmt.Println(tPath)
	return tPath
}

func getTempFile(t *testing.T, dir, name, content string) *os.File {
	file, err := os.CreateTemp(dir, name)
	assert.NoError(t, err)
	_, err = file.WriteString(content)
	assert.NoError(t, err)
	err = file.Sync()
	assert.NoError(t, err)
	err = file.Close()
	assert.NoError(t, err)
	return file
}
