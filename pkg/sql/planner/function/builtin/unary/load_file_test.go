// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package unary

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestLoadFile(t *testing.T) {
	dir := t.TempDir()
	proc := testutil.NewProc()
	ctx := context.Background()
	filepath := dir + "test"
	fs, readPath, err := fileservice.GetForETL(proc.FileService, filepath)
	assert.Nil(t, err)
	err = fs.Write(ctx, fileservice.IOVector{
		FilePath: readPath,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   4,
				Data:   []byte("1234"),
			},
			{
				Offset: 4,
				Size:   4,
				Data:   []byte("5678"),
			},
		},
	})
	assert.Nil(t, err)

	cases1 := []struct {
		name     string
		filename string
		want     []byte
	}{
		{
			name:     "File Case",
			filename: filepath,
			want:     []byte("12345678"),
		},
	}
	for _, c := range cases1 {
		convey.Convey(c.name, t, func() {
			var inStrs []string
			inStrs = append(inStrs, c.filename)
			inVector := testutil.MakeVarcharVector(inStrs, nil)
			res, err := LoadFile([]*vector.Vector{inVector}, proc)
			convey.So(err, convey.ShouldBeNil)
			convey.So(res.GetBytesAt(0), convey.ShouldResemble, c.want)
		})
	}

	//Test empty file
	filepath = dir + "emptyfile"
	fs, readPath, err = fileservice.GetForETL(proc.FileService, filepath)
	assert.Nil(t, err)
	err = fs.Write(ctx, fileservice.IOVector{
		FilePath: readPath,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   0,
				Data:   []byte(""),
			},
		},
	})
	assert.Nil(t, err)

	cases2 := []struct {
		name     string
		filename string
		want     [][]byte
	}{
		{
			name:     "Empty File Case",
			filename: filepath,
		},
	}
	for _, c := range cases2 {
		convey.Convey(c.name, t, func() {
			var inStrs []string
			inStrs = append(inStrs, c.filename)
			inVector := testutil.MakeVarcharVector(inStrs, nil)
			res, err := LoadFile([]*vector.Vector{inVector}, proc)
			convey.So(err, convey.ShouldBeNil)
			convey.So(vector.MustBytesCol(res), convey.ShouldResemble, c.want)
		})
	}

	// Test Error
	size := 1024 * 1024 * 1
	data := make([]byte, size)
	filepath = dir + "bigfile"
	fs, readPath, err = fileservice.GetForETL(proc.FileService, filepath)
	assert.Nil(t, err)
	err = fs.Write(ctx, fileservice.IOVector{
		FilePath: readPath,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   int64(size),
				Data:   data,
			},
		},
	})
	assert.Nil(t, err)

	cases3 := []struct {
		name     string
		filename string
		want     error
	}{
		{
			name:     "Error Case",
			filename: filepath,
			want:     moerr.NewInternalError(ctx, "Data too long for blob"),
		},
	}
	for _, c := range cases3 {
		convey.Convey(c.name, t, func() {
			var inStrs []string
			inStrs = append(inStrs, c.filename)
			inVector := testutil.MakeVarcharVector(inStrs, nil)
			_, err := LoadFile([]*vector.Vector{inVector}, proc)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldResemble, c.want)
		})
	}
}
