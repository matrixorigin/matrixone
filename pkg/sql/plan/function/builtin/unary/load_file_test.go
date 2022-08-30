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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestLoadFile(t *testing.T) {
	dir := t.TempDir()
	proc := testutil.NewProc()
	fs := proc.FileService
	// fs := testutil.NewFS()
	ctx := context.Background()
	filepath := dir + "test"
	err := fs.Write(ctx, fileservice.IOVector{
		FilePath: filepath,
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

	convey.Convey("FileCase", t, func() {
		cases := []struct {
			filename string
			want     []byte
		}{
			{
				filename: filepath,
				want:     []byte("12345678"),
			},
		}
		var inStrs []string
		for _, k := range cases {
			inStrs = append(inStrs, k.filename)
		}
		inVector := testutil.MakeVarcharVector(inStrs, nil)
		res, err := LoadFile([]*vector.Vector{inVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		convey.So(res.Data, convey.ShouldResemble, []byte("12345678"))

	})

	// Test Error
	size := 1024 * 1024 * 1
	data := make([]byte, size)
	filepath = dir + "bigfile"
	err = fs.Write(ctx, fileservice.IOVector{
		FilePath: filepath,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   size,
				Data:   data,
			},
		},
	})
	assert.Nil(t, err)
	// bigf, err := os.CreateTemp("", "bigfile.bin")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer os.Remove(bigf.Name())
	// if err := bigf.Truncate(size); err != nil {
	// 	t.Fatal(err)
	// }
	convey.Convey("ErrorCase", t, func() {
		cases := []struct {
			filename string
			want     error
		}{
			{
				filename: filepath,
				want:     errors.New("Data too long for blob"),
			},
		}
		var inStrs []string
		for _, k := range cases {
			inStrs = append(inStrs, k.filename)
		}
		inVector := testutil.MakeVarcharVector(inStrs, nil)
		_, err := LoadFile([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldNotBeNil)
	})

}
