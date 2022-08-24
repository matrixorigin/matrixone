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
	"errors"
	"log"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/smartystreets/goconvey/convey"
)

func TestLoadFile(t *testing.T) {
	f, err := os.CreateTemp("", "testfile.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(f.Name())
	content := []byte("Test load_file function")
	if _, err = f.Write(content); err != nil {
		log.Fatal(err)
	}

	convey.Convey("FileCase", t, func() {
		cases := []struct {
			filename string
			want     []byte
		}{
			{
				filename: f.Name(),
				want:     content,
			},
		}
		var inStrs []string
		for _, k := range cases {
			inStrs = append(inStrs, k.filename)
		}
		inVector := testutil.MakeVarcharVector(inStrs, nil)
		proc := testutil.NewProc()
		res, err := LoadFile([]*vector.Vector{inVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		convey.So(res.Data, convey.ShouldResemble, content)

	})

	// Test Error
	size := int64(1024 * 1024 * 1)
	bigf, err := os.CreateTemp("", "bigfile.bin")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(bigf.Name())
	if err := bigf.Truncate(size); err != nil {
		t.Fatal(err)
	}
	convey.Convey("ErrorCase", t, func() {
		cases := []struct {
			filename string
			want     error
		}{
			{
				filename: bigf.Name(),
				want:     errors.New("Data too long for blob"),
			},
		}
		var inStrs []string
		for _, k := range cases {
			inStrs = append(inStrs, k.filename)
		}
		inVector := testutil.MakeVarcharVector(inStrs, nil)
		proc := testutil.NewProc()
		_, err := LoadFile([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldNotBeNil)
	})

}
