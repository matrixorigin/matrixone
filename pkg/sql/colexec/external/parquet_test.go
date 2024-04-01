// Copyright 2024 Matrix Origin
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

package external

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/smartystreets/goconvey/convey"
)

type Contact struct {
	ID   int64  `parquet:"id"`
	Name string `parquet:"name"`
}

func buildFile() *bytes.Buffer {
	buf := bytes.Buffer{}
	writer := parquet.NewWriter(&buf)
	data := [][]Contact{
		{
			{ID: 1, Name: "user1"},
			{ID: 2, Name: "user2"},
			{ID: 7, Name: "user7"},
		},
		{
			{ID: 8, Name: "user8"},
			{ID: 10, Name: "user10"},
			{ID: 12, Name: "user12"},
		},
		{
			{ID: 15, Name: "user15"},
			{ID: 16, Name: "user16"},
		},
	}
	for _, rows := range data {
		for _, row := range rows {
			err := writer.Write(&row)
			if err != nil {
				panic(err)
			}
		}
		err := writer.Flush()
		if err != nil {
			panic(err)
		}
	}
	err := writer.Close()
	if err != nil {
		panic(err)
	}
	return &buf
}

func Test_makeVecFromPage(t *testing.T) {
	convey.Convey("makeVecFromPage succ", t, func() {
		proc := testutil.NewProc()

		tests := []struct {
			typ       parquet.Type
			numValues int
			values    encoding.Values
			vType     types.T
		}{
			{
				typ:       parquet.BooleanType,
				numValues: 2,
				values:    encoding.BooleanValues([]byte{2}),
				vType:     types.T_bool,
			},
			{
				typ:       parquet.Int32Type,
				numValues: 2,
				values:    encoding.Int32Values([]int32{1, 5}),
				vType:     types.T_int32,
			},
			{
				typ:       parquet.Int64Type,
				numValues: 2,
				values:    encoding.Int64Values([]int64{2, 7}),
				vType:     types.T_int64,
			},
			// {
			// 	typ: parquet.Int96Type,
			// },
			{
				typ:       parquet.FloatType,
				numValues: 2,
				values:    encoding.FloatValues([]float32{7.5, 3.2}),
				vType:     types.T_float32,
			},
			{
				typ:       parquet.DoubleType,
				numValues: 2,
				values:    encoding.DoubleValues([]float64{77.9, 0}),
				vType:     types.T_float64,
			},
			{
				typ:       parquet.ByteArrayType,
				numValues: 2,
				values:    encoding.ByteArrayValues([]byte("abcdefg"), []uint32{0, 3, 7}),
				vType:     types.T_varchar,
			},
			{
				typ:       parquet.FixedLenByteArrayType(3),
				numValues: 2,
				values:    encoding.FixedLenByteArrayValues([]byte("abcdef"), 3),
				vType:     types.T_char,
			},
		}
		for _, tc := range tests {
			page := tc.typ.NewPage(0, tc.numValues, tc.values)
			vec := proc.GetVector(types.New(tc.vType, 0, 0))

			err := makeVecFromPage(page, proc, vec)
			convey.So(err, convey.ShouldBeNil)

			t.Log(vec.String())
		}

		// 	data := buildFile()

		// 	pr, err := parquet.OpenFile(bytes.NewReader(data.Bytes()), int64(data.Len()))
		// 	convey.So(err, convey.ShouldBeNil)

		// 	name := pr.Root().Column("id")

		// 	proc := testutil.NewProc()
		// 	vec := proc.GetVector(types.New(types.T_int64, 0, 0))

		// 	pages := name.Pages()
		// L:
		// 	for {
		// 		page, err := pages.ReadPage()
		// 		switch {
		// 		case errors.Is(err, io.EOF):
		// 			break L
		// 		case err != nil:
		// 			convey.So(err, convey.ShouldBeNil)
		// 		}

		// 		err = makeVecFromPage(page, proc, vec)
		// 		convey.So(err, convey.ShouldBeNil)
		// 	}
	})
}
