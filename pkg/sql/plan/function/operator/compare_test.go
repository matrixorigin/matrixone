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

package operator

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
)

func TestEqUuid(t *testing.T) {
	convey.Convey("test uuid equal", t, func() {
		cases := []struct {
			left   string
			right  string
			result bool
		}{
			{
				//name:  "test01",
				left:  "0d5687da-2a67-11ed-99e0-000c29847904",
				right: "0d5687da-2a67-11ed-99e0-000c29847904",
				// 0
				result: true,
			},
			{
				//name:  "test02",
				left:  "0d5687da-2a67-11ed-99e0-000c29847904",
				right: "6119dffd-2a6b-11ed-99e0-000c29847904",
				// -1
				result: false,
			},
			{
				//name:  "test03",
				left:  "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				right: "04ddf8bf-2a8a-11ed-99e0-000c29847904",
				// -1
				result: false,
			},
			{
				//name:  "test04",
				left:  "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				right: "0d568802-2a67-11ed-99e0-000c29847904",
				// -1
				result: false,
			},
			{
				//name:  "test05",
				left:  "6d1b1fd4-2dbf-11ed-940f-000c29847904",
				right: "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				// 1
				result: false,
			},
			{
				//name:  "test06",
				left:  "6d1b1fd4-2dbf-11ed-940f-000c29847904",
				right: "6d1b1fd4-2dbf-11ed-940f-000c29847904",
				// 0
				result: true,
			},
		}

		var lefts []string
		var rights []string
		var wants []bool
		for _, c := range cases {
			lefts = append(lefts, c.left)
			rights = append(rights, c.right)
			wants = append(wants, c.result)
		}

		leftVector := testutil.MakeUuidVectorByString(lefts, nil)
		rightVector := testutil.MakeUuidVectorByString(rights, nil)
		wantVector := testutil.MakeBoolVector(wants)

		proc := testutil.NewProc()
		actVector, err := EqUuid([]*vector.Vector{leftVector, rightVector}, proc)
		convey.ShouldBeNil(err)
		compare := testutil.CompareVectors(wantVector, actVector)
		convey.ShouldBeTrue(compare)
		t.Logf("want slice: %+v \n", vector.MustFixedCol[bool](wantVector))
		t.Logf("actu slice: %+v \n", vector.MustFixedCol[bool](actVector))
		require.Equal(t, vector.MustFixedCol[bool](wantVector), vector.MustFixedCol[bool](actVector))
	})

}

func TestLeUuid(t *testing.T) {
	convey.Convey("test uuid less equal", t, func() {
		cases := []struct {
			left   string
			right  string
			result bool
		}{
			{
				//name:  "test01",
				left:  "0d5687da-2a67-11ed-99e0-000c29847904",
				right: "0d5687da-2a67-11ed-99e0-000c29847904",
				// 0
				result: true,
			},
			{
				//name:  "test02",
				left:  "0d5687da-2a67-11ed-99e0-000c29847904",
				right: "6119dffd-2a6b-11ed-99e0-000c29847904",
				// -1
				result: true,
			},
			{
				//name:  "test03",
				left:  "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				right: "04ddf8bf-2a8a-11ed-99e0-000c29847904",
				// -1
				result: true,
			},
			{
				//name:  "test04",
				left:  "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				right: "0d568802-2a67-11ed-99e0-000c29847904",
				// -1
				result: true,
			},
			{
				//name:  "test05",
				left:  "6d1b1fd4-2dbf-11ed-940f-000c29847904",
				right: "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				// 1
				result: false,
			},
			{
				//name:  "test06",
				left:  "6d1b1fd4-2dbf-11ed-940f-000c29847904",
				right: "6d1b1fd4-2dbf-11ed-940f-000c29847904",
				// 0
				result: true,
			},
			{
				//name:  "test07",
				left:  "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				right: "0d568802-2a67-11ed-99e0-000c29847904",
				//-1,
				result: true,
			},
		}

		var lefts []string
		var rights []string
		var wants []bool
		for _, c := range cases {
			lefts = append(lefts, c.left)
			rights = append(rights, c.right)
			wants = append(wants, c.result)
		}

		leftVector := testutil.MakeUuidVectorByString(lefts, nil)
		rightVector := testutil.MakeUuidVectorByString(rights, nil)
		wantVector := testutil.MakeBoolVector(wants)

		proc := testutil.NewProc()
		actVector, err := LeUuid([]*vector.Vector{leftVector, rightVector}, proc)
		convey.ShouldBeNil(err)

		compare := testutil.CompareVectors(wantVector, actVector)
		convey.ShouldBeTrue(compare)
		t.Logf("want slice: %+v \n", vector.MustFixedCol[bool](wantVector))
		t.Logf("actu slice: %+v \n", vector.MustFixedCol[bool](actVector))
		require.Equal(t, vector.MustFixedCol[bool](wantVector), vector.MustFixedCol[bool](actVector))

	})
}

func TestLtUuid(t *testing.T) {
	convey.Convey("test uuid less than", t, func() {
		cases := []struct {
			left   string
			right  string
			result bool
		}{
			{
				//name:  "test01",
				left:  "0d5687da-2a67-11ed-99e0-000c29847904",
				right: "0d5687da-2a67-11ed-99e0-000c29847904",
				// 0
				result: false,
			},
			{
				//name:  "test02",
				left:  "0d5687da-2a67-11ed-99e0-000c29847904",
				right: "6119dffd-2a6b-11ed-99e0-000c29847904",
				// -1
				result: true,
			},
			{
				//name:  "test03",
				left:  "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				right: "04ddf8bf-2a8a-11ed-99e0-000c29847904",
				// -1
				result: true,
			},
			{
				//name:  "test04",
				left:  "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				right: "0d568802-2a67-11ed-99e0-000c29847904",
				// -1
				result: true,
			},
			{
				//name:  "test05",
				left:  "6d1b1fd4-2dbf-11ed-940f-000c29847904",
				right: "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				// 1
				result: false,
			},
			{
				//name:  "test06",
				left:  "6d1b1fd4-2dbf-11ed-940f-000c29847904",
				right: "6d1b1fd4-2dbf-11ed-940f-000c29847904",
				// 0
				result: false,
			},
			{
				//name:  "test07",
				left:  "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				right: "0d568802-2a67-11ed-99e0-000c29847904",
				//-1,
				result: true,
			},
		}

		var lefts []string
		var rights []string
		var wants []bool
		for _, c := range cases {
			lefts = append(lefts, c.left)
			rights = append(rights, c.right)
			wants = append(wants, c.result)
		}

		leftVector := testutil.MakeUuidVectorByString(lefts, nil)
		rightVector := testutil.MakeUuidVectorByString(rights, nil)
		wantVector := testutil.MakeBoolVector(wants)

		proc := testutil.NewProc()
		actVector, err := LtUuid([]*vector.Vector{leftVector, rightVector}, proc)
		convey.ShouldBeNil(err)

		compare := testutil.CompareVectors(wantVector, actVector)
		convey.ShouldBeTrue(compare)
		t.Logf("want slice: %+v \n", vector.MustFixedCol[bool](wantVector))
		t.Logf("actu slice: %+v \n", vector.MustFixedCol[bool](actVector))
		require.Equal(t, vector.MustFixedCol[bool](wantVector), vector.MustFixedCol[bool](actVector))

	})
}

func TestGeUuid(t *testing.T) {
	convey.Convey("test uuid great equal", t, func() {
		cases := []struct {
			left   string
			right  string
			result bool
		}{
			{
				//name:  "test01",
				left:  "0d5687da-2a67-11ed-99e0-000c29847904",
				right: "0d5687da-2a67-11ed-99e0-000c29847904",
				// 0
				result: true,
			},
			{
				//name:  "test02",
				left:  "0d5687da-2a67-11ed-99e0-000c29847904",
				right: "6119dffd-2a6b-11ed-99e0-000c29847904",
				// -1
				result: false,
			},
			{
				//name:  "test03",
				left:  "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				right: "04ddf8bf-2a8a-11ed-99e0-000c29847904",
				// -1
				result: false,
			},
			{
				//name:  "test04",
				left:  "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				right: "0d568802-2a67-11ed-99e0-000c29847904",
				// -1
				result: false,
			},
			{
				//name:  "test05",
				left:  "6d1b1fd4-2dbf-11ed-940f-000c29847904",
				right: "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				// 1
				result: true,
			},
			{
				//name:  "test06",
				left:  "6d1b1fd4-2dbf-11ed-940f-000c29847904",
				right: "6d1b1fd4-2dbf-11ed-940f-000c29847904",
				// 0
				result: true,
			},
			{
				//name:  "test07",
				left:  "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				right: "0d568802-2a67-11ed-99e0-000c29847904",
				//-1,
				result: false,
			},
		}

		var lefts []string
		var rights []string
		var wants []bool
		for _, c := range cases {
			lefts = append(lefts, c.left)
			rights = append(rights, c.right)
			wants = append(wants, c.result)
		}

		leftVector := testutil.MakeUuidVectorByString(lefts, nil)
		rightVector := testutil.MakeUuidVectorByString(rights, nil)
		wantVector := testutil.MakeBoolVector(wants)

		proc := testutil.NewProc()
		actVector, err := GeUuid([]*vector.Vector{leftVector, rightVector}, proc)
		convey.ShouldBeNil(err)

		compare := testutil.CompareVectors(wantVector, actVector)
		convey.ShouldBeTrue(compare)
		t.Logf("want slice: %+v \n", vector.MustFixedCol[bool](wantVector))
		t.Logf("actu slice: %+v \n", vector.MustFixedCol[bool](actVector))
		require.Equal(t, vector.MustFixedCol[bool](wantVector), vector.MustFixedCol[bool](actVector))
	})
}

func TestGtUuid(t *testing.T) {
	convey.Convey("test uuid great than", t, func() {
		cases := []struct {
			left   string
			right  string
			result bool
		}{
			{
				//name:  "test01",
				left:  "0d5687da-2a67-11ed-99e0-000c29847904",
				right: "0d5687da-2a67-11ed-99e0-000c29847904",
				// 0
				result: false,
			},
			{
				//name:  "test02",
				left:  "0d5687da-2a67-11ed-99e0-000c29847904",
				right: "6119dffd-2a6b-11ed-99e0-000c29847904",
				// -1
				result: false,
			},
			{
				//name:  "test03",
				left:  "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				right: "04ddf8bf-2a8a-11ed-99e0-000c29847904",
				// -1
				result: false,
			},
			{
				//name:  "test04",
				left:  "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				right: "0d568802-2a67-11ed-99e0-000c29847904",
				// -1
				result: false,
			},
			{
				//name:  "test05",
				left:  "6d1b1fd4-2dbf-11ed-940f-000c29847904",
				right: "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				// 1
				result: true,
			},
			{
				//name:  "test06",
				left:  "6d1b1fd4-2dbf-11ed-940f-000c29847904",
				right: "6d1b1fd4-2dbf-11ed-940f-000c29847904",
				// 0
				result: false,
			},
			{
				//name:  "test07",
				left:  "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				right: "0d568802-2a67-11ed-99e0-000c29847904",
				//-1,
				result: false,
			},
		}

		var lefts []string
		var rights []string
		var wants []bool
		for _, c := range cases {
			lefts = append(lefts, c.left)
			rights = append(rights, c.right)
			wants = append(wants, c.result)
		}

		leftVector := testutil.MakeUuidVectorByString(lefts, nil)
		rightVector := testutil.MakeUuidVectorByString(rights, nil)
		wantVector := testutil.MakeBoolVector(wants)

		proc := testutil.NewProc()
		actVector, err := GtUuid([]*vector.Vector{leftVector, rightVector}, proc)
		convey.ShouldBeNil(err)

		compare := testutil.CompareVectors(wantVector, actVector)
		convey.ShouldBeTrue(compare)
		t.Logf("want slice: %+v \n", vector.MustFixedCol[bool](wantVector))
		t.Logf("actu slice: %+v \n", vector.MustFixedCol[bool](actVector))
		require.Equal(t, vector.MustFixedCol[bool](wantVector), vector.MustFixedCol[bool](actVector))
	})
}

func TestNeUuid(t *testing.T) {
	convey.Convey("test uuid not equal", t, func() {
		cases := []struct {
			left   string
			right  string
			result bool
		}{
			{
				//name:  "test01",
				left:  "0d5687da-2a67-11ed-99e0-000c29847904",
				right: "0d5687da-2a67-11ed-99e0-000c29847904",
				// 0
				result: false,
			},
			{
				//name:  "test02",
				left:  "0d5687da-2a67-11ed-99e0-000c29847904",
				right: "6119dffd-2a6b-11ed-99e0-000c29847904",
				// -1
				result: true,
			},
			{
				//name:  "test03",
				left:  "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				right: "04ddf8bf-2a8a-11ed-99e0-000c29847904",
				// -1
				result: true,
			},
			{
				//name:  "test04",
				left:  "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				right: "0d568802-2a67-11ed-99e0-000c29847904",
				// -1
				result: true,
			},
			{
				//name:  "test05",
				left:  "6d1b1fd4-2dbf-11ed-940f-000c29847904",
				right: "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				// 1
				result: true,
			},
			{
				//name:  "test06",
				left:  "6d1b1fd4-2dbf-11ed-940f-000c29847904",
				right: "6d1b1fd4-2dbf-11ed-940f-000c29847904",
				// 0
				result: false,
			},
			{
				//name:  "test07",
				left:  "04ddf3f8-2a8a-11ed-99e0-000c29847904",
				right: "0d568802-2a67-11ed-99e0-000c29847904",
				//-1,
				result: true,
			},
		}

		var lefts []string
		var rights []string
		var wants []bool
		for _, c := range cases {
			lefts = append(lefts, c.left)
			rights = append(rights, c.right)
			wants = append(wants, c.result)
		}

		leftVector := testutil.MakeUuidVectorByString(lefts, nil)
		rightVector := testutil.MakeUuidVectorByString(rights, nil)
		wantVector := testutil.MakeBoolVector(wants)

		proc := testutil.NewProc()
		actVector, err := NeUuid([]*vector.Vector{leftVector, rightVector}, proc)
		convey.ShouldBeNil(err)

		compare := testutil.CompareVectors(wantVector, actVector)
		convey.ShouldBeTrue(compare)
		t.Logf("want slice: %+v \n", vector.MustFixedCol[bool](wantVector))
		t.Logf("actu slice: %+v \n", vector.MustFixedCol[bool](actVector))
		require.Equal(t, vector.MustFixedCol[bool](wantVector), vector.MustFixedCol[bool](actVector))
	})
}
