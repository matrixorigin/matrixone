// Copyright 2021 - 2022 Matrix Origin
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

package types

import (
	"fmt"
	"testing"

	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
)

func TestParseUuid(t *testing.T) {
	cases := []struct {
		name   string
		str    string
		expect Uuid
	}{
		{
			name:   "test01",
			str:    "0d5687da-2a67-11ed-99e0-000c29847904",
			expect: Uuid{13, 86, 135, 218, 42, 103, 17, 237, 153, 224, 0, 12, 41, 132, 121, 4},
		},
		{
			name:   "test02",
			str:    "6119dffd-2a6b-11ed-99e0-000c29847904",
			expect: Uuid{97, 25, 223, 253, 42, 107, 17, 237, 153, 224, 0, 12, 41, 132, 121, 4},
		},
		{
			name:   "test03",
			str:    "3e350a5c-222a-11eb-abef-0242ac110002",
			expect: Uuid{62, 53, 10, 92, 34, 42, 17, 235, 171, 239, 2, 66, 172, 17, 0, 2},
		},
		{
			name:   "test04",
			str:    "3e350a5c222a11ebabef0242ac110002",
			expect: Uuid{62, 53, 10, 92, 34, 42, 17, 235, 171, 239, 2, 66, 172, 17, 0, 2},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			uuid, err := ParseUuid(c.str)
			t.Log(uuid.ToString())
			if err != nil {
				t.Error(err)
			}
			fmt.Printf("%+v", uuid)
			require.Equal(t, uuid, c.expect)
		})
	}

	errcases := []struct {
		name string
		str  string
	}{
		{
			name: "err01",
			str:  "6119dffd-2a6b-11ed-99e0-000c298479043",
		},
		{
			name: "err02",
			str:  "6119dffd-2a6b-11e-99e0-000c298479043",
		},
		{
			name: "err03",
			str:  "6119dffd+2a6b-11ed-99e0-000c29847904",
		},
		{
			name: "err04",
			str:  "3e350a5c222a11ebabef0242ac110002d",
		},
	}

	for _, errcase := range errcases {
		t.Run(errcase.name, func(t *testing.T) {
			_, err := ParseUuid(errcase.str)
			if err == nil {
				t.Fail()
				convey.ShouldNotBeNil(err)
			}
			t.Log(err)
		})
	}
}

func TestEqualUuid(t *testing.T) {
	cases := []struct {
		name   string
		left   string
		right  string
		expect bool
	}{
		{
			name:   "test01",
			left:   "0d5687da-2a67-11ed-99e0-000c29847904",
			right:  "0d5687da-2a67-11ed-99e0-000c29847904",
			expect: true,
		},
		{
			name:   "test02",
			left:   "0d5687da-2a67-11ed-99e0-000c29847904",
			right:  "6119dffd-2a6b-11ed-99e0-000c29847904",
			expect: false,
		},
		{
			name:   "test03",
			left:   "04ddf3f8-2a8a-11ed-99e0-000c29847904",
			right:  "04ddf8bf-2a8a-11ed-99e0-000c29847904",
			expect: false,
		},
		{
			name:   "test03",
			left:   "04ddf3f8-2a8a-11ed-99e0-000c29847904",
			right:  "0d568802-2a67-11ed-99e0-000c29847904",
			expect: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			left, err := ParseUuid(c.left)
			if err != nil {
				t.Error(err)
			}
			right, err := ParseUuid(c.right)
			if err != nil {
				t.Error(err)
			}
			require.Equal(t, EqualUuid(left, right), c.expect)
		})
	}

}

func TestCompareUuid(t *testing.T) {
	cases := []struct {
		name   string
		left   string
		right  string
		expect int
	}{
		{
			name:   "test01",
			left:   "0d5687da-2a67-11ed-99e0-000c29847904",
			right:  "0d5687da-2a67-11ed-99e0-000c29847904",
			expect: 0,
		},
		{
			name:   "test02",
			left:   "0d5687da-2a67-11ed-99e0-000c29847904",
			right:  "6119dffd-2a6b-11ed-99e0-000c29847904",
			expect: -1,
		},
		{
			name:   "test03",
			left:   "04ddf3f8-2a8a-11ed-99e0-000c29847904",
			right:  "04ddf8bf-2a8a-11ed-99e0-000c29847904",
			expect: -1,
		},
		{
			name:   "test03",
			left:   "6d1b1fd5-2dbf-11ed-940f-000c29847904",
			right:  "0d568802-2a67-11ed-99e0-000c29847904",
			expect: 1,
		},
		{
			name:   "test04",
			left:   "04ddf3f8-2a8a-11ed-99e0-000c29847904",
			right:  "0d568802-2a67-11ed-99e0-000c29847904",
			expect: -1,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			left, err := ParseUuid(c.left)
			if err != nil {
				t.Error(err)
			}
			right, err := ParseUuid(c.right)
			if err != nil {
				t.Error(err)
			}
			require.Equal(t, CompareUuid(left, right), c.expect)
		})
	}
}

func TestCompareLeUuid(t *testing.T) {
	cases := []struct {
		name   string
		left   string
		right  string
		expect bool
	}{
		{
			name:   "test01",
			left:   "16355110-2d0c-11ed-940f-000c29847904",
			right:  "f6355110-2d0c-11ed-940f-000c29847904",
			expect: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			left, err := ParseUuid(c.left)
			if err != nil {
				t.Error(err)
			}
			t.Logf("left [16]byte: %+v\n", left)
			right, err := ParseUuid(c.right)
			if err != nil {
				t.Error(err)
			}
			t.Logf("right [16]byte: %+v\n", right)
			require.Equal(t, left.Lt(right), c.expect)
		})
	}
}
