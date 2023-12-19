// Copyright 2021 - 2023 Matrix Origin
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

package moconnector

import (
	"testing"
)

func TestSQLConverter_Convert(t *testing.T) {
	//c := newSQLConverter("d1", "t1")
	//type testCase struct {
	//	obj RawObject
	//	sql string
	//}
	//var cases = []testCase{
	//	{
	//		obj: map[string]any{"k1": "v1"},
	//		sql: "INSERT INTO d1.t1 (k1) VALUES('v1')",
	//	},
	//	{
	//		obj: map[string]any{"k1": "v1", "k2": 20},
	//		sql: "INSERT INTO d1.t1 (k1, k2) VALUES('v1', 20)",
	//	},
	//	{
	//		obj: map[string]any{"k1": "v1", "k2": 20.3},
	//		sql: "INSERT INTO d1.t1 (k1, k2) VALUES('v1', 20.3)",
	//	},
	//	{
	//		obj: map[string]any{"k1": "v1", "k2": 20.3, "k3": false},
	//		sql: "INSERT INTO d1.t1 (k1, k2, k3) VALUES('v1', 20.3, 0)",
	//	},
	//}
	//
	//for _, ca := range cases {
	//	_, err := c.Convert(context.Background(), ca.obj)
	//	assert.NoError(t, err)
	//}
}
