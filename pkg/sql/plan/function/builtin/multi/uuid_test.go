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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

// test return multi line
func TestUUID(t *testing.T) {
	//scalar
	vec := testutil.MakeScalarNull(5)
	proc := testutil.NewProc()
	res, err := UUID([]*vector.Vector{vec}, proc)
	if err != nil {
		t.Fatal(err)
	}

	bytes := res.Col.(*types.Bytes)
	uuids := make([]string, 5)

	for i := 0; i < 5; i++ {
		offset := bytes.Offsets[i]
		length := bytes.Lengths[i]
		bytes := bytes.Data[offset : offset+length]
		uuid := string(bytes)
		uuids[i] = uuid
		t.Log(uuid)
	}

	for i := 0; i < 5; i++ {
		for j := 0; j < 5; j++ {
			if i == j {
				continue
			}
			require.NotEqual(t, uuids[i], uuids[j])
		}
	}
}

// test retuan one line
func TestUUID2(t *testing.T) {
	//scalar
	vec := testutil.MakeScalarInt64(1, 1)
	proc := testutil.NewProc()
	res, err := UUID([]*vector.Vector{vec}, proc)
	if err != nil {
		t.Fatal(err)
	}

	bytes := res.Col.(*types.Bytes)
	uuids := make([]string, 1)

	for i := 0; i < 1; i++ {
		offset := bytes.Offsets[i]
		length := bytes.Lengths[i]
		bytes := bytes.Data[offset : offset+length]
		uuid := string(bytes)
		uuids[i] = uuid
		t.Log(uuid)
	}
}
