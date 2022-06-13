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

package unary

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLength(t *testing.T) {
	makeTempVector := func(src string, t types.T, srcIsScalar bool) []*vector.Vector {
		vectors := make([]*vector.Vector, 1)
		vectors[0] = &vector.Vector{
			Col: &types.Bytes{
				Data:    []byte(src),
				Offsets: []uint32{0},
				Lengths: []uint32{uint32(len(src))},
			},
			Nsp:     &nulls.Nulls{},
			Typ:     types.Type{Oid: t, Size: 24},
			IsConst: srcIsScalar,
			Length:  1,
		}
		return vectors
	}

	makeProcess := func() *process.Process {
		hm := host.New(1 << 40)
		gm := guest.New(1<<40, hm)
		return process.New(mheap.New(gm))
	}
	procs := makeProcess()

	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantBytes  []int64
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVector("abcdefghijklm", types.T_varchar, true),
			proc:       procs,
			wantBytes:  []int64{13},
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVector("abcdefghijklm", types.T_char, true),
			proc:       procs,
			wantBytes:  []int64{13},
			wantScalar: true,
		},
		{
			name:       "Test03",
			vecs:       makeTempVector("abcdefghijklm", types.T_varchar, false),
			proc:       procs,
			wantBytes:  []int64{13},
			wantScalar: false,
		},
		{
			name:       "Test04",
			vecs:       makeTempVector("abcdefghijklm", types.T_char, false),
			proc:       procs,
			wantBytes:  []int64{13},
			wantScalar: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			lengthRes, err := Length(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, lengthRes.Col)
			require.Equal(t, c.wantScalar, lengthRes.IsScalar())

		})
	}
}
