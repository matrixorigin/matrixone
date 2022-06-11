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

func TestDateToDate(t *testing.T) {
	makeTempVectors := func(value types.Date, paramIsScalar bool) []*vector.Vector {
		vectors := make([]*vector.Vector, 1)
		vectors[0] = &vector.Vector{
			Col:     []types.Date{value},
			Nsp:     &nulls.Nulls{},
			IsConst: paramIsScalar,
			Length:  1,
		}
		return vectors
	}

	proce := func() *process.Process {
		hm := host.New(1 << 40)
		gm := guest.New(1<<40, hm)
		return process.New(mheap.New(gm))
	}()

	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantValue  []types.Date
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVectors(types.Date(729848), true),
			proc:       proce,
			wantValue:  []types.Date{729848},
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVectors(types.Date(729848), false),
			proc:       proce,
			wantValue:  []types.Date{729848},
			wantScalar: false,
		},
	}
	for _, c := range cases {
		res, err := DateToDate(c.vecs, c.proc)
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, c.wantValue, res.Col)
		require.Equal(t, c.wantScalar, res.IsScalar())
	}

}

func TestDatetimeToDate(t *testing.T) {
	makeTempVectors := func(value types.Datetime, paramIsScalar bool) []*vector.Vector {
		vectors := make([]*vector.Vector, 1)
		vectors[0] = &vector.Vector{
			Col:     []types.Datetime{value},
			Nsp:     &nulls.Nulls{},
			IsConst: paramIsScalar,
			Length:  1,
		}
		return vectors
	}

	proce := func() *process.Process {
		hm := host.New(1 << 40)
		gm := guest.New(1<<40, hm)
		return process.New(mheap.New(gm))
	}()

	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantValue  []types.Date
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVectors(types.Datetime(66122056321728512), true),
			proc:       proce,
			wantValue:  []types.Date{729848},
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVectors(types.Datetime(66122056321728512), false),
			proc:       proce,
			wantValue:  []types.Date{729848},
			wantScalar: false,
		},
	}
	for _, c := range cases {
		res, err := DatetimeToDate(c.vecs, c.proc)
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, c.wantValue, res.Col)
		require.Equal(t, c.wantScalar, res.IsScalar())
	}
}

func TestDateNull(t *testing.T) {

}
