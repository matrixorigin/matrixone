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

package buffer

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const (
	NCPU  = 100
	Loop  = 10
	Query = 10
)

func TestBuffer(t *testing.T) {
	buf := New()
	for i := 0; i < Loop; i++ {
		n := Alloc[plan.Node](buf)
		n.Limit = Alloc[plan.Expr](buf)
		Free(buf, n.Limit)
		Free(buf, n)
	}
	for i := 0; i < Loop; i++ {
		ss := MakeSlice[*plan.Node](buf, 0, Query)
		FreeSlice(buf, ss)
	}
	buf.Free()
}

func BenchmarkBuffer(b *testing.B) {
	var wg sync.WaitGroup

	buf := New()
	for j := 0; j < NCPU; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			qry := make([]*plan.Node, 0, Query)
			for i := 0; i < Loop; i++ {
				qry = qry[:0]
				for k := 0; k < Query; k++ {
					n := Alloc[plan.Node](buf)
					n.Limit = Alloc[plan.Expr](buf)
					qry = append(qry, n)
				}
				for k := 0; k < Query; k++ {
					Free(buf, qry[k].Limit)
					Free(buf, qry[k])
				}
			}
		}()
	}
	wg.Wait()
	buf.Free()
}

func BenchmarkNew(b *testing.B) {
	var wg sync.WaitGroup

	for j := 0; j < NCPU; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			qry := make([]*plan.Node, 0, Query)
			for i := 0; i < Loop; i++ {
				qry = qry[:0]
				for k := 0; k < Query; k++ {
					n := new(plan.Node)
					n.Limit = new(plan.Expr)
					qry = append(qry, n)
				}
			}
		}()
	}
	wg.Wait()
}
