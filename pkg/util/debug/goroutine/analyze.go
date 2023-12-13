// Copyright 2023 Matrix Origin
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

package goroutine

import (
	"bytes"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/util/profile"
)

var (
	defaultAnalyzer = &analyzer{pools: map[string]int{}}
)

func init() {
	defaultAnalyzer.addGoroutinePool("created by github.com/panjf2000/ants/v2.(*goWorker).run", 2)
	defaultAnalyzer.addGoroutinePool("created by github.com/matrixorigin/matrixone/pkg/common/stopper.(*Stopper).doRunCancelableTask", 2)
	defaultAnalyzer.addGoroutinePool("created by github.com/lni/goutils/syncutil.(*Stopper).runWorker", 2)
}

func GetAnalyzer() *analyzer {
	return defaultAnalyzer
}

type analyzer struct {
	pools map[string]int
}

func (z *analyzer) ParseSystem() []Goroutine {
	var buf bytes.Buffer
	if err := profile.ProfileGoroutine(&buf, 2); err != nil {
		panic("impossible")
	}
	return z.Parse(buf.Bytes())
}

func (z *analyzer) Parse(data []byte) []Goroutine {
	gs := parse(data)
	for i := range gs {
		if v, ok := z.pools[gs[i].Last()]; ok {
			gs[i].realFuncLevel = v
		}
	}
	return gs
}

func (z *analyzer) GroupAnalyze(gs []Goroutine) AnalyzeResult {
	result := AnalyzeResult{
		Goroutines: gs,
	}
	result.createGroups = z.group(
		gs,
		nil,
		func(g Goroutine) string {
			v, _ := g.CreateBy()
			return v
		})
	for _, values := range result.createGroups {
		groups := z.group(
			gs,
			values,
			func(g Goroutine) string {
				return g.files[0]
			})
		result.firstMethodGroups = append(result.firstMethodGroups, groups)
	}
	return result
}

func (z *analyzer) group(
	gs []Goroutine,
	indexes []int,
	fn func(Goroutine) string) [][]int {
	var groups [][]int
	find := func(c string) int {
		for i := range groups {
			m := fn(gs[groups[i][0]])
			if m == c {
				return i
			}
		}
		return -1
	}
	handle := func(i int) {
		c := fn(gs[i])
		idx := find(c)
		if idx == -1 {
			groups = append(groups, []int{i})
			return
		}
		groups[idx] = append(groups[idx], i)
	}

	if len(indexes) == 0 {
		for i := range gs {
			handle(i)
		}
	} else {
		for _, i := range indexes {
			handle(i)
		}
	}
	sort.Slice(groups, func(i, j int) bool {
		return len(groups[i]) > len(groups[j])
	})
	return groups
}

func (z *analyzer) addGoroutinePool(
	name string,
	realFuncLevel int) {
	z.pools[name] = realFuncLevel
}
