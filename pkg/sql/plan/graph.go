// Copyright 2021 Matrix Origin
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

package plan

func isEmptyGraph(gp *Graph) bool {
	return len(gp.Es) == 0
}

func cloneGraph(gp *Graph) *Graph {
	gq := new(Graph)
	gq.Es = make([]*Edge, len(gp.Es))
	for i, e := range gp.Es {
		gq.Es[i] = new(Edge)
		gq.Es[i].Vs = make([]int, len(e.Vs))
		copy(gq.Es[i].Vs, e.Vs)
	}
	return gq
}

func existIndependentEdge(gp *Graph) bool {
	for i, e := range gp.Es {
		if isIndependentEdge(i, e.Vs, gp.Es) {
			return true
		}
	}
	return false
}

func pruneGraph(gp *Graph) *Graph {
	var flg bool

	{ // remove isolated vertex
		mp := make(map[int]int)
		for _, e := range gp.Es {
			for _, v := range e.Vs {
				mp[v]++
			}
		}
		for _, e := range gp.Es {
			vs := make([]int, 0, len(e.Vs))
			for _, v := range e.Vs {
				if cnt, ok := mp[v]; ok && cnt > 1 {
					vs = append(vs, v)
				}
			}
			if len(vs) != len(e.Vs) {
				e.Vs = vs
				flg = true
			}
		}
	}
	{
		for i := 0; i < len(gp.Es); i++ {
			if len(gp.Es[i].Vs) == 0 {
				gp.Es = append(gp.Es[:i], gp.Es[i+1:]...)
				i--
				continue
			}
		}
	}
	{
		for i := 0; i < len(gp.Es); i++ {
			for j := 0; j < len(gp.Es); j++ {
				if i == j {
					continue
				}
				if isCoverByEdge(gp.Es[i].Vs, gp.Es[j].Vs) {
					flg = true
					gp.Es = append(gp.Es[:i], gp.Es[i+1:]...)
					i--
					break
				}
			}
		}
	}
	if flg && !isEmptyGraph(gp) {
		return pruneGraph(gp)
	}
	return gp
}

func (vp *VertexSet) Contains(j int) bool {
	for _, i := range vp.Is {
		if i == j {
			return true
		}
	}
	return false
}

func isIndependentEdge(j int, vs []int, es []*Edge) bool {
	for i, e := range es {
		if i == j {
			continue
		}
		if isConnected(vs, e.Vs) {
			return false
		}
	}
	return true
}

func weight(vs, ws []int) int {
	var w int

	mp := make(map[int]int)
	for _, v := range ws {
		mp[v]++
	}
	for _, v := range vs {
		if _, ok := mp[v]; ok {
			w++
		}
	}
	return w
}

func isConnected(vs, ws []int) bool {
	mp := make(map[int]int)
	for _, w := range ws {
		mp[w]++
	}
	for _, v := range vs {
		if _, ok := mp[v]; ok {
			return true
		}
	}
	return false
}

func isCoverByEdge(vs, ws []int) bool {
	mp := make(map[int]int)
	for _, w := range ws {
		mp[w]++
	}
	for _, v := range vs {
		if _, ok := mp[v]; !ok {
			return false
		}
	}
	return true
}

func connectedVertexs(vs, ws []int) []int {
	rs := make([]int, 0, len(vs))
	for _, v := range vs {
		for _, w := range ws {
			if v == w {
				rs = append(rs, v)
			}
		}
	}
	return rs
}

func getEdgesCount(i int, ess []*EdgeSet) int {
	var cnt int

	for _, es := range ess {
		if es.I1 == i || es.I2 == i {
			cnt++
		}
	}
	return cnt
}

func buildVertexSet(es []*Edge) *VertexSet {
	vp := &VertexSet{
		Es: make([]*Edge, len(es)),
	}
	for i, e := range es {
		vp.Es[i] = e
	}
	return vp
}

func buildEdgeSet(es []*Edge) []*EdgeSet {
	var ess []*EdgeSet

	for i, e := range es {
		for j := range es {
			if i == j {
				continue
			}
			if w := weight(e.Vs, es[j].Vs); w > 0 {
				ess = append(ess, &EdgeSet{
					W:  w,
					I1: i,
					I2: j,
					E1: es[i],
					E2: es[j],
				})
			}
		}
	}
	return ess
}

func isEndEdgeSet(es *EdgeSet, ess []*EdgeSet) bool {
	for i := range ess {
		if es.I2 == ess[i].I1 {
			return false
		}
	}
	return true
}
