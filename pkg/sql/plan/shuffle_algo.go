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

package plan

import (
	"math"
)

const DefaultEvalSize = 1024

type shuffleHeap struct {
	left    *shuffleHeap
	right   *shuffleHeap
	key     float64
	value   float64
	height  int
	size    int
	nulls   int
	reverse bool
}

type shuffleList struct {
	size  int
	value float64
	next  *shuffleList
	tree  *shuffleHeap
}

type ShuffleRange struct {
	isStrType bool
	size      int
	tree      *shuffleHeap
	min       float64
	max       float64
	mins      [][]byte
	maxs      [][]byte
	rows      []int
	nulls     []int
	maxlen    int
	flags     []bool
	Overlap   float64
	Uniform   float64
	Result    []float64
}

func (t *shuffleHeap) Merge(s *shuffleHeap) *shuffleHeap {
	if t.key > s.key != t.reverse {
		if s.right == nil {
			s.right = t
		} else {
			s.right = t.Merge(s.right)
		}
		if s.left == nil || s.left.height < s.right.height {
			tmp := s.left
			s.left = s.right
			s.right = tmp
		}
		s.height = s.left.height + 1
		return s
	} else {
		if t.right == nil {
			t.right = s
		} else {
			t.right = t.right.Merge(s)
		}
		if t.left == nil || t.left.height < t.right.height {
			tmp := t.left
			t.left = t.right
			t.right = tmp
		}
		t.height = t.left.height + 1
		return t
	}
}

func (t *shuffleHeap) Pop() (*shuffleHeap, *shuffleHeap) {
	if t.left == nil {
		return nil, t
	}
	if t.right == nil {
		return t.left, t
	}
	return t.left.Merge(t.right), t
}

func NewShuffleRange(isString bool) *ShuffleRange {
	return &ShuffleRange{isStrType: isString}
}
func (s *ShuffleRange) UpdateString(zmmin []byte, zmmax []byte, rowCount uint32, nullCount uint32) {
	if len(zmmin) > 8 {
		zmmin = zmmin[:8]
	}
	if len(zmmax) > 8 {
		zmmax = zmmax[:8]
	}
	if s.size == 0 {
		s.size = int(rowCount)
		s.flags = make([]bool, 256)
		s.mins = make([][]byte, 0)
		s.maxs = make([][]byte, 0)
		s.mins = append(s.mins, zmmin)
		s.maxs = append(s.maxs, zmmax)
		s.rows = make([]int, 0)
		s.rows = append(s.rows, int(rowCount))
		s.nulls = make([]int, 0)
		s.nulls = append(s.nulls, int(nullCount))
	} else {
		s.size += int(rowCount)
		s.mins = append(s.mins, zmmin)
		s.maxs = append(s.maxs, zmmax)
		s.rows = append(s.rows, int(rowCount))
		s.nulls = append(s.nulls, int(nullCount))
	}
	if s.maxlen < len(zmmin) {
		s.maxlen = len(zmmin)
	}
	for _, c := range zmmin {
		s.flags[int(c)] = true
	}
	if s.maxlen < len(zmmax) {
		s.maxlen = len(zmmax)
	}
	for _, c := range zmmax {
		s.flags[int(c)] = true
	}
}

func (s *ShuffleRange) Update(zmmin float64, zmmax float64, rowCount uint32, nullCount uint32) {
	s.size += int(rowCount)
	if s.tree == nil {
		s.tree = &shuffleHeap{
			height: 1,
			key:    zmmax,
			value:  zmmin,
			size:   int(rowCount),
			nulls:  int(nullCount),
		}
		s.min = zmmin
		s.max = zmmax
	} else {
		s.tree = s.tree.Merge(&shuffleHeap{
			height: 1,
			key:    zmmax,
			value:  zmmin,
			size:   int(rowCount),
			nulls:  int(nullCount),
		})
		if s.min > zmmin {
			s.min = zmmin
		}
		if s.max < zmmax {
			s.max = zmmax
		}
	}

}

func (s *ShuffleRange) Eval() {
	k := DefaultEvalSize
	if s.size == 0 {
		return
	}
	bytetoint := make(map[byte]int)
	inttobyte := make([]byte, 0)
	var lens float64
	if s.isStrType {
		for i := 0; i < 256; i++ {
			if s.flags[i] {
				bytetoint[byte(i)] = len(inttobyte)
				inttobyte = append(inttobyte, byte(i))
			}
		}
		if len(inttobyte) == 0 {
			return
		}
		lens = float64(len(inttobyte))
		for i := range s.mins {
			node := &shuffleHeap{
				height: 1,
				key:    0,
				value:  0,
				size:   s.rows[i],
				nulls:  s.nulls[i],
			}
			for _, c := range s.maxs[i] {
				node.key = node.key*lens + float64(bytetoint[c])
			}
			for j := len(s.maxs[i]); j < s.maxlen; j++ {
				node.key = node.key * lens
			}
			for _, c := range s.mins[i] {
				node.value = node.value*lens + float64(bytetoint[c])
			}
			for j := len(s.mins[i]); j < s.maxlen; j++ {
				node.value = node.value * lens
			}
			if s.tree == nil {
				s.tree = node
			} else {
				s.tree = s.tree.Merge(node)
			}
		}
	}
	var head *shuffleList
	var node *shuffleHeap
	var nulls int
	s.Result = make([]float64, k-1)
	for s.tree != nil {
		s.tree, node = s.tree.Pop()
		node.left = nil
		node.right = nil
		node.height = 1
		node.size -= node.nulls
		nulls += node.nulls
		node.reverse = true
		head = &shuffleList{
			next:  head,
			tree:  node,
			size:  node.size,
			value: node.value,
		}
		if head.next != nil {
			for head.next != nil {
				next := head.next
				if head.tree.value >= next.tree.key {
					break
				}
				if head.tree.key != head.value {
					if head.value <= next.value {
						s.Overlap += float64(head.size) * float64(next.size) * (next.tree.key - next.value) / (head.tree.key - head.value)
					} else {
						s.Overlap += float64(head.size) * float64(next.size) * (next.tree.key - head.value) * (next.tree.key - head.value) / (head.tree.key - head.value) / (next.tree.key - next.value)
						head.value = next.value
					}
				}
				head.tree = head.tree.Merge(next.tree)
				head.size += next.size
				head.next = next.next
			}

		}
	}
	s.Overlap /= float64(s.size) * float64(s.size)
	s.Overlap = math.Sqrt(s.Overlap)

	step := float64(s.size) / float64(k)
	if float64(nulls) >= step {
		step = float64(s.size-nulls) / float64(k-1)
	}
	last := step
	k -= 2
	s.Uniform = float64(s.size) / (s.max - s.min)
	for {
		if head == nil {
			for i := 0; i <= k; i++ {
				s.Result[k-i] = s.min
			}
			break
		}
		size := float64(head.size)
		var valuetree *shuffleHeap
		var speed float64
		now := head.tree.key
		for {
			if valuetree == nil || (head.tree != nil && valuetree.key < head.tree.key) {
				if head.tree == nil {
					break
				}
				head.tree, node = head.tree.Pop()
				delta := speed * (now - node.key)
				last -= delta
				size -= delta
				for last <= 0 {
					s.Result[k] = node.key - (last/delta)*(now-node.key)
					if s.Result[k] != s.Result[k] {
						s.Result[k] = node.key
					}
					last += step
					k--
					if k < 0 || last > size {
						break
					}

				}
				if k < 0 {
					break
				}
				now = node.key
				if node.key-node.value < 0.1 {
					last -= float64(node.size)
					size -= float64(node.size)
					if last <= 0 {
						if -last <= last+float64(node.size) {
							s.Result[k] = now
							last = step
							k--
							if k < 0 {
								break
							}
						} else {
							s.Result[k] = now + 1
							last = step - float64(node.size)
							k--
							if k < 0 {
								break
							}
							if last <= 0 {
								s.Result[k] = now
								last = step
								k--
								if k < 0 {
									break
								}
							}
						}

					}
					continue
				}
				speed += float64(node.size) / (node.key - node.value)
				if s.Uniform < speed {
					s.Uniform = speed
				}
				node.left = nil
				node.right = nil
				node.height = 1
				node.key += node.value
				node.value = node.key - node.value
				node.key -= node.value
				if valuetree == nil {
					valuetree = node
				} else {
					valuetree = valuetree.Merge(node)
				}
			} else {
				valuetree, node = valuetree.Pop()
				delta := speed * (now - node.key)
				last -= delta
				size -= delta
				for last < 0 {
					s.Result[k] = node.key - (last/delta)*(now-node.key)
					if s.Result[k] != s.Result[k] {
						s.Result[k] = node.key
					}
					last += step
					k--
					if k < 0 || last > size {
						break
					}

				}
				if k < 0 {
					break
				}
				now = node.key
				speed -= float64(node.size) / (node.value - node.key)
			}
		}
		if k < 0 {
			break
		}
		head = head.next
	}
	s.Uniform = float64(s.size) / (s.max - s.min) / s.Uniform
	for i := range s.Result {
		if s.Result[i] != s.Result[i] {
			s.Result = nil
			return
		}
	}
	if s.isStrType {
		for i := range s.Result {
			var frac float64
			str := make([]byte, s.maxlen)
			s.Result[i], _ = math.Modf(s.Result[i])
			for j := 0; j < s.maxlen; j++ {
				s.Result[i], frac = math.Modf(s.Result[i] / lens)
				k := int(frac*lens + 0.01)
				if k < 0 {
					s.Result = nil
					return
				}
				str[j] = inttobyte[k]
			}
			s.Result[i] = 0
			for j := len(str) - 1; j >= 0; j-- {
				s.Result[i] = s.Result[i]*256 + float64(str[j])
			}
			for j := 8 - len(str); j > 0; j-- {
				s.Result[i] = s.Result[i] * 256
			}
		}
	}
	for i := 1; i < len(s.Result); i++ {
		if s.Result[i] == s.Result[i-1] {
			s.Result = nil
			return
		}
	}
}

func ShuffleRangeReEvalUnsigned(ranges []float64, k2 int, nullCnt int64, tableCnt int64) []uint64 {
	k1 := len(ranges)
	if k2 > k1/2 {
		return nil
	}
	result := make([]uint64, k2-1)
	if tableCnt/int64(k2) <= nullCnt {
		result[0] = uint64(ranges[0])
		for i := 1; i <= k2-2; i++ {
			result[i] = uint64(ranges[(i)*(k1-1)/(k2-1)])
		}
	} else {
		for i := 0; i <= k2-2; i++ {
			result[i] = uint64(ranges[(i+1)*k1/k2-1])
		}
	}
	return result
}

func ShuffleRangeReEvalSigned(ranges []float64, k2 int, nullCnt int64, tableCnt int64) []int64 {
	k1 := len(ranges)
	if k2 > k1/2 {
		return nil
	}
	result := make([]int64, k2-1)
	if tableCnt/int64(k2) <= nullCnt {
		result[0] = int64(ranges[0])
		for i := 1; i <= k2-2; i++ {
			result[i] = int64(ranges[(i)*(k1-1)/(k2-1)])
		}
	} else {
		for i := 0; i <= k2-2; i++ {
			result[i] = int64(ranges[(i+1)*k1/k2-1])
		}
	}
	return result
}
