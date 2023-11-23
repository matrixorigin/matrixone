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

import "math"

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
	next  *shuffleList
	tree  *shuffleHeap
	size  int
	value float64
}

type ShuffleRange struct {
	tree    *shuffleHeap
	Result  []float64
	size    int
	max     float64
	min     float64
	Overlap float64
	Uniform float64
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

func NewShuffleRange() *ShuffleRange {
	return &ShuffleRange{}
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

func (s *ShuffleRange) Eval(k int) {
	if k <= 1 || s.size == 0 {
		return
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
			for k >= 0 {
				s.Result[k] = s.min - 0.1
				k--
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
				if node.key == node.value {
					last -= float64(node.size)
					size -= float64(node.size)
					if last <= 0 {
						if -last <= last+float64(node.size) {
							s.Result[k] = now - 0.1
							last = step
							k--
							if k < 0 {
								break
							}
						} else {
							s.Result[k] = now + 0.1
							last = step - float64(node.size)
							k--
							if k < 0 {
								break
							}
							if last <= 0 {
								s.Result[k] = now - 0.1
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
}

func (s *ShuffleRange) ReEval(k1 int, k2 int) []float64 {
	if k1 <= k2 {
		return s.Result
	}
	result := make([]float64, k2-1)
	if s.Result[0] < s.min {
		result[0] = s.Result[0]
		for i := 1; i <= k2-2; i++ {
			result[i] = s.Result[(i-1)*(k1-1)/(k2-1)+1]
		}

	} else {
		for i := 0; i <= k2-2; i++ {
			result[i] = s.Result[i*k1/k2]
		}
	}
	return result
}
