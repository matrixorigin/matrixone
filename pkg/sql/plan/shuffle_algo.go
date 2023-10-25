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

type shuffleHeap struct {
	left    *shuffleHeap
	right   *shuffleHeap
	key     float64
	value   float64
	height  int
	reverse bool
}

type shuffleList struct {
	next    *shuffleList
	tree    *shuffleHeap
	size    int
	value   float64
	overlap float64
}

type ShuffleRange struct {
	tree    *shuffleHeap
	Result  []float64
	size    int
	Overlap float64
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

func (t *shuffleHeap) Pop() (*shuffleHeap, float64, float64) {
	if t.left == nil {
		return nil, t.key, t.value
	}
	if t.right == nil {
		return t.left, t.key, t.value
	}
	return t.left.Merge(t.right), t.key, t.value
}

func NewShuffleRange() *ShuffleRange {
	return &ShuffleRange{}
}

func (s *ShuffleRange) Update(zmmin float64, zmmax float64) {
	s.size++
	if s.tree == nil {
		s.tree = &shuffleHeap{
			height: 1,
			key:    zmmax,
			value:  zmmin,
		}
	} else {
		s.tree = s.tree.Merge(&shuffleHeap{
			height: 1,
			key:    zmmax,
			value:  zmmin,
		})
	}
}

func (s *ShuffleRange) Eval(k int) {
	if k <= 1 {
		return
	}
	var Head *shuffleList
	var key, value float64
	s.Result = make([]float64, k-1)
	for s.tree != nil {
		s.tree, key, value = s.tree.Pop()
		Head = &shuffleList{
			next: Head,
			tree: &shuffleHeap{
				height:  1,
				key:     key,
				value:   value,
				reverse: true,
			},
			size:    1,
			value:   value,
			overlap: 1,
		}
		for Head.next != nil {
			next := Head.next
			if Head.tree.value >= next.tree.key {
				break
			}
			var delta float64
			if next.value >= Head.value {
				delta = next.overlap
			} else {
				delta = (next.tree.key-Head.value)/(next.tree.key-next.value)*(next.overlap) + (next.tree.key-Head.value)/(Head.tree.key-Head.value)*(Head.overlap)
				Head.value = next.value
			}
			s.Overlap += delta
			Head.overlap += next.overlap - delta
			Head.tree = Head.tree.Merge(next.tree)
			Head.size += next.size
			Head.next = next.next
		}
	}

	step := float64(s.size) / float64(k)
	last := step
	k -= 2
	for {
		size := float64(Head.size)
		if last > size {
			last -= size
			Head = Head.next
			continue
		}
		var valuetree *shuffleHeap
		var speed float64
		now := Head.tree.key
		for last <= size {
			if valuetree == nil || (Head.tree != nil && valuetree.key < Head.tree.key) {
				Head.tree, key, value = Head.tree.Pop()
				delta := speed * (now - key)
				last -= delta
				size -= delta
				for last < 0 {
					s.Result[k] = key - (last/delta)*(now-key)
					last += step
					k--
					if k < 0 || last > size {
						break
					}

				}
				if k < 0 {
					break
				}
				now = key
				if key == value {
					last -= 1
					size -= 1
					for last < 0 {
						s.Result[k] = key
						last += step
						k--
						if k < 0 || last > size {
							break
						}

					}
					if k < 0 {
						break
					}
					continue
				}
				speed += 1.0 / (key - value)
				if valuetree == nil {
					valuetree = &shuffleHeap{
						key:     value,
						value:   key,
						height:  1,
						reverse: true,
					}
				} else {
					valuetree = valuetree.Merge(&shuffleHeap{
						key:     value,
						value:   key,
						height:  1,
						reverse: true,
					})
				}
			} else {
				valuetree, key, value = valuetree.Pop()
				delta := speed * (now - key)
				last -= delta
				size -= delta
				for last < 0 {
					s.Result[k] = key - (last/delta)*(now-key)
					last += step
					k--
					if k < 0 || last > size {
						break
					}

				}
				if k < 0 {
					break
				}
				now = key
				speed -= 1.0 / (value - key)
			}
		}
		if k < 0 {
			break
		}
		last -= size
		Head = Head.next
	}
	s.Overlap /= float64(s.size)
}
