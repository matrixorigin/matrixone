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

type ShuffleTree struct {
	Left    *ShuffleTree
	Right   *ShuffleTree
	Key     float64
	Value   float64
	Height  int
	Reverse bool
}

type ShuffleRange struct {
	Tree      *ShuffleTree
	Result    []float64
	Size      int
	Overlap   int
	IsRegular bool
}

type ShuffleList struct {
	Next *ShuffleList
	Tree *ShuffleTree
	Size int
}

func (t *ShuffleTree) Merge(s *ShuffleTree) *ShuffleTree {
	if t.Key > s.Key != t.Reverse {
		if s.Right == nil {
			s.Right = t
		} else {
			s.Right = t.Merge(s.Right)
		}
		if s.Left == nil || s.Left.Height < s.Right.Height {
			tmp := s.Left
			s.Left = s.Right
			s.Right = tmp
		}
		s.Height = s.Left.Height + 1
		return s
	} else {
		if t.Right == nil {
			t.Right = s
		} else {
			t.Right = t.Right.Merge(s)
		}
		if t.Left == nil || t.Left.Height < t.Right.Height {
			tmp := t.Left
			t.Left = t.Right
			t.Right = tmp
		}
		t.Height = t.Left.Height + 1
		return t
	}
}

func (t *ShuffleTree) Pop() (*ShuffleTree, float64, float64) {
	if t.Left == nil {
		return nil, t.Key, t.Value
	}
	if t.Right == nil {
		return t.Left, t.Key, t.Value
	}
	return t.Left.Merge(t.Right), t.Key, t.Value
}

func NewShuffleFloat() *ShuffleRange {
	return &ShuffleRange{}
}

func (s *ShuffleRange) Update(zmmin []float64, zmmax []float64) {
	len := len(zmmin)
	s.Size += len
	for i := 0; i < len; i++ {
		if s.Tree == nil {
			s.Tree = &ShuffleTree{
				Height: 1,
				Key:    zmmax[i],
				Value:  zmmin[i],
			}
		} else {
			s.Tree = s.Tree.Merge(&ShuffleTree{
				Height: 1,
				Key:    zmmax[i],
				Value:  zmmin[i],
			})
		}
	}
}

func (s *ShuffleRange) Eval(k int) {
	if k <= 1 {
		return
	}
	var Head *ShuffleList
	var key, value float64
	var overlap int
	s.Result = make([]float64, k-1)
	for s.Tree != nil {
		s.Tree, key, value = s.Tree.Pop()
		Head = &ShuffleList{
			Next: Head,
			Tree: &ShuffleTree{
				Height:  1,
				Key:     key,
				Value:   value,
				Reverse: true,
			},
			Size: 1,
		}
		for Head.Next != nil {
			next := Head.Next
			if Head.Tree.Value >= next.Tree.Key {
				break
			}
			Head.Tree = Head.Tree.Merge(next.Tree)
			Head.Size += next.Size
			Head.Next = next.Next
		}
	}

	step := float64(s.Size) / float64(k)
	last := step
	k -= 2
	for {
		size := float64(Head.Size)
		if last > size {
			last -= size
			Head = Head.Next
			continue
		}
		var valuetree *ShuffleTree
		var speed float64
		now := Head.Tree.Key
		for last <= size {
			if valuetree == nil || (Head.Tree != nil && valuetree.Key < Head.Tree.Key) {
				Head.Tree, key, value = Head.Tree.Pop()
				delta := speed * (now - key)
				last -= delta
				size -= delta
				overlap++
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
				speed += 1.0 / (key - value)
				if valuetree == nil {
					valuetree = &ShuffleTree{
						Key:     value,
						Value:   key,
						Height:  1,
						Reverse: true,
					}
				} else {
					valuetree = valuetree.Merge(&ShuffleTree{
						Key:     value,
						Value:   key,
						Height:  1,
						Reverse: true,
					})
				}
			} else {
				valuetree, key, value = valuetree.Pop()
				delta := speed * (now - key)
				last -= delta
				size -= delta
				overlap--
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
		Head = Head.Next
	}
	s.Overlap = overlap
}
