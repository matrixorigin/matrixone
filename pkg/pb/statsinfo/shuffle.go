// Copyright 2021 - 2024 Matrix Origin
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

package statsinfo

import "math"

const DefaultEvalSize = 1024

type ShuffleList struct {
	Size  int64
	Value float64
	Next  *ShuffleList
	Tree  *ShuffleHeap
}

func (t *ShuffleHeap) Merge(s *ShuffleHeap) *ShuffleHeap {
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

func (t *ShuffleHeap) Pop() (*ShuffleHeap, *ShuffleHeap) {
	if t.Left == nil {
		return nil, t
	}
	if t.Right == nil {
		return t.Left, t
	}
	return t.Left.Merge(t.Right), t
}

func (s *ShuffleRange) UpdateString(zmmin []byte, zmmax []byte, rowCount int64, nullCount int64) {
	if len(zmmin) > 8 {
		zmmin = zmmin[:8]
	}
	if len(zmmax) > 8 {
		zmmax = zmmax[:8]
	}
	if s.Sz == 0 {
		s.Sz = rowCount
		s.Flags = make([]bool, 256)
		s.Mins = make([][]byte, 0)
		s.Maxs = make([][]byte, 0)
		s.Mins = append(s.Mins, zmmin)
		s.Maxs = append(s.Maxs, zmmax)
		s.Rows = make([]int64, 0)
		s.Rows = append(s.Rows, rowCount)
		s.Nulls = make([]int64, 0)
		s.Nulls = append(s.Nulls, nullCount)
	} else {
		s.Sz += rowCount
		s.Mins = append(s.Mins, zmmin)
		s.Maxs = append(s.Maxs, zmmax)
		s.Rows = append(s.Rows, rowCount)
		s.Nulls = append(s.Nulls, nullCount)
	}
	if s.MaxLen < int64(len(zmmin)) {
		s.MaxLen = int64(len(zmmin))
	}
	for _, c := range zmmin {
		s.Flags[int(c)] = true
	}
	if s.MaxLen < int64(len(zmmax)) {
		s.MaxLen = int64(len(zmmax))
	}
	for _, c := range zmmax {
		s.Flags[int(c)] = true
	}
}

func (s *ShuffleRange) Update(zmmin float64, zmmax float64, rowCount int64, nullCount int64) {
	s.Sz += rowCount
	if s.Tree == nil {
		s.Tree = &ShuffleHeap{
			Height: 1,
			Key:    zmmax,
			Value:  zmmin,
			Sz:     rowCount,
			Nulls:  nullCount,
		}
		s.Min = zmmin
		s.Max = zmmax
	} else {
		s.Tree = s.Tree.Merge(&ShuffleHeap{
			Height: 1,
			Key:    zmmax,
			Value:  zmmin,
			Sz:     rowCount,
			Nulls:  nullCount,
		})
		if s.Min > zmmin {
			s.Min = zmmin
		}
		if s.Max < zmmax {
			s.Max = zmmax
		}
	}
}

func (s *ShuffleRange) Eval() {
	k := DefaultEvalSize
	if s.Sz == 0 {
		return
	}
	bytetoint := make(map[byte]int)
	inttobyte := make([]byte, 0)
	var lens float64
	if s.IsStrType {
		for i := 0; i < 256; i++ {
			if s.Flags[i] {
				bytetoint[byte(i)] = len(inttobyte)
				inttobyte = append(inttobyte, byte(i))
			}
		}
		if len(inttobyte) == 0 {
			return
		}
		lens = float64(len(inttobyte))
		for i := range s.Mins {
			node := &ShuffleHeap{
				Height: 1,
				Key:    0,
				Value:  0,
				Sz:     s.Rows[i],
				Nulls:  s.Nulls[i],
			}
			for _, c := range s.Maxs[i] {
				node.Key = node.Key*lens + float64(bytetoint[c])
			}
			for j := int64(len(s.Maxs[i])); j < s.MaxLen; j++ {
				node.Key = node.Key * lens
			}
			for _, c := range s.Mins[i] {
				node.Value = node.Value*lens + float64(bytetoint[c])
			}
			for j := int64(len(s.Mins[i])); j < s.MaxLen; j++ {
				node.Value = node.Value * lens
			}
			if s.Tree == nil {
				s.Tree = node
			} else {
				s.Tree = s.Tree.Merge(node)
			}
		}
	}
	var head *ShuffleList
	var node *ShuffleHeap
	var nulls int64
	s.Result = make([]float64, k-1)
	for s.Tree != nil {
		s.Tree, node = s.Tree.Pop()
		node.Left = nil
		node.Right = nil
		node.Height = 1
		node.Sz -= node.Nulls
		nulls += node.Nulls
		node.Reverse = true
		head = &ShuffleList{
			Next:  head,
			Tree:  node,
			Size:  node.Sz,
			Value: node.Value,
		}
		if head.Next != nil {
			for head.Next != nil {
				next := head.Next
				if head.Tree.Value >= next.Tree.Key {
					break
				}
				if head.Tree.Key != head.Value {
					if head.Value <= next.Value {
						s.Overlap += float64(head.Size) * float64(next.Size) * (next.Tree.Key - next.Value) / (head.Tree.Key - head.Value)
					} else {
						s.Overlap += float64(head.Size) * float64(next.Size) * (next.Tree.Key - head.Value) * (next.Tree.Key - head.Value) / (head.Tree.Key - head.Value) / (next.Tree.Key - next.Value)
						head.Value = next.Value
					}
				}
				head.Tree = head.Tree.Merge(next.Tree)
				head.Size += next.Size
				head.Next = next.Next
			}

		}
	}
	s.Overlap /= float64(s.Sz) * float64(s.Sz)

	step := float64(s.Sz) / float64(k)
	if float64(nulls) >= step {
		step = float64(s.Sz-nulls) / float64(k-1)
	}
	last := step
	k -= 2
	s.Uniform = float64(s.Sz) / (s.Max - s.Min)
	for {
		if head == nil {
			for i := 0; i <= k; i++ {
				s.Result[k-i] = s.Min
			}
			break
		}
		Sz := float64(head.Size)
		var valueTree *ShuffleHeap
		var speed float64
		now := head.Tree.Key
		for {
			if valueTree == nil || (head.Tree != nil && valueTree.Key < head.Tree.Key) {
				if head.Tree == nil {
					break
				}
				head.Tree, node = head.Tree.Pop()
				delta := speed * (now - node.Key)
				last -= delta
				Sz -= delta
				for last <= 0 {
					s.Result[k] = node.Key - (last/delta)*(now-node.Key)
					if s.Result[k] != s.Result[k] {
						s.Result[k] = node.Key
					}
					last += step
					k--
					if k < 0 || last > Sz {
						break
					}

				}
				if k < 0 {
					break
				}
				now = node.Key
				if node.Key-node.Value < 0.1 {
					last -= float64(node.Sz)
					Sz -= float64(node.Sz)
					if last <= 0 {
						if -last <= last+float64(node.Sz) {
							s.Result[k] = now
							last = step
							k--
							if k < 0 {
								break
							}
						} else {
							s.Result[k] = now + 1
							last = step - float64(node.Sz)
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
				speed += float64(node.Sz) / (node.Key - node.Value)
				if s.Uniform < speed {
					s.Uniform = speed
				}
				node.Left = nil
				node.Right = nil
				node.Height = 1
				node.Key += node.Value
				node.Value = node.Key - node.Value
				node.Key -= node.Value
				if valueTree == nil {
					valueTree = node
				} else {
					valueTree = valueTree.Merge(node)
				}
			} else {
				valueTree, node = valueTree.Pop()
				delta := speed * (now - node.Key)
				last -= delta
				Sz -= delta
				for last < 0 {
					s.Result[k] = node.Key - (last/delta)*(now-node.Key)
					if s.Result[k] != s.Result[k] {
						s.Result[k] = node.Key
					}
					last += step
					k--
					if k < 0 || last > Sz {
						break
					}

				}
				if k < 0 {
					break
				}
				now = node.Key
				speed -= float64(node.Sz) / (node.Value - node.Key)
			}
		}
		if k < 0 {
			break
		}
		head = head.Next
	}
	s.Uniform = float64(s.Sz) / (s.Max - s.Min) / s.Uniform
	for i := range s.Result {
		if s.Result[i] != s.Result[i] {
			s.Result = nil
			return
		}
	}
	if s.IsStrType {
		for i := range s.Result {
			var frac float64
			str := make([]byte, s.MaxLen)
			s.Result[i], _ = math.Modf(s.Result[i])
			for j := int64(0); j < s.MaxLen; j++ {
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
